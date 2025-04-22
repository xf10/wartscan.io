import time
from math import exp

import psycopg2.extras
import requests
from scipy.optimize import fsolve

import utils
import db
from jobs.job import Job
from logger import logger


class ChainSync(Job):

    def __init__(self, hostman, notifier):
        super().__init__()
        self.hostman = hostman
        self.host = hostman.get_host()
        self.con = None
        self.notifier = notifier

    def execute(self):
        logger.info("Executing ChainSync")

        # init db connection
        self.con = db.db_connect()

        # check for forks
        fr = self.forkcheck()
        if fr == "fail":
            self.con.close()
            return "fail"

        # check latest block in db against node height and sync if <
        node_height = self.get_node_height()
        db_height = self.get_db_height()

        # If latest block in db is older than 10 minutes mark update as failed
        if db_height != 0:
            if db.getblock(self.con, self.get_db_height())["Timestamp"] < time.time() - 600:
                self.hostman.conn_fail()

        # if node height is not higher than db height return
        if db_height >= node_height:
            self.con.close()
            return "nochange"

        # sync
        status = self.sync(db_height+1, node_height)

        # close db connection
        self.con.close()

        return status

    def get_node_height(self):
        # get height current height from node
        try:
            r = requests.get(f"{self.hostman.get_host()}/chain/head", timeout=5)
            rdata = r.json()
            height = rdata["data"]["height"]
        except Exception as e:
            logger.error(f"Error getting node height: {e}")
            self.hostman.conn_fail()
            height = 0
        return height

    def get_db_height(self):
        # get db height
        with self.con:
            with self.con.cursor() as cur:
                try:
                    cur.execute("SELECT MAX(height) from blocks WHERE forked=0")
                    row = cur.fetchone()
                except Exception:
                    logger.error("SQL error in get_db_height")
                    return 0
        return row[0] if row[0] else 0

    def sync(self, startheight, endheight):
        sql = ""

        try:
            # fetch mempool
            r = requests.get((self.hostman.get_host()) + "/transaction/mempool", timeout=5)
            rdata = r.json()
            if rdata["code"] != 0:
                raise Exception("Return code is not 0")
            self.insertmempool(rdata["data"]["data"])
        except Exception as e:
            logger.error(f"Error getting mempool: {e}")
            # increment hostindex
            self.hostman.conn_fail()

        logger.info(f"Starting scan at height {str(startheight)}/{str(endheight)}")
        for i in range(startheight, endheight+1):

            try:
                r = requests.get(f"{self.hostman.get_host()}/chain/block/{i}", timeout=5)
            except Exception as e:
                logger.error(f"Error getting block data @ Sync: {e}")
                self.hostman.conn_fail()
                return "error"
            else:
                rdata = r.json()

                if rdata["code"] != 0:
                    return "fail"

                block = utils.buildblock(rdata["data"])
                miner = utils.calculate_mined_by(block)

                sql += f"""INSERT INTO blocks (height, difficulty, timestamp, timestamp_seen, hash, merkleRoot, nonce, janushash1,
                 janushash2, prevHash, raw, target, version, minedby, txn) VALUES ({block["height"]}, {block["difficulty"]},
                  {block["timestamp"]}, {round(time.time())}, '{block["hash"]}', '{block["merkleRoot"]}', '{block["nonce"]}',
                   '{block["floatSha256t"]}', '{block["floatVerus"]}', '{block["prevHash"]}', '{block["raw"]}',
                    '{block["target"]}', '{block["version"]}', '{miner}', {len(block["transactions"])});"""

                for txraw in block["transactions"]:
                    tx = utils.buildtx(txraw)
                    tx["height"] = i
                    sql += "INSERT INTO txs (type, hash, amount, fee, nonce, pinHeight, height, sender, recipient, block_timestamp)" \
                           " VALUES ('{}', '{}', {}, {}, {}, {}, {}, '{}', '{}', {});".format(tx["type"],
                                                                                              tx["hash"],
                                                                                              tx["amount"],
                                                                                              tx["fee"],
                                                                                              tx["nonce"],
                                                                                              tx["pinHeight"],
                                                                                              block["height"],
                                                                                              tx["sender"],
                                                                                              tx["recipient"],
                                                                                              block["timestamp"])
                logger.debug(f"Block {str(i)} done.")

                # commit every 1000 blocks
                if i % 1000 == 0:
                    logger.debug("Committing block data")
                    db.commit_sql(self.con, sql)
                    sql = ""

                # # notify every 100 blocks
                # if self.get_db_height() % 100 == 0 and startheight != 1:
                #     self.notify_general("Block {}".format(self.get_db_height()),
                #                         "Hashrate: {}".format(self.gethashrate()))

        # dont commit empty query (every 1000 blocks)
        if sql != "":
            db.commit_sql(self.con, sql)

        self.calculate_balances(startheight)
        logger.debug("Sync done")

        return "success"

    def calculate_balances(self, startheight, rollback=False):
        logger.info(f"{'Rolling back' if rollback else 'Calculating'} balances...")
        t1 = time.perf_counter()

        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM txs WHERE height >= {};".format(startheight))
                txn = cur.fetchone()[0]

        balance_deltas = {}

        # split into rounds of 100000 txs
        r = txn // 100000
        if txn % 100000 != 0:
            r += 1

        for i in range(r):
            logger.debug(f"round {i+1} of {r}")
            with self.con:
                with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    try:
                        cur.execute(
                            "SELECT * FROM txs WHERE height >= {} ORDER BY id LIMIT 100000 OFFSET {};".format(startheight, i*100000))
                        txs = cur.fetchall()
                    except Exception as e:
                        logger.error("SQL error in calculate_balances SELECT")

            for tx in txs:
                # check tx type
                if tx["type"] == "transfer":
                    # recipient balance
                    if tx["recipient"] not in balance_deltas.keys():
                        # balance, first_movement, last_movement
                        balance_deltas[tx["recipient"]] = [tx["amount"], tx["block_timestamp"], tx["block_timestamp"]]
                    else:
                        balance_deltas[tx["recipient"]][0] += tx["amount"]
                        balance_deltas[tx["recipient"]][2] = tx["block_timestamp"]

                    # sender balance
                    if tx["sender"] not in balance_deltas.keys():
                        balance_deltas[tx["sender"]] = [-tx["amount"] - tx["fee"], tx["block_timestamp"], tx["block_timestamp"]]
                    else:
                        balance_deltas[tx["sender"]][0] -= tx["amount"] + tx["fee"]
                        balance_deltas[tx["sender"]][2] = tx["block_timestamp"]

                elif tx["type"] == "reward":
                    # recipient balance
                    if tx["recipient"] not in balance_deltas.keys():
                        balance_deltas[tx["recipient"]] = [tx["amount"], tx["block_timestamp"], tx["block_timestamp"]]
                    else:
                        balance_deltas[tx["recipient"]][0] += tx["amount"]
                        balance_deltas[tx["recipient"]][2] = tx["block_timestamp"]

                    # miningratio
                    if startheight != 1:
                        r = utils.calculate_miningratio(self.con, tx["recipient"])
                        r24 = utils.calculate_miningratio(self.con, tx["recipient"], daily=True)
                        if r is None:
                            r = 0
                        if r24 is None:
                            r24 = 0
                        balance_deltas[tx["recipient"]].append(r)
                        balance_deltas[tx["recipient"]].append(r24)
            logger.debug(f"balcalc iteration {time.perf_counter()-t1}s")


        sql = ""
        for account in balance_deltas.keys():
            t_ratio = time.perf_counter()
            # miningratio
            if startheight == 1:
                r = utils.calculate_miningratio(self.con, account)
                if r is None:
                    r = 0
                balance_deltas[account].append(r)
                balance_deltas[account].append(0)
            logger.debug(f"ratio time {time.perf_counter()-t_ratio}s")


            # no mining ratio update
            if len(balance_deltas[account]) == 3:
                    sql += "INSERT INTO balances (account, balance, first_movement, last_movement)"\
                    f" VALUES ('{account}', {balance_deltas[account][0]}, {balance_deltas[account][1]},"\
                    f" {balance_deltas[account][2]}) ON CONFLICT (account) DO UPDATE SET balance = balances.balance"\
                    f" {'-' if rollback else '+'} EXCLUDED.balance, last_movement = EXCLUDED.last_movement;"
            # miningratio update
            else:
                    sql += "INSERT INTO balances (account, balance, first_movement, last_movement, miningratio, miningratio24h)"\
                    f" VALUES ('{account}', {balance_deltas[account][0]}, {balance_deltas[account][1]},"\
                    f" {balance_deltas[account][2]}, {balance_deltas[account][3]}, {balance_deltas[account][4]})"\
                    f" ON CONFLICT (account) DO UPDATE SET"\
                    f" balance = balances.balance {'-' if rollback else '+'} EXCLUDED.balance, last_movement = EXCLUDED.last_movement,"\
                    " miningratio = EXCLUDED.miningratio, miningratio24h = EXCLUDED.miningratio24h;"



        with self.con:
            with self.con.cursor() as cur:
                try:
                    db.commit_sql(self.con, sql)
                except Exception:
                    logger.error("SQL error in calculate_balances INSERT")

        logger.debug(f"after insertion {time.perf_counter() - t1}s")

        # check balance total against expected circulating supply
        with self.con:
            with self.con.cursor() as cur:
                try:
                    cur.execute("SELECT * FROM balances;")
                    data = cur.fetchall()
                except Exception:
                    logger.error("SQL error in calculate_balances SELECT check")
        total = 0
        if data:
            for row in data:
                total += row[2]

        height = self.get_db_height()
        logger.debug("total: {} | expected: {}".format(total, utils.calculate_expected_supply(height)))
        if total != utils.calculate_expected_supply(height):
            self.notifier.notify_general("Supply Error", "expected: `{}`, got: `{}`, at height: `{}`, startheight: `{}`".format(
                utils.calculate_expected_supply(height), total, self.get_db_height(), startheight))

        logger.debug(f"{'Rollback' if rollback else 'Calculation'} done! Took {str(time.perf_counter() - t1)} seconds")

    def reset_balances(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("UPDATE balances SET balance=0;")

    def calculate_miningratio(self, address, daily=False):
        sha256t_list = []
        with self:
            with self.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                try:
                    if daily:
                        cur.execute(
                            f"SELECT * FROM blocks WHERE minedBy = '{address}' AND height >= 855000 AND timestamp >= {round(time.time() - 86400)} ORDER BY height DESC LIMIT 30")
                    else:
                        cur.execute(
                            f"SELECT * FROM blocks WHERE minedBy = '{address}' AND height >= 855000 ORDER BY height DESC LIMIT 30")
                    data = cur.fetchall()
                except Exception:
                    logger.error("SQL error in calculating_miningratio")
                    return None

        for row in data:
            sha256t_list.append(float(row["janushash1"]))

        """Function to determine the mining ratio
        from a list of observed sha256t values

        :sha256t_list: list of numbers in [0,1] corresponding to sha256t hashes
        :returns: estimate of mining ratio

        """
        y_avg = sum(sha256t_list) / len(sha256t_list)
        c = exp(-6.5)
        p = lambda a: 0.3 / 1.3 * ((c + 1 / a) ** 1.3 - c ** 1.3) / ((c + 1 / a) ** 0.3 - c ** 0.3)
        threshold = p(100000)
        if y_avg < threshold:
            return 100000
        elif y_avg > p(1):
            return 1
        f = lambda a: p(a) - y_avg
        return round(fsolve(f, [1, 100000])[0], 4)

    def insertmempool(self, mempool):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("DELETE FROM mempool;")
        if len(mempool) >= 1:
            for txraw in mempool:
                tx = utils.buildtx(txraw)
                with self.con:
                    with self.con.cursor() as cur:
                        cur.execute(
                            "INSERT INTO mempool (type, hash, amount, fee, nonce, pinHeight, sender, recipient)"
                            " VALUES ('{}', '{}', {}, {}, {}, {}, '{}', '{}')".format("transfer",
                                                                                      tx["hash"],
                                                                                      tx["amount"],
                                                                                      tx["fee"],
                                                                                      tx["nonce"],
                                                                                      tx["pinHeight"],
                                                                                      tx["sender"],
                                                                                      tx["recipient"]))

    # FORKCHECK

    def forkcheck(self):
        db_height = self.get_db_height()
        if db_height < 1:
            return "nochange"

        # check blocks for forks in descending order until we find a block that's correct
        for i in range(db_height, 1, -1):
            try:
                blockdata = requests.get(f"{self.hostman.get_host()}/chain/block/{i}", timeout=5).json()
            except Exception as e:
                logger.error("Requesting block data failed @ ForkCheck")
                self.hostman.conn_fail()
                return "fail"
            else:
                block = utils.buildblock(blockdata["data"])
                # if block correct
                if self.checkblock(i, block):
                    # if i == db_height nothing happened
                    if i == db_height:
                        return "nochange"

                    with self.con:
                        with self.con.cursor() as cur:
                            cur.execute(f"UPDATE blocks SET forked=1 WHERE height>={i + 1};")
                            self.calculate_balances(i + 1, rollback=True)
                            cur.execute(f"DELETE FROM txs WHERE height>={i + 1};")

                    return "success"


    # checks block against data in db
    def checkblock(self, height, block):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    f"SELECT * FROM blocks WHERE forked=0 AND height={height};")
                data = cur.fetchone()
        if not data:
            return False
        # if block data mismatch
        if float(data["difficulty"]) != block["difficulty"] or data["timestamp"] != block["timestamp"] or \
                data["hash"] != block["hash"] or data["merkleroot"] != block["merkleRoot"] or data["prevhash"] != block["prevHash"]:

            logger.debug(f"ForkCheck found invalid block at height {block['height']}")

            # delete latest chart datapoint if necessary
            with self.con:
                with self.con.cursor() as cur:
                    cur.execute(
                        "DELETE FROM historic_chart_data WHERE timestamp>=" + str(data[3]) + ";")
            return False
        if self.gettxn(height) != len(block["transactions"]):
            return False
        return True


    def gettxn(self, height):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM txs WHERE height={height}")
                data = cur.fetchone()
        if not data:
            return 0
        return data[0]
