import time
import os
import psycopg2
import psycopg2.extras
import utils

DECIMALS = 8


class Explorer:
    def __init__(self):
        user = os.environ.get('POSTGRES_DB')
        password = os.environ.get('POSTGRES_PASSWORD')
        db = os.environ.get('POSTGRES_DB')
        self.con = psycopg2.connect(database=db,
                                    host="db",
                                    user=user,
                                    password=password,
                                    port="5432",
                                    connect_timeout=3,
                                    keepalives=1,
                                    keepalives_idle=5,
                                    keepalives_interval=2,
                                    keepalives_count=2)

    #
    # general
    #

    def get_hashrate(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 ORDER BY id DESC LIMIT 501")
                data = cur.fetchall()
        blocktimes = []
        hashrates = []

        if len(data) == 0:
            return 0

        # calculate blocktimes
        for i, block in enumerate(data):
            if i == 500:
                break
            blocktime = block["timestamp"] - data[i + 1]["timestamp"]
            if blocktime == 0:
                blocktime = 1
            blocktimes.append(blocktime)
        # calculate t_hashes and t_time for last 40 blocks at i
        t_hashes = 0
        t_time = 0
        for i in range(0, 500):
            t_hashes += int(round(data[i]["difficulty"]))
            t_time += blocktimes[i]
        hashrates.append(round((t_hashes / t_time / 10 ** 12), 2))
        return hashrates[0]

    def get_hashrate_for_blocks(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 ORDER BY id DESC LIMIT 4821")
                data = cur.fetchall()
        hashrates = []
        blocktimes = []

        if len(data) == 0:
            return None

        # calculate blocktimes
        for i, block in enumerate(data):
            if i == 4820:
                break
            else:
                blocktime = block["timestamp"] - data[i + 1]["timestamp"]
                if blocktime == 0:
                    blocktime = 1
                blocktimes.append(blocktime)
        t_hashes = 0
        t_time = 0
        for i, blocktime in enumerate(blocktimes):
            t_hashes += int(round(data[i]["difficulty"]))
            t_time += blocktimes[i]
            if i < 500:
                continue
            elif i == 499:
                hashrates.append(float(round((t_hashes / t_time / 10 ** 12), 3)))
                continue
            # calculate t_hashes and t_time

            t_hashes -= int(round(data[i-500]["difficulty"]))
            t_time -= blocktimes[i-500]

            hashrates.append(float(round((t_hashes / t_time / 10 ** 12), 3)))
        return hashrates


    def get_historic_chart_data(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM historic_chart_data ORDER BY id")
                data = cur.fetchall()
        chart_data = []

        if len(data) == 0:
            return None

        for row in data:
            chart_data.append({row[1]: [row[2], row[3], row[4], row[5]]})
        return chart_data

    def get_mempool(self, page):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM mempool ORDER BY id DESC LIMIT 25 OFFSET %s;", ((page - 1) * 25,))
                data = cur.fetchall()

        if len(data) == 0:
            return None

        txs = []
        for row in data:
            txs.append({"hash": row["hash"], "amount": f'{row["amount"] / 10 ** DECIMALS:,.8f}',
                        "fee": f'{row["fee"] / 10 ** DECIMALS:,.8f}', "nonce": row["nonce"],
                        "sender": row["sender"], "recipient": row["recipient"]})
        return txs

    def get_tx_from_mempool(self, hash):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM mempool WHERE hash=%s;", (hash,))
                data = cur.fetchone()

        if not data:
            return None

        tx = {"type": data["type"], "hash": data["hash"], "amount": f'{data["amount"] / 10 ** DECIMALS:,.8f}',
              "fee": f'{data["fee"] / 10 ** DECIMALS:,.8f}', "nonce": data["nonce"], "pinHeight": data["pinheight"],
              "sender": data["sender"], "recipient": data["recipient"]}
        return tx

    #
    # node data
    #

    def get_nodes(self, p):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM nodes ORDER BY last_seen DESC, id LIMIT 25 OFFSET %s;", ((p - 1) * 25,))
                data = cur.fetchall()

        nodes = []
        for row in data:
            nodes.append({"Host": row[4], "Version": row[5], "Height": row[3], "First Seen": row[1], "Last Seen": row[2]})
        return nodes

    #
    # block data
    #

    def get_block(self, height):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE height=%s AND forked=0", (height,))
                data = cur.fetchone()

        if not data:
            return None

        block = {"height": data["height"], "difficulty": data["difficulty"], "timestamp": data["timestamp"],
                 "hash": data["hash"], "merkleRoot": data["merkleroot"], "nonce": data["nonce"],
                 "prevHash": data["prevhash"], "target": data["target"], "version": data["version"],
                 "minedBy": data["minedby"]}
        if int(height) >= 745200:
            block["floatSha256t"] = data["janushash1"]
            block["floatVerus"] = data["janushash2"]
        return block

    def get_last20_blocks(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 ORDER BY id DESC LIMIT 20;")
                data = cur.fetchall()
        blocks = []
        if len(data) == 0:
            return []

        h = data[0]["height"]
        txs = {}

        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM txs WHERE height>=%s AND height<=%s ORDER BY id;", (h - 20, h))
                data2 = cur.fetchall()

        txs_b = []

        for row in data2:
            txs_b.append({"hash": row[2],
                            "amount": row[3] / 100000000, "fee": row[4] / 100000000, "timestamp": row[10], "height": row[7],
                            "sender": row[8],
                            "recipient": row[9]})

        for tx in txs_b:
            if tx["height"] not in txs.keys():
                txs[tx["height"]] = []
            txs[tx["height"]].append(tx)

        for row in data:
            blocks.append({"height": row[1], "difficulty": float(row[2]), "timestamp": row[3], "minedBy": row[13],
                     "prevHash": row[9],
                     "merkleRoot": row[5], "nonce": row[6], "transactions": txs[row[1]]})
        return blocks

    def get_forked_blocks(self, page):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=1 ORDER BY id DESC LIMIT 25 OFFSET %s;", ((page - 1) * 25,))
                data = cur.fetchall()
        blocks = []

        if len(data) == 0:
            return []

        for row in data:
            blocks.append({"height": row["height"], "difficulty": row["difficulty"], "timestamp": row["timestamp"],
                           "hash": row["hash"], "merkleRoot": row["merkleroot"], "nonce": row["nonce"],
                           "prevHash": row["prevhash"], "raw": row["raw"], "target": row["target"],
                           "version": row["version"], "minedBy": row["minedby"]})
        return blocks

    def get_txs_for_block(self, height, page):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM txs WHERE height=%s ORDER BY id LIMIT 25 OFFSET %s;", (height, (page-1)*25))
                data = cur.fetchall()

        if len(data) == 0:
            return None

        txs = []
        for row in data:

            txs.append({"hash": row["hash"], "amount": f'{row["amount"] / 10 ** DECIMALS:,.8f}',
                        "fee": f'{row["fee"] / 10 ** DECIMALS:,.8f}', "timestamp": row["block_timestamp"],
                        "height": row["height"], "sender": row["sender"], "recipient": row["recipient"]})
        return txs

    def get_latest_blocks(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 ORDER BY id DESC LIMIT 10")
                data = cur.fetchall()

        if len(data) == 0:
            return None

        blocks = []
        for row in data:
            blocks.append([f'<a href="/block/{row["height"]}">{row["height"]}</a>', row["txn"], row["timestamp"]])
        return blocks

    def get_difficulty_for_blocks(self, c):
        diffs = []
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 ORDER BY id DESC LIMIT %s;", (c,))
                data = cur.fetchall()

        if len(data) == 0:
            return []

        for row in data:
            # in TH/s
            diffs.append(float(round(row["difficulty"] / 1000000000000, 2)))
        return diffs

    def get_blocktime_for_blocks(self, c):
        times = []
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 ORDER BY id DESC LIMIT %s;", (c,))
                data = cur.fetchall()

        if len(data) == 0:
            return []

        for i, row in enumerate(data):
            if i == c-1:
                continue
            times.append(row["timestamp"] - data[i+1]["timestamp"])
        return times

    def get_height(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT MAX(height) from blocks WHERE forked=0")
                row = cur.fetchone()
        if not row:
            return 0
        return row[0]

    #
    # tx data
    #

    def get_tx(self, txhash):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM txs WHERE hash=%s", (txhash,))
                data = cur.fetchone()

        if not data:
            return None

        tx = {"type": data["type"], "hash": data["hash"], "amount": f'{data["amount"] / 10 ** DECIMALS:,.8f}',
              "fee": f'{data["fee"] / 10 ** DECIMALS:,.8f}', "nonce": data["nonce"], "pinHeight": data["pinheight"],
              "height": data["height"], "sender": data["sender"], "recipient": data["recipient"],
              "timestamp": data["block_timestamp"]}
        return tx

    def get_txs_csv(self, address, start_timestamp, end_timestamp):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM txs WHERE (sender=%s or recipient=%s) AND block_timestamp >= %s AND"
                            " block_timestamp <= %s ORDER BY ID LIMIT 5000;", (address, address, start_timestamp,
                                                                               end_timestamp))
                data = cur.fetchall()

        if len(data) == 0:
            return None

        csv = [["type", "hash", "amount", "fee", "nonce", "pinHeight", "height", "sender", "recipient", "timestamp"]]
        for row in data:
            csv.append([row["type"], row["hash"], f'{row["amount"] / 10 ** DECIMALS:,.8f}',
                        f'{row["fee"] / 10 ** DECIMALS:,.8f}', row["nonce"], row["pinheight"], row["height"],
                        row["sender"], row["recipient"], utils.timestamp_to_datetime(row["block_timestamp"]) + " UTC"])
        return csv

    def get_latest_txs(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM txs ORDER BY id DESC LIMIT 10;")
                data = cur.fetchall()

        if data:
            txs = []
            for row in data:
                txs.append(["<a href=/tx/" + row[2] + ">" + row[2][:18] + "..." + "</a>",
                            f'{row[3] / 10 ** DECIMALS:,.8f}', row[10],
                            "<a href=/block/" + str(row[7]) + ">" + str(row[7]) + "</a>"])
            return txs
        return None

    def get_latest_tps(self):
        t = round(time.time())
        tps = []
        labels = []
        for i in range(20):
            with self.con:
                with self.con.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM txs WHERE block_timestamp > %s AND block_timestamp <= %s;",
                        (t - (i + 1) * 30, t - (i * 30)))
                    data = cur.fetchone()
                    tps.append(data[0] / 30)
                    labels.append(f"-{(i + 1)*30}s")
        tps.reverse()
        labels.reverse()
        return tps, labels

    # txs per block
    def get_txn_for_blocks(self):
        txdata = []
        height = self.get_height() - 4320
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE height > %s ORDER BY height DESC;", (height,))
                data = cur.fetchall()

        for row in data:
            txdata.append(row["txn"])
        return txdata

    def get_total_txs(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM txs")
                data = cur.fetchall()
        return data[0]

    #
    # account data
    #

    def get_account(self, address):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM balances WHERE account=%s;", (address,))
                data = cur.fetchone()
        balance = 0
        label = ""
        ratio = 0
        if data:
            balance = data["balance"]
            label = data["label"]
            ratio = data["miningratio"]

        return {"address": address, "label": label, "balance": balance / 10 ** DECIMALS, "ratio": ratio}

    def get_txs_for_account(self, address, page):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM txs WHERE sender=%s OR recipient=%s ORDER BY id DESC LIMIT 25 OFFSET %s;",
                            (address, address, (page - 1) * 25))
                data = cur.fetchall()

        if len(data) == 0:
            return None

        txs = []
        for row in data:
            txs.append({"hash": row["hash"], "amount": f'{row["amount"] / 10 ** DECIMALS:,.8f}',
                        "fee": f'{row["fee"] / 10 ** DECIMALS:,.8f}', "timestamp": row["block_timestamp"],
                        "height": row["height"], "sender": row["sender"], "recipient": row["recipient"]})
        return txs

    def get_accounts(self, page):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM balances ORDER BY balance DESC LIMIT 50 OFFSET %s;",
                            ((page - 1) * 50,))
                data = cur.fetchall()
        accounts = []
        if data:
            i = 1 + ((page-1)*50)
            for row in data:
                accounts.append({"#": i, "address": row['account'], "label": row["label"],
                                 "balance": f'{row["balance"] / 10 ** DECIMALS:,.8f}'})
                i += 1
        return accounts

    def get_miningratios(self, page, daily=False):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                if daily:
                    cur.execute("SELECT * FROM balances ORDER BY miningratio24h DESC LIMIT 50 OFFSET %s",
                                ((page - 1) * 50,))
                else:
                    cur.execute("SELECT * FROM balances ORDER BY miningratio DESC LIMIT 50 OFFSET %s",
                                ((page - 1) * 50,))
                data = cur.fetchall()

        accounts = []
        i = 1 + ((page-1)*50)
        for row in data:
            if row['miningratio24h' if daily else 'miningratio'] >= 60:
                accounts.append({"#": i, "address": "<a class='underline' href='/account/{}'>".format(row["account"]) + str(row["account"]) + "</a>",
                                 "hashrateRatio": f"<p class='text-red-500 inline'>{str(round(row['miningratio24h' if daily else 'miningratio'], 4))}</p>"})
            elif row['miningratio24h' if daily else 'miningratio'] >= 45:
                accounts.append({"#": i, "address": "<a class='underline' href='/account/{}'>".format(row[1]) + str(row[1]) + "</a>",
                                 "hashrateRatio": f"<p class='text-orange-300 inline'>{str(round(row['miningratio24h' if daily else 'miningratio'], 4))}</p>"})
            else:
                accounts.append({"#": i, "address": "<a class='underline' href='/account/{}'>".format(row[1]) + str(row[1]) + "</a>",
                                 "hashrateRatio": f"<p class='text-green-500 inline'>{str(round(row['miningratio24h' if daily else 'miningratio'], 4))}</p>"})


            i += 1
        return accounts

    def get_top100(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM balances ORDER BY balance DESC LIMIT 100")
                data = cur.fetchall()
        accounts = []
        if data:
            for row in data:
                accounts.append([f'{row["account"]} {row["label"]}', row["balance"] / 10 ** DECIMALS])
        return accounts

    def get_account_total(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM balances")
                data = cur.fetchone()
        if not data:
            return 0
        return data[0]

    def get_balance_total(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM balances")
                data = cur.fetchall()
        if len(data) == 0:
            return 0

        t_bal = 0

        for row in data:
            # if row[1] != "00000000000000000000000000000000000000000000000000":
            t_bal += row["balance"]

        return t_bal

    def get_stats_accounts(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM statistics WHERE type='accounts' ORDER BY id;")
                data = cur.fetchall()
        r = []
        r2 = {}
        data_24h = {}
        data_7d = {}
        data_30d = {}
        for row in data:
            t = ""
            # filter by period
            if "_24h" in row[2]:
                data_24h[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            elif "_7d" in row[2]:
                data_7d[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            elif "_30d" in row[2]:
                data_30d[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            else:
                r2[row[3]] = str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]
        for dp in data_24h.keys():
            data = {"": data_24h[dp][0]}
            data["24h"] = data_24h[dp][1]
            data["7d"] = data_7d[dp][1]
            data["30d"] = data_30d[dp][1]
            r.append(data)
        return r2, r

    def get_stats_txs(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM statistics WHERE type='txs' ORDER BY id;")
                data = cur.fetchall()
        r = []
        r2 = {}
        data_24h = {}
        data_7d = {}
        data_30d = {}
        for row in data:
            t = ""
            # filter by period
            if "_24h" in row[2]:
                data_24h[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            elif "_7d" in row[2]:
                data_7d[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            elif "_30d" in row[2]:
                data_30d[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            else:
                r2[row[3]] = str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]
        for dp in data_24h.keys():
            data = {"": data_24h[dp][0]}
            data["24h"] = data_24h[dp][1]
            data["7d"] = data_7d[dp][1]
            data["30d"] = data_30d[dp][1]
            r.append(data)
        return r2, r

    def get_stats_mining(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM statistics WHERE type='mining' ORDER BY id;")
                data = cur.fetchall()
        r = []
        r2 = {}
        data_24h = {}
        data_7d = {}
        data_30d = {}
        for row in data:
            t = ""
            # filter by period
            if "_24h" in row[2]:
                data_24h[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            elif "_7d" in row[2]:
                data_7d[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            elif "_30d" in row[2]:
                data_30d[row[3]] = [row[3], str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]]
            else:
                r2[row[3]] = str(f'{(row[4] / 10 ** DECIMALS):,.8f}' if row[5] == "WART" else row[4]) + " " + row[5]
        for dp in data_24h.keys():
            data = {"": data_24h[dp][0]}
            data["24h"] = data_24h[dp][1]
            data["7d"] = data_7d[dp][1]
            data["30d"] = data_30d[dp][1]
            r.append(data)
        return r2, r

    def get_price(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM price_data ORDER BY id DESC")
                data = cur.fetchone()
        if not data:
            return 0
        return data[2]

    def get_last_block_seen(self):
        logs = []

        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM blocks ORDER BY id DESC LIMIT 1")
                data = cur.fetchone()
        if data:
            return data[4]
        return 0

    def get_logs(self, event):
        logs = []

        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM logs WHERE event=%s", (event,))
                data = cur.fetchall()
        if data:
            for row in data:
                logs.append({"event": row[1], "timestamp": row[2], "duration": row[3]})
        return logs

    def get_logs_paged(self, event, page):
        logs = []

        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM logs WHERE event=%s ORDER BY timestamp DESC LIMIT 25 OFFSET %s",
                            (event, (page - 1) * 25))
                data = cur.fetchall()
        if data:
            for row in data:
                logs.append({"Event": row[1], "Timestamp": row[2], "Duration": row[3]})
        return logs

    def get_latest_logs(self):
        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 25")
                data = cur.fetchall()
        return data

    def get_blocktime_delta(self):
        s = 0
        c = 0
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT timestamp, timestamp_seen FROM blocks WHERE timestamp > %s;",
                            (round(time.time()) - 86400,))
                data = cur.fetchall()
        for r in data:
            s = s + (r[1] - r[0])
            c += 1
        print((s / c))
        return round((s / c), 2)

    def get_supply_delta(self):
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT SUM(balance) FROM balances;")
                data = cur.fetchone()
        return data[0] - utils.calculate_expected_supply(self.get_height())
