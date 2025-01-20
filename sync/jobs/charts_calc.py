import time

import psycopg2.extras
from jobs.job import Job
import utils
from logger import logger
import db


class ChartsCalc(Job):

    def __init__(self):
        super().__init__()
        self.con = None

    def execute(self):
        logger.info("Executing ChartsCalc")

        # init db connection
        self.con = db.db_connect()

        chartdata = self.calculate_historic_charts()
        self.insert_historic_chart_data(chartdata)

        # close db connection
        self.con.close()

        return "nochange" if chartdata is None else "success"

    def calculate_historic_charts(self):
        t1 = time.time()

        # last day in db
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT * FROM historic_chart_data ORDER BY id DESC LIMIT 1")
                data = cur.fetchone()

        if data:
            # first block after last day timestamp
            with self.con:
                with self.con.cursor() as cur:
                    cur.execute("SELECT * FROM blocks WHERE forked=0 AND timestamp > '{}' ORDER BY id ASC LIMIT 1".format(data[1]))
                    data = cur.fetchone()

            if data:
                startheight = data[1]
            else:
                startheight = 1
        else:

            startheight = 1

        # select blocks since last day timestamp

        with self.con:
            with self.con.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT * FROM blocks WHERE forked=0 AND height > {} ORDER BY id ASC LIMIT 25000;".format(startheight))
                data = cur.fetchall()

        hashrates = []
        diffs_b = []
        t_hashes = 0
        t_txn = 0
        if data:
            t_start = data[1]["timestamp"]

            # cancel early
            if data[len(data) - 1]["timestamp"] < t_start + 86400:
                return None

            # calculate blocktimes
            for i, block in enumerate(data):

                if block["timestamp"] > t_start + 86400 and i < len(data) - 1:
                    print(i)
                    t_time = block["timestamp"] - t_start

                    # count daily active users
                    with self.con:
                        with self.con.cursor() as cur:
                            cur.execute(
                                "SELECT * FROM txs WHERE {} < block_timestamp AND block_timestamp <= {};".format(
                                    t_start, block["timestamp"]))
                            txs = cur.fetchall()
                    users = []
                    for tx in txs:
                        if tx[8] not in users:
                            users.append(tx[8])
                        if tx[9] not in users:
                            users.append(tx[9])
                    dau = len(users)

                    hashrates.append({t_start + 86400: [round((t_hashes / t_time / 10 ** 12), 2),  # hashrate in TH/s
                                                        round(sum(diffs_b) / len(diffs_b) / 10 ** 12, 2),  # difficulty
                                                        round(t_txn / t_time, 3),  # tps
                                                        dau
                                                        ]})
                    t_start = data[i]["timestamp"]
                    t_hashes = 0
                    t_txn = 0
                    diffs_b = []
                t_hashes += int(round(block["difficulty"]))
                diffs_b.append(block["difficulty"])
                t_txn += block["txn"]
            return hashrates
        return None

    def insert_historic_chart_data(self, data):
        if data:
            for day in data:
                keys = list(day.keys())
                timestamp = keys[0]
                hashrate = day[timestamp][0]
                difficulty = day[timestamp][1]
                tps = day[timestamp][2]
                dau = day[timestamp][3]
                with self.con:
                    with self.con.cursor() as cur:
                        cur.execute(
                            "INSERT INTO historic_chart_data (timestamp, hashrate, difficulty, tps, dau) values('{}', '{}', '{}', '{}', {})".format(
                                timestamp, hashrate, difficulty, tps, dau))