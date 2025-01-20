import time
from jobs.job import Job
import utils
from logger import logger
import db


class StatsCalc(Job):

    def __init__(self):
        super().__init__()
        self.con = None

    def execute(self):
        logger.info("Executing StatsCalc")

        # init db connection
        self.con = db.db_connect()

        self.calculate_stats()

        # close db connection
        self.con.close()

        return "success"

    def calculate_stats(self):
        t1 = time.time()

        # table: id, type, name, value, unit

        stats = []

        # account stats
        with self.con:
            with self.con.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM balances;")
                data = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE first_movement >={round(time.time()) - 86400};")
                data2 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE first_movement >={round(time.time()) - 604800};")
                data3 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE first_movement >={round(time.time()) - 2592000};")
                data4 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE last_movement >={round(time.time()) - 86400};")
                data5 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE last_movement >={round(time.time()) - 604800};")
                data6 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE last_movement >={round(time.time()) - 2592000};")
                data7 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE balance >= 100000000;")
                data8 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE balance >= 10000000000;")
                data9 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE balance >= 100000000000;")
                data10 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE balance >= 1000000000000;")
                data11 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE balance >= 5000000000000;")
                data12 = cur.fetchone()
                cur.execute(f"SELECT COUNT(*) FROM balances WHERE balance >= 10000000000000;")
                data13 = cur.fetchone()
                cur.execute(f"SELECT SUM(balance) FROM (SELECT balance FROM balances ORDER BY balance DESC LIMIT 10) AS sub;")
                data14 = cur.fetchone()
                cur.execute(f"SELECT SUM(balance) FROM (SELECT balance FROM balances ORDER BY balance DESC LIMIT 100) AS sub;")
                data15 = cur.fetchone()

        total_accounts = data[0]
        new_accounts_24h = data2[0]
        new_accounts_7d = data3[0]
        new_accounts_30d = data4[0]
        active_accounts_24h = data5[0]
        active_accounts_7d = data6[0]
        active_accounts_30d = data7[0]
        balance_1 = data8[0]
        balance_100 = data9[0]
        balance_1k = data10[0]
        balance_10k = data11[0]
        balance_50k = data12[0]
        balance_100k = data13[0]
        sum_10 = data14[0]
        sum_100 = data15[0]

        stats.append(["accounts", "total", "Total", total_accounts, ""])
        stats.append(["accounts", "new_24h", "New Accounts", new_accounts_24h, ""])
        stats.append(["accounts", "new_7d", "New Accounts", new_accounts_7d, ""])
        stats.append(["accounts", "new_30d", "New Accounts", new_accounts_30d, ""])
        stats.append(["accounts", "active_24h", "Active Accounts", active_accounts_24h, ""])
        stats.append(["accounts", "active_7d", "Active Accounts", active_accounts_7d, ""])
        stats.append(["accounts", "active_30d", "Active Accounts", active_accounts_30d, ""])
        stats.append(["accounts", "balance_1", "Balance >= 1", balance_1, ""])
        stats.append(["accounts", "balance_100", "Balance >= 100", balance_100, ""])
        stats.append(["accounts", "balance_1k", "Balance >= 1k", balance_1k, ""])
        stats.append(["accounts", "balance_10k", "Balance >= 10k", balance_10k, ""])
        stats.append(["accounts", "balance_50k", "Balance >= 50k", balance_50k, ""])
        stats.append(["accounts", "balance_100k", "Balance >= 100k", balance_100k, ""])
        stats.append(["accounts", "sum_10", "Top 10", sum_10, "WART"])
        stats.append(["accounts", "sum_100", "Top 100", sum_100, "WART"])

        # txs stats
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"SELECT SUM(txn) FROM blocks where forked=0 AND timestamp >= {round(time.time()) - 86400};")
                data = cur.fetchone()
                cur.execute(f"SELECT SUM(txn) FROM blocks where forked=0 AND  timestamp >= {round(time.time()) - 604800};")
                data2 = cur.fetchone()
                cur.execute(f"SELECT SUM(txn) FROM blocks where forked=0 AND  timestamp >= {round(time.time()) - 2592000};")
                data3 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM txs where block_timestamp >= {round(time.time()) - 86400} AND type='transfer';")
                data4 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM txs where block_timestamp >= {round(time.time()) - 604800} AND type='transfer';")
                data5 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM txs where block_timestamp >= {round(time.time()) - 2592000} AND type='transfer';")
                data6 = cur.fetchone()
                cur.execute(
                    f"SELECT SUM(amount) FROM txs where block_timestamp >= {round(time.time()) - 86400} AND type='transfer';")
                data7 = cur.fetchone()
                cur.execute(
                    f"SELECT SUM(amount) FROM txs where block_timestamp >= {round(time.time()) - 604800} AND type='transfer';")
                data8 = cur.fetchone()
                cur.execute(
                    f"SELECT SUM(amount) FROM txs where block_timestamp >= {round(time.time()) - 2592000} AND type='transfer';")
                data9 = cur.fetchone()
                cur.execute(
                    f"SELECT SUM(fee) FROM txs where block_timestamp >= {round(time.time()) - 86400} AND type='transfer';")
                data10 = cur.fetchone()
                cur.execute(
                    f"SELECT SUM(fee) FROM txs where block_timestamp >= {round(time.time()) - 604800} AND type='transfer';")
                data11 = cur.fetchone()
                cur.execute(
                    f"SELECT SUM(fee) FROM txs where block_timestamp >= {round(time.time()) - 2592000} AND type='transfer';")
                data12 = cur.fetchone()

        total_24h = data[0]
        total_7d = data2[0]
        total_30d = data3[0]
        transfers_24h = data4[0]
        transfers_7d = data5[0]
        transfers_30d = data6[0]
        transfer_amount_24h = data7[0]
        transfer_amount_7d = data8[0]
        transfer_amount_30d = data9[0]
        fee_24h = data10[0]
        fee_7d = data11[0]
        fee_30d = data12[0]

        stats.append(["txs", "total_24h", "Total Txs", total_24h, ""])
        stats.append(["txs", "total_7d", "Total Txs", total_7d, ""])
        stats.append(["txs", "total_30d", "Total Txs", total_30d, ""])
        stats.append(["txs", "transfers_24h", "Transfers", transfers_24h, ""])
        stats.append(["txs", "transfers_7d", "Transfers", transfers_7d, ""])
        stats.append(["txs", "transfers_30d", "Transfers", transfers_30d, ""])
        stats.append(["txs", "transfer_amount_24h", "Transfer Volume", transfer_amount_24h, "WART"])
        stats.append(["txs", "transfer_amount_7d", "Transfer Volume", transfer_amount_7d, "WART"])
        stats.append(["txs", "transfer_amount_30d", "Transfer Volume", transfer_amount_30d, "WART"])
        stats.append(["txs", "fee_24h", "Fees Spent", fee_24h, "WART"])
        stats.append(["txs", "fee_7d", "Fees Spent", fee_7d, "WART"])
        stats.append(["txs", "fee_30d", "Fees Spent", fee_30d, "WART"])

        # mining stats
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM blocks WHERE forked=0 AND timestamp >= {round(time.time()) - 86400};")
                data = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM blocks WHERE forked=0 AND timestamp >= {round(time.time()) - 604800};")
                data2 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM blocks WHERE forked=0 AND timestamp >= {round(time.time()) - 2592000};")
                data3 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(DISTINCT minedBy) FROM blocks where forked=0 AND timestamp >= {round(time.time()) - 86400};")
                data4 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(DISTINCT minedBy) FROM blocks WHERE forked=0 AND timestamp >= {round(time.time()) - 604800};")
                data5 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(DISTINCT minedBy) FROM blocks WHERE forked=0 AND timestamp >= {round(time.time()) - 2592000};")
                data6 = cur.fetchone()
                cur.execute(
                    f"SELECT AVG(difficulty) FROM blocks where forked=0 AND timestamp >= {round(time.time()) - 86400};")
                data7 = cur.fetchone()
                cur.execute(
                    f"SELECT AVG(difficulty) FROM blocks where forked=0 AND timestamp >= {round(time.time()) - 604800};")
                data8 = cur.fetchone()
                cur.execute(
                    f"SELECT AVG(difficulty) FROM blocks where forked=0 AND timestamp >= {round(time.time()) - 2592000};")
                data9 = cur.fetchone()

        emission_24h = data[0] * 300000000
        emission_7d = data2[0] * 300000000
        emission_30d = data3[0] * 300000000
        miners_24h = data4[0]
        miners_7d = data5[0]
        miners_30d = data6[0]
        difficulty_24h = data7[0]
        difficulty_7d = data8[0]
        difficulty_30d = data9[0]

        stats.append(["mining", "emission_24h", "Emission", emission_24h, "WART"])
        stats.append(["mining", "emission_7d", "Emission", emission_7d, "WART"])
        stats.append(["mining", "emission_30d", "Emission", emission_30d, "WART"])
        stats.append(["mining", "miners_24h", "Miners", miners_24h, ""])
        stats.append(["mining", "miners_7d", "Miners", miners_7d, ""])
        stats.append(["mining", "miners_30d", "Miners", miners_30d, ""])
        stats.append(["mining", "difficulty_24h", "Average Difficulty", difficulty_24h, ""])
        stats.append(["mining", "difficulty_7d", "Average Difficulty", difficulty_7d, ""])
        stats.append(["mining", "difficulty_30d", "Average Difficulty", difficulty_30d, ""])

        # node stats
        with self.con:
            with self.con.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM nodes WHERE last_seen >= {round(time.time()) - 86400};")
                data = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM nodes WHERE last_seen >= {round(time.time()) - 604800};")
                data2 = cur.fetchone()
                cur.execute(
                    f"SELECT COUNT(*) FROM nodes WHERE last_seen >= {round(time.time()) - 2592000};")
                data3 = cur.fetchone()

        peers_24h = data[0]
        peers_7d = data2[0]
        peers_30d = data3[0]

        stats.append(["peers", "peers_24h", "Peers", peers_24h, ""])
        stats.append(["peers", "peers_7d", "Peers", peers_7d, ""])
        stats.append(["peers", "peers_30d", "Peers", peers_30d, ""])

        d = ""
        for stat in stats:
            d += f"('{stat[0]}', '{stat[1]}', '{stat[2]}', {stat[3]}, '{stat[4]}'), "
        d = d[:-2]

        with self.con:
            with self.con.cursor() as cur:
                cur.execute(f"INSERT INTO statistics (type, name, label, value, unit) VALUES {d} ON CONFLICT(name) DO UPDATE SET label=EXCLUDED.label, value=EXCLUDED.value, unit=EXCLUDED.unit;")

        t_stats = time.time() - t1
        print(f"Done! took {t_stats} seconds")