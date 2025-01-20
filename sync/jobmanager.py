import time
from hostmanager import HostManager
from jobs.chainsync import ChainSync
from jobs.charts_calc import ChartsCalc
from jobs.stats_calc import StatsCalc
from prometheus_client import Counter, generate_latest, Histogram

from jobs.update_price import UpdatePrice
from logger import logger
from notifier import Notifier
import db


class JobManager:
    def __init__(self):
        logger.info("Initializing Job Manager")
        # variables
        self.last_tick = 0
        self.last_price = 0
        self.last_stats = 0
        self.last_charts = 0

        self.hostman = HostManager()
        self.notifier = Notifier()

        self.chainsync = ChainSync(self.hostman, self.notifier)
        self.statscalc = StatsCalc()
        self.chartscalc = ChartsCalc()
        self.updateprice = UpdatePrice()

        # prometheus metrics
        self.chainsynctime_metric = Histogram('chainsync_time_seconds', 'Chain Update Time',
                                          buckets=[0.001, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 3, 4, 5,
                                                   7.5, 10, 15, 20])
        self.updatepricetime_metric = Histogram('updateprice_time_seconds', 'Price Update Time',
                                              buckets=[0.001, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 3, 4,
                                                       5, 7.5, 10, 15, 20])
        self.statscalctime_metric = Histogram('statscalc_time_seconds', 'Stats Update Time',
                                              buckets=[0.001, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 3, 4,
                                                       5, 7.5, 10, 15, 20])
        self.chartscalctime_metric = Histogram('chartscalc_time_seconds', 'Charts Update Time',
                                           buckets=[0.001, 0.01, 0.025, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 3, 4, 5,
                                                    7.5, 10, 15, 20])

        self.schedule = [{"jobName": "chainsync", "obj": self.chainsync, "lastRun": 0, "interval": 5,
                          "metric": self.chainsynctime_metric},
                         {"jobName": "updateprice", "obj": self.updateprice, "lastRun": 0, "interval": 300,
                          "metric": self.updatepricetime_metric},
                         {"jobName": "statscalc", "obj": self.statscalc, "lastRun": 0, "interval": 3600,
                          "metric": self.statscalctime_metric},
                         {"jobName": "chartscalc", "obj": self.chartscalc, "lastRun": 0, "interval": 14400,
                          "metric": self.chartscalctime_metric}]

    def tick(self):
        tick_t_s = time.time()

        logger.debug("Job Manager Tick")

        for job in self.schedule:
            # skip if not scheduled yet
            if job["lastRun"] + job["interval"] > tick_t_s:
                continue

            t_s = time.perf_counter()
            rt_s = time.time()
            status = "fail"
            try:
                status = job["obj"].execute()
            except Exception as e:
                logger.error(f"Unexpected exception while executing {job['jobName']}: {e}")

            job["lastRun"] = tick_t_s
            d = time.perf_counter() - t_s
            job["metric"].observe(d)
            self.log_job(job["jobName"], status, rt_s, d)

        # decide if we should use a different host for next run
        self.hostman.decide()

    def log_job(self, job, status, timestamp, duration):
        try:
            con = db.db_connect()
            db.commit_sql(con, f"DELETE FROM logs WHERE timestamp<{round(time.time()) - 86400}")
            db.commit_sql(con,f"INSERT INTO logs (event, status, timestamp, duration) values('{job}', '{status}',"
                              f" {timestamp}, {duration});")
        except Exception as e:
            logger.error(f"Error while inserting logs: {e}")

    def get_metrics(self):
        return generate_latest()
