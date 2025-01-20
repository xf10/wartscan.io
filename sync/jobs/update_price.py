import time

import requests

from jobs.job import Job
import utils
from logger import logger
import db


class UpdatePrice(Job):

    def __init__(self):
        super().__init__()
        self.con = None

    def execute(self):
        logger.info("Executing UpdatePrice")

        # init db connection
        self.con = db.db_connect()

        status = self.update_price()

        # close db connection
        self.con.close()

        return status

    def update_price(self):
        try:
            r = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=warthog&vs_currencies=usd&include_24hr_vol=true&include_24hr_change=true&precision=2", timeout=5).json()
            p = r["warthog"]["usd"]

            db.commit_sql(self.con,
                          f"INSERT INTO price_data (timestamp, price) values({round(time.time())}, {round(p, 4)});")
            return "success"
        except Exception as e:
            logger.error(f"Error while executing UpdatePrice: {e}")
            return "fail"
