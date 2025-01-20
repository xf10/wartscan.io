import time
from flask import Flask
import threading
from jobmanager import JobManager
from logger import logger
import db

jobman = JobManager()

# Metrics
app = Flask(__name__)


@app.route('/metrics')
def metrics():
    return jobman.get_metrics()


def metrics_server():
    app.run(host='0.0.0.0', port=3131, debug=False, use_reloader=False)


t = threading.Thread(target=metrics_server)
logger.info('Starting metrics server')
t.start()

# initialize db
db.createtables(db.db_connect())

# DEBUG
# jobman.chainsync.con = db.db_connect()
# jobman.chainsync.reset_balances()
# jobman.chainsync.calculate_balances(1)
# jobman.chainsync.con.close()
# jobman.chainsync.con = None

# main loop
while True:
    t1 = time.perf_counter()

    jobman.tick()

    t2 = time.perf_counter()-t1

    if t2 < 5:
        time.sleep(5-t2)
