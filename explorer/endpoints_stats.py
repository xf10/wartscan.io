import time

from flask import Blueprint, render_template, abort, current_app, request, redirect, send_from_directory, make_response

from explorer.utils import timestamp_to_datetime, timestamp_to_time_since

s = None
turnstile = None


def construct_blueprint(exp, t):
    global s, turnstile

    stats = Blueprint('explorer', __name__, template_folder='templates')

    turnstile = t
    s = exp

    @stats.route("/stats", methods=['GET'])
    def statistics():
        return render_template("stats.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), escape=False)

    @stats.route("/stats/accounts", methods=['GET'])
    def stats_accounts():
        r2, r = s.get_stats_accounts()

        st = []
        accdata = []

        for d in r:
            accdata.append([d[""], d["24h"], d["7d"], d["30d"]])

        for d in r2.keys():
            st.append([d, r2[d]])

        return render_template("stats-accounts.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), stats=st, accdata=accdata)

    @stats.route("/stats/txs", methods=['GET'])
    def stats_txs():
        r2, r = s.get_stats_txs()

        st = []
        txdata = []

        for d in r:
            txdata.append([d[""], d["24h"], d["7d"], d["30d"]])

        for d in r2.keys():
            st.append([d, r2[d]])

        return render_template("stats-transactions.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), txdata=txdata)

    @stats.route("/stats/mining", methods=['GET'])
    def stats_mining():
        r2, r = s.get_stats_mining()

        st = []
        miningdata = []

        for d in r:
            miningdata.append([d[""], d["24h"], d["7d"], d["30d"]])

        for d in r2.keys():
            st.append([d, r2[d]])

        return render_template("stats-mining.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), miningdata=miningdata)

    return stats
