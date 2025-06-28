import datetime
import time

from flask import Blueprint, render_template, abort, current_app, request, redirect, send_from_directory, make_response
from utils import (replace_timestamps, timestamp_to_datetime, timestamp_to_date, replace_timestamps_with_time_since,
                   timestamp_to_time_since, calculate_expected_supply, calculate_blockreward, address_to_link_full,
                   address_to_link_short, tx_to_link_short)
import io
import csv

turnstile = None
s = None


def construct_blueprint(exp, t):
    global s, turnstile

    base = Blueprint('base', __name__, template_folder='templates')
    turnstile = t
    s = exp

    #
    #
    # ENDPOINTS
    #
    #

    @base.route("/", methods=['GET'])
    def index():
        height = s.get_height()
        tsupply = calculate_expected_supply(height) // 10 ** 8
        blockreward = calculate_blockreward(height) / 10 ** 8
        price = s.get_price()

        tps, tps_labels = s.get_latest_tps()
        l_blocks = s.get_latest_blocks()
        l_txs = s.get_latest_txs()
        for i, block in enumerate(l_blocks):
            l_blocks[i][2] = timestamp_to_time_since(block[2])
        for i, tx in enumerate(l_txs):
            l_txs[i][2] = timestamp_to_time_since(tx[2])

        return render_template("index.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               txdata=l_txs, blockdata=l_blocks, hashrate=s.get_hashrate(),
                               height=f'{height:,}', supply=f'{tsupply:,}', price=f"{price:.2f}",
                               marketcap=f'{round(price * tsupply):,}', blockreward=blockreward,
                               tps_data=tps, tps_labels=tps_labels)

    @base.route("/search", methods=['POST'])
    def search():
        if request.form:
            data = request.form["search"]
            if data and data.isalnum():
                # is account
                if len(data) == 48:
                    return redirect("/account/{}".format(data))
                # is tx
                elif len(data) == 64:
                    return redirect("/tx/{}".format(data.lower()))
                # is block
                elif len(data) <= 12 and data.isnumeric():
                    return redirect("/block/{}".format(data))
        # return 400
        abort(400)

    @base.route("/richlist")
    def top():
        page = request.args.get('p', default=1, type=int)

        if page < 1:
            page = 1
        elif page > 1000000:
            page = 1

        accdata = s.get_accounts(page)

        return render_template("richlist.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), page=page, accdata=accdata)

    @base.route("/mining")
    def mining():
        page = request.args.get('p', default=1, type=int)
        period = request.args.get('t', default="at", type=str)

        if page < 1:
            page = 1
        elif page > 1000000:
            page = 1

        if period != "24h" and period != "at":
            abort(400)

        if period == "24h":
            miningratios = s.get_miningratios(page, daily=True)
        else:
            miningratios = s.get_miningratios(page)

        return render_template("miningratios.html",
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               page=page, t=period, hrdata=miningratios)

    @base.route("/block/<height>", methods=['GET'])
    def getblock(height):
        page = request.args.get('p', default=1, type=int)

        if not height.isnumeric():
            abort(400)

        if page < 1:
            page = 1
        elif page > 1000000:
            page = 1

        block = s.get_block(height)
        if block is None:
            abort(404)
        block["minedBy"] = "<a class='underline' class=color-purple href=/account/" + block["minedBy"] + ">" + block["minedBy"] + "</a>"
        block["height"] = """<a href='/block/{}?p=1'><svg class="h-4 inline fill-black dark:fill-white" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><!--!Font Awesome Free 6.5.1 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2024 Fonticons, Inc.--><path d="M41.4 233.4c-12.5 12.5-12.5 32.8 0 45.3l160 160c12.5 12.5 32.8 12.5 45.3 0s12.5-32.8 0-45.3L109.3 256 246.6 118.6c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0l-160 160zm352-160l-160 160c-12.5 12.5-12.5 32.8 0 45.3l160 160c12.5 12.5 32.8 12.5 45.3 0s12.5-32.8 0-45.3L301.3 256 438.6 118.6c12.5-12.5 12.5-32.8 0-45.3s-32.8-12.5-45.3 0z"/></svg>
        </a> {} <a href='/block/{}?p=1'><svg class="h-4 inline fill-black dark:fill-white" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"><!--!Font Awesome Free 6.5.1 by @fontawesome - https://fontawesome.com License - https://fontawesome.com/license/free Copyright 2024 Fonticons, Inc.--><path d="M470.6 278.6c12.5-12.5 12.5-32.8 0-45.3l-160-160c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3L402.7 256 265.4 393.4c-12.5 12.5-12.5 32.8 0 45.3s32.8 12.5 45.3 0l160-160zm-352 160l160-160c12.5-12.5 12.5-32.8 0-45.3l-160-160c-12.5-12.5-32.8-12.5-45.3 0s-12.5 32.8 0 45.3L210.7 256 73.4 393.4c-12.5 12.5-12.5 32.8 0 45.3s32.8 12.5 45.3 0z"/></svg></a>""".format(int(height) - 1,
                                                                                                  height,
                                                                                                  int(height) + 1)
        block["timestamp"] = timestamp_to_datetime(block["timestamp"])

        txs = s.get_txs_for_block(height, page)

        if not txs:
            txs = []

        return render_template("block.html", title=f"Block {height}", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), page=page, height=height, block=block, txdata=txs)


    @base.route("/tx/<txhash>", methods=['GET'])
    def gettx(txhash):
        if not txhash.isalnum():
            abort(400)
        txhash = txhash.lower()
        tx = s.get_tx(txhash)
        confirmations = 0
        if tx is None:
            # tx not found, try to get from mempool
            tx = s.get_tx_from_mempool(txhash)
        else:
            tx["timestamp"] = timestamp_to_datetime(tx["timestamp"])
            confirmations = s.get_height() - tx["height"] + 1

        # tx still not found
        if tx is None:
            return render_template("explorer.html",
                                   c1="Transaction not found! <a class='underline' href='/'>return home</a>")

        return render_template("transaction.html", title="Transaction {}".format(txhash),
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), tx=tx, confirmations=confirmations)


    @base.route("/mempool", methods=['GET'])
    def getmempool():
        page = request.args.get('p', default=1, type=int)

        if page < 1:
            page = 1
        elif page > 1000000:
            page = 1

        c1 = ""
        c1 += "<h2>Mempool</h2>"

        txs = s.get_mempool(page)
        if not txs:
            txs=[]

        return render_template("mempool.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               page=page, txdata=txs)


    @base.route("/account/<address>", methods=['GET'])
    def getaccount(address):
        page = request.args.get('p', default=1, type=int)

        if not address.isalnum() or len(address) != 48:
            abort(400)

        # force lower case
        address = address.lower()

        if page < 1:
            page = 1
        elif page > 1000000:
            page = 1

        c2 = "<center>"
        acc = s.get_account(address)
        acc["address"] = "<h3 class='text-purple-500'>" + acc["address"] + "</h3>"
        value = round(acc["balance"] * s.get_price(), 2)
        acc["value"] = f"${value:,.2f}"
        bal = acc["balance"]
        acc["balance"] = f"{bal:,.8f}"

        # janushash mining ratio
        miningratio = acc["ratio"]
        if miningratio is not None and miningratio > 0:
            if miningratio >= 60:
                acc["ratio"] = f"<p class='text-red-500 inline'>{str(round(miningratio, 4))}</p>"
            elif miningratio >= 45:
                acc["ratio"] = f"<p class='text-orange-300 inline'>{str(round(miningratio, 4))}</p>"
            else:
                acc["ratio"] = f"<p class='text-green-500 inline'>{str(round(miningratio, 4))}</p>"
        else:
            acc["ratio"] = 0

        txs = s.get_txs_for_account(address, page)
        if txs is None:
            return render_template("account.html", title="Address {}".format(address),
                                   lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                                   servertime=timestamp_to_datetime(round(time.time())), page=page, txdata=[],
                                   address=address, label=acc["label"], balance=acc["balance"], value=acc["value"],
                                   miningratio=acc["ratio"] if acc["ratio"] > 0 else "")

        for i, tx in enumerate(txs):
            txs[i]["timestamp"] = timestamp_to_datetime(tx["timestamp"])


        return render_template("account.html", title="Address {}".format(address),
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), page=page, txdata=txs,
                               address=address, balance=acc["balance"], value=acc["value"],
                               miningratio=acc["ratio"] if miningratio > 0.0 else "", label=acc["label"])


    @base.route("/forked-blocks", methods=['GET'])
    def getforkedblocks():
        page = request.args.get('p', default=1, type=int)

        if page < 1:
            page = 1
        elif page > 1000000:
            page = 1
        r = s.get_forked_blocks(page)
        blockdata = []
        for block in r:
            blockdata.append([block["height"], block["hash"], timestamp_to_datetime(block["timestamp"]),
                              "<a href=/account/" + block["minedBy"] + ">" + block["minedBy"][:18] + "..." + "</a>"])

        return render_template("forked-blocks.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               page=page, blockdata=blockdata)


    @base.route("/calculator", methods=['GET'])
    def mining_calculator():
        price = s.get_price()
        height = s.get_height()
        blockreward = calculate_blockreward(height) / 10 ** 8

        return render_template("calc.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), network_hashrate=s.get_hashrate(),
                               price=f"{price:.2f}", blockreward=blockreward)


    @base.route("/links", methods=['GET'])
    def links():
        return render_template("links.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())))

    # @base.route("/nodes", methods=['GET'])
    # def nodes():
    #     page = request.args.get('p', default=1, type=int)
    #
    #     if page < 1:
    #         page = 1
    #     elif page > 1000:
    #         page = 1
    #
    #     c = "<center>"
    #     c += json2html.convert(replace_timestamps(s.get_nodes(page)), escape=False)
    #
    #     c += "<a href='/nodes?p={}'><--</a> {} <a href='/nodes?p={}'>--></a>".format(
    #         page - 1, page, page + 1)
    #
    #     return render_template("explorer.html", title="Nodes", c1=c)


    @base.route("/csv-export", methods=['GET'])
    def csv_exporter():
        address = request.args.get('address', default="", type=str)
        if not address.isalnum() and address != "":
            abort(400)

        return render_template("csv-export.html",
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), address=address, escape=False)

    @base.route("/exportcsv", methods=["POST"])
    def exportcsv():
        if not request.form:
            abort(400)

        if not turnstile.verify():
            abort(403)

        address = request.form["address"]
        try:
            timestamp_start = round(time.mktime(datetime.datetime.strptime(request.form["start_date"], "%Y-%m-%dT%H:%M").timetuple()))
            timestamp_end = round(time.mktime(datetime.datetime.strptime(request.form["end_date"], "%Y-%m-%dT%H:%M").timetuple()))
        except Exception:
            abort(400)

        if not address.isalnum():
            abort(400)

        v = s.get_txs_csv(address, timestamp_start, timestamp_end)

        if v is None:
            return render_template("explorer.html",
                                   lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                                   servertime=timestamp_to_datetime(round(time.time())), c1="No transactions found for specified time range!", escape=False)

        si = io.StringIO()
        cw = csv.writer(si)
        cw.writerows(v)
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = "attachment; filename=export.csv"
        output.headers["Content-type"] = "text/csv"
        return output

    @base.route("/status", methods=['GET'])
    def status():
        latest_logs = s.get_latest_logs()
        avg_time_to_find_block = s.get_blocktime_delta()
        supply_delta = s.get_supply_delta()

        for i, log in enumerate(latest_logs):
            latest_logs[i]["timestamp"] = timestamp_to_time_since(log["timestamp"])


        return render_template("status.html", logs=latest_logs, blocktime_delta=avg_time_to_find_block,
                               supply_delta=supply_delta,
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), escape=False)

    @base.route("/about", methods=['GET'])
    def about():

        return render_template("about.html",
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), escape=False)

    @base.route("/difficulty", methods=['GET'])
    def difficulty():
        height = s.get_height()
        latest_block = s.get_block(height)
        diff = round(float(latest_block["difficulty"]) / 1000000000000, 2)
        epoch_start = (height // 8640) * 8640 + 1
        epoch_end = epoch_start + 8639
        first_block = s.get_block(epoch_start)
        epoch_start_time = timestamp_to_datetime(first_block["timestamp"])
        blocktime = round((latest_block["timestamp"] - first_block["timestamp"]) / (height - epoch_start), 2)
        epoch_end_time = timestamp_to_datetime(latest_block["timestamp"] + (epoch_end - height) * blocktime)
        diff_next = round((diff / blocktime) * 20, 2)
        return render_template("difficulty.html", diff=diff, height=height, epoch_start=epoch_start,
                               epoch_end=epoch_end, blocktime=blocktime, diff_next=diff_next,
                               epoch_start_time=epoch_start_time, epoch_end_time=epoch_end_time,
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), escape=False)


    @base.route("/halving", methods=['GET'])
    def halving():
        height = s.get_height()
        block = s.get_block(height)
        reward = calculate_blockreward(height) / (10 ** 8)

        halving_height = (height // 3153600 + 1) * 3153600
        halving_timestamp = timestamp_to_datetime(block["timestamp"] + (halving_height - height) * 20)
        reward_next = calculate_blockreward(halving_height+1) / (10 ** 8)


        return render_template("halving.html", height=height, reward=reward,
                               halving_height=halving_height, halving_timestamp=halving_timestamp,
                               reward_next=reward_next,
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), escape=False)
    return base
