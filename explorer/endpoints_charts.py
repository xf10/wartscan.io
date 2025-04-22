import time

from flask import Blueprint, render_template, abort, current_app, request, redirect, send_from_directory, make_response

from utils import timestamp_to_time_since, timestamp_to_datetime, timestamp_to_date

s = None
turnstile = None


def construct_blueprint(exp, t):
    global s, turnstile

    charts = Blueprint('charts', __name__, template_folder='templates')

    turnstile = t
    s = exp

    @charts.route("/charts", methods=['GET'])
    def charts_overview():
        return render_template("charts.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), escape=False)

    # wealthchart
    @charts.route("/charts/wealthchart", methods=['GET'])
    def chart_wealthchart():
        wealth_data = []
        wealth_labels = []
        accounts = s.gettop100()

        for account in accounts:
            wealth_data.append(account[1])
            wealth_labels.append(account[0])

        return render_template("chart_wealthchart.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), wealth_data=wealth_data,
                               wealth_labels=wealth_labels, escape=False)

    # 24h hashrate + difficutly
    @charts.route("/charts/24hhashrate", methods=['GET'])
    def chart_24h_hashrate():
        last_960 = [i for i in range(s.get_height() - 4319, s.get_height() + 1)]

        diffs = s.get_difficulty_for_blocks(4320)
        difficulty_data = list(reversed(diffs))
        difficulty_labels = last_960

        difficulty_dataset = str([{"x": height, "y": difficulty_data[i]} for i, height in enumerate(last_960)]).replace(
            "'", "")

        hashrate_data = list(reversed(s.get_hashrate_for_blocks()))

        hashrate_dataset = str([{"x": height, "y": hashrate_data[i]} for i, height in enumerate(last_960)]).replace("'",
                                                                                                                    "")

        return render_template("chart_24h_hashrate.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               difficulty_dataset=difficulty_dataset,
                               hashrate_dataset=hashrate_dataset, min=difficulty_labels[0], max=difficulty_labels[4319],
                               escape=False)

    # 24h tps
    @charts.route("/charts/24htps", methods=['GET'])
    def chart_24h_tps():
        last_960 = [i for i in range(s.get_height() - 4319, s.get_height() + 1)]

        transactions_data = list(reversed(s.get_txn_for_blocks()))
        transactions_labels = last_960

        transactions_dataset = str(
            [{"x": height, "y": transactions_data[i]} for i, height in enumerate(last_960)]).replace("'", "")

        return render_template("chart_24h_tps.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               transactions_dataset=transactions_dataset,
                               min=transactions_labels[0], max=transactions_labels[4319], escape=False)

    # 24h blocktime
    @charts.route("/charts/24hblocktime", methods=['GET'])
    def chart_24h_blocktime():
        last_960 = [i for i in range(s.get_height() - 4319, s.get_height() + 1)]

        blocktime_data = list(reversed(s.get_blocktime_for_blocks(4321)))
        blocktime_labels = last_960

        blocktime_dataset = str([{"x": height, "y": blocktime_data[i]} for i, height in enumerate(last_960)]).replace(
            "'", "")

        return render_template("chart_24h_blocktime.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               blocktime_dataset=blocktime_dataset,
                               min=blocktime_labels[0], max=blocktime_labels[4319], escape=False)

    # daily hashrate
    @charts.route("/charts/dailyhashrate", methods=['GET'])
    def chart_daily_hashrate():
        hashrate_data = []
        hashrate_labels = []
        difficulty_data = []
        d = s.get_historic_chart_data()
        if d:
            for day in d:
                for k in list(day.keys()):
                    hashrate_data.append(day[k][0])
                    hashrate_labels.append(timestamp_to_date(k))
                    difficulty_data.append(day[k][1])

        difficulty_labels = hashrate_labels

        return render_template("chart_daily_hashrate.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), difficulty_data=difficulty_data,
                               difficulty_labels=difficulty_labels,
                               hashrate_data=hashrate_data, hashrate_labels=hashrate_labels, escape=False)

    # daily tps
    @charts.route("/charts/dailytps", methods=['GET'])
    def chart_daily_tps():
        transactions_labels = []
        transactions_data = []
        d = s.get_historic_chart_data()
        if d:
            for day in d:
                for k in list(day.keys()):
                    transactions_labels.append(timestamp_to_date(k))
                    transactions_data.append(day[k][2])

        return render_template("chart_daily_tps.html", lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())),
                               transactions_data=transactions_data, transactions_labels=transactions_labels,
                               escape=False)

    # daily active addresses
    @charts.route("/charts/dailyactive", methods=['GET'])
    def chart_daily_active():
        dailyactive_labels = []
        dailyactive_data = []
        d = s.get_historic_chart_data()
        if d:
            for day in d:
                for k in list(day.keys()):
                    dailyactive_labels.append(timestamp_to_date(k))
                    dailyactive_data.append(day[k][3])

        return render_template("chart_daily_activeaddresses.html",
                               lastblock=timestamp_to_time_since(s.get_last_block_seen()),
                               servertime=timestamp_to_datetime(round(time.time())), dailyactive_data=dailyactive_data,
                               dailyactive_labels=dailyactive_labels, escape=False)

    return charts
