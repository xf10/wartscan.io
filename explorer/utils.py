from datetime import datetime
import pytz
import time


# utils


def timestamp_to_datetime(timestamp):
    try:
        d = datetime.fromtimestamp(timestamp, pytz.UTC)
        return d.strftime("%m/%d/%Y, %H:%M:%S")
    except Exception as e:
        return timestamp


def timestamp_to_date(timestamp):
    try:
        d = datetime.fromtimestamp(timestamp, pytz.UTC)
        return d.strftime("%m/%d/%Y")
    except Exception as e:
        return timestamp


def replace_timestamps(l):
    if l:
        if l[0]:
            for i in l:
                if "Timestamp" in i.keys():
                    i["Timestamp"] = timestamp_to_datetime(i["Timestamp"])
                if "First Seen" in i.keys():
                    i["First Seen"] = timestamp_to_date(i["First Seen"])
                    i["Last Seen"] = timestamp_to_datetime(i["Last Seen"])
            return l
    return None


def replace_timestamps_with_time_since(l):
    if l:
        if l[0]:
            for i in l:
                if "Timestamp" in i.keys():
                    i["Timestamp"] = timestamp_to_time_since(i["Timestamp"])
                if "First Seen" in i.keys():
                    i["First Seen"] = timestamp_to_date(i["First Seen"])
                    i["Last Seen"] = timestamp_to_time_since(i["Last Seen"])
            return l
    return None


def timestamp_to_time_since(timestamp):
    seconds_since = round(time.time()) - timestamp
    time_since = f"{seconds_since} seconds ago"

    if seconds_since == 1:
        time_since = f"a second ago"

    if seconds_since >= 60:
        minutes_since = seconds_since // 60
        if minutes_since == 1:
            time_since = f"a minute ago"
        else:
            time_since = f"{minutes_since} minutes ago"

    return time_since

def calculate_blockreward(height):
    return round(3 * 10 ** 8 * (0.5 ** (height // 3153601)))


def calculate_expected_supply(height):
    s = 0
    r = 3 * 10 ** 8

    while height >= 3153600:
        s += 3153600 * r
        r = r / 2
        height -= 3153600
    s += height * r

    return round(s)


def capitalize_dict(d):
    dict_new = {}
    for k, v in d.items():
        dict_new[k[0].upper() + k[1:]] = v
    return dict_new
