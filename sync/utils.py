from datetime import datetime
import time

import psycopg2.extras
import pytz


# chain utils

def buildblock(blockdata):
    body = blockdata["body"]
    header = blockdata["header"]

    block = {
        "height": blockdata["height"],
        "difficulty": header["difficulty"],
        "timestamp": header["timestamp"],
        "hash": header["hash"],
        "merkleRoot": header["merkleroot"],
        "nonce": header["nonce"],
        "floatSha256t": header["pow"]["floatSha256t"],
        "floatVerus": header["pow"]["floatVerus"],
        "prevHash": header["prevHash"],
        "raw": header["raw"],
        "target": header["target"],
        "version": header["version"],
        "transactions": body["rewards"] + body["transfers"]
    }

    return block

def buildtx(txdata):
    # reward
    if len(txdata.keys()) == 4:
        tx = {
            "type": "reward",
            "hash": txdata["txHash"],
            "amount": txdata["amountE8"],
            "fee": 0,
            "nonce": "null",
            "pinHeight": "null",
            "sender": "",
            "recipient": txdata["toAddress"],
        }
    # transfer
    elif len(txdata.keys()) == 9:
        tx = {
            "type": "transfer",
            "hash": txdata["txHash"],
            "amount": txdata["amountE8"],
            "fee": txdata["feeE8"],
            "nonce": txdata["nonceId"],
            "pinHeight": txdata["pinHeight"],
            "sender": txdata["fromAddress"],
            "recipient": txdata["toAddress"],
        }

    return tx


def calculate_mined_by(block):
    return block["transactions"][0]["toAddress"]


def calculate_total_fees(block):
    t_fee = 0
    for tx in block["transactions"]:
        t_fee += tx["fee"]
    return t_fee


def calculate_blockreward(height):
    return round(3 * 10 ** 8 * (0.5 ** ((height - 1) // 3153600)))


def calculate_expected_supply(height):
    s = 0
    r = 3 * 10 ** 8

    while height >= 3153600:
        s += 3153600 * r
        r = r / 2
        height -= 3153600
    s += height * r

    return round(s)

def timestamp_to_datetime(timestamp):
    try:
        d = datetime.fromtimestamp(timestamp, pytz.UTC)
        return d.strftime("%m/%d/%Y, %H:%M:%S")
    except Exception as e:
        return timestamp



