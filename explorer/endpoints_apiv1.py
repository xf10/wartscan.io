from flask import Blueprint, render_template, abort, current_app, request
import json
import requests
from flask_restx import Api, Resource, reqparse

s = None


def construct_blueprint(scanner):
    global s

    bp = Blueprint('apiv1', __name__, template_folder='templates')
    api = Api(bp, version='1.0', title='wartscan.io API', description='Request rate should not exceed 3600 requests per hour / 60 per minute. Feel free to reach out in case you need custom limits.')

    ns = api.namespace("v1", description="API Methods")

    s = scanner

    #
    # API
    #

    # stats endpoints


    @ns.route("/stats/height", methods=['GET'])
    class Height(Resource):
        @api.response(200, 'Success')
        def get(self):
            return s.get_height()


    @ns.route("/stats/hashrate", methods=['GET'])
    class Hashrate(Resource):
        @api.response(200, 'Success')
        def get(self):
            return s.get_hashrate()


    @ns.route("/stats/totalsupply", methods=['GET'])
    class TotalSupply(Resource):
        @api.response(200, 'Success')
        def get(self):
            return s.calculate_expected_supply(s.getlastblockindb())


    @ns.route("/stats/totalaccounts", methods=['GET'])
    class TotalAccounts(Resource):
        @api.response(200, 'Success')
        def get(self):
            return s.get_account_total()


    @ns.route("/stats/totaltxs", methods=['GET'])
    class TotalTxs(Resource):
        @api.response(200, 'Success')
        def get(self):
            return s.get_total_txs()

    # account endpoints

    @ns.route("/accounts/balance", methods=['GET'])
    @ns.doc(params={'address': '48-Character Hexadecimal Address'})
    class AccountsBalance(Resource):
        @api.response(200, 'Success')
        @api.response(400, 'Invalid Input Parameters')
        def get(self):
            address = request.args.get('address', type=str)
            if address:
                if not address.isalnum() or not len(address) == 48:
                    return {"error": "invalid address format"}, 400

                # force lower case
                address = address.lower()

                return s.get_account(address)["balance"]
            else:
                return {"error": "invalid address format"}

    @ns.route("/accounts/transactions", methods=['GET'])
    @ns.doc(params={'address': '48-character hexadecimal address',
                    'p': 'Page Index (starting at 1) (25 transactions per page)'})
    class AccountsTransactions(Resource):
        @api.response(200, 'Success')
        @api.response(400, 'Invalid Input Parameters')
        def get(self):
            address = request.args.get('address', type=str)
            page = request.args.get('p', default=1, type=int)
            if page < 1:
                page = 1
            elif page > 1000000:
                page = 1
            if address:
                if not address.isalnum() or not len(address) == 48:
                    return {"error": "invalid address format"}, 400

                # force lower case
                address = address.lower()

                txs = s.get_txs_for_account(address, page)
                if txs is None:
                    txs = {"error": "no transactions found"}, 400
                return txs
            else:
                return {"error": "invalid address format"}, 400

    # block endpoints

    @ns.route("/blocks/block", methods=['GET'])
    @ns.doc(params={'height': 'Integer block height (First block is 1 not 0)'})
    class BlocksBlock(Resource):
        @api.response(200, 'Success')
        @api.response(400, 'Invalid Input Parameters')
        def get(self):
            height = request.args.get('height', type=int)
            if height:
                if not height >= 1:
                    return {"error": "invalid height"}, 400
                block = s.get_block(height)
                if block is None:
                    block = {"error": "invalid height"}, 400
                return block
            else:
                return {"error": "invalid height"}, 400

    @ns.route("/blocks/last20", methods=['GET'])
    class BlocksLast20(Resource):
        @api.response(200, 'Success')
        def get(self):
            # height = request.args.get('height', type=int)
            blocks = s.get_last20_blocks()
            return blocks

    @ns.route("/blocks/transactions", methods=['GET'])
    @ns.doc(params={'height': 'Integer block height (First block is 1 not 0)',
                    'p': 'Page Index (starting at 1) (25 transactions per page)'})
    class BlocksTransactions(Resource):
        @api.response(200, 'Success')
        @api.response(400, 'Invalid Input Parameters')
        def get(self):
            height = request.args.get('height', type=int)
            page = request.args.get('p', default=1, type=int)
            if page < 1:
                page = 1
            elif page > 1000000:
                page = 1
            if height:
                if not height >= 1:
                    return {"error": "invalid height"}, 400
                txs = s.get_txs_for_block(height, page)
                if txs is None:
                    txs = {"error": "no transactions found"}, 400
                return txs
            else:
                return {"error": "invalid height"}, 400

    # txs endpoint


    @ns.route("/transactions/transaction", methods=['GET'])
    @ns.doc(params={'hash': '64-character hexadecimal hash'})
    class TransactionsTransaction(Resource):
        @api.response(200, 'Success')
        @api.response(400, 'Invalid Input Parameters')
        def get(self):
            hash = request.args.get('hash', type=str)
            if hash:
                hash = hash.lower()
                if not len(hash) == 64 and hash.isalnum():
                    return {"error": "invalid hash format"}, 400
                tx = s.get_tx(hash)
                if tx is None:
                    tx = {"error": "no transaction found"}, 400
                return tx
            else:
                return {"error": "invalid hash format"}, 400

    return bp