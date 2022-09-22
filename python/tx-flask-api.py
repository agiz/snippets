import datetime
import sqlite3

from flask import Flask
from flask_restful import Api, Resource, reqparse

app = Flask(__name__)
api = Api(app)

conn1 = sqlite3.connect('file:./tx.sqlite?mode=ro', uri=True, check_same_thread=False)
c1 = conn1.cursor()

conn2 = sqlite3.connect('file:./db.sqlite?mode=ro', uri=True, check_same_thread=False)
c2 = conn2.cursor()

def getTransactions(user_id, txs):
    user = {}

    for tx in txs:
        from_account = tx[0]
        to_account = tx[1]
        time = datetime.datetime.fromtimestamp(float(tx[2]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
        comment = tx[3]
        amount = round(tx[4], 8)

        if user_id == from_account:
            if from_account not in user:
                user[from_account] = {}

            if 'transactions' not in user[from_account]:
                user[from_account]['transactions'] = []

            user[from_account]['transactions'].append({ 'time': time, 'amount': -amount, 'comment': comment })

        if user_id == to_account:
            if to_account not in user:
                user[to_account] = {}

            if 'transactions' not in user[to_account]:
                user[to_account]['transactions'] = []

            user[to_account]['transactions'].append({ 'time': time, 'amount': amount, 'comment': comment })

    if user_id in user:
        return user[user_id]

    return {}

def getTransactions2(user_id, txs, x):
    user = {}
    duplicates = 0

    for tx in txs:
        if tx in x:
            duplicates += 1

        from_account = tx[0]
        to_account = tx[1]
        time = datetime.datetime.fromtimestamp(float(tx[2]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
        comment = tx[3]
        amount = round(tx[4], 8)

        if user_id == from_account:
            if from_account not in user:
                user[from_account] = {}

            if 'transactions' not in user[from_account]:
                user[from_account]['transactions'] = []

            user[from_account]['transactions'].append({ 'time': time, 'amount': -amount, 'comment': comment })

        if user_id == to_account:
            if to_account not in user:
                user[to_account] = {}

            if 'transactions' not in user[to_account]:
                user[to_account]['transactions'] = []

            user[to_account]['transactions'].append({ 'time': time, 'amount': amount, 'comment': comment })

    print(user_id, 'duplicates (getTransactions2):', duplicates)
    if user_id in user:
        return user[user_id]

    return {}

#
# Deposits
#

def getDeposits(user_id, txs):
    user = {}

    for tx in txs:
        time = datetime.datetime.fromtimestamp(float(tx[0]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
        account = tx[1]
        amount = round(tx[2], 8)
        address = tx[3]
        txid = tx[4]

        if account not in user:
            user[account] = {}

        if 'deposits' not in user[account]:
            user[account]['deposits'] = []

        user[account]['deposits'].append({ 'time': time, 'amount': amount, 'txid': txid, 'address': address })

    if user_id in user:
        return user[user_id]

    return {}


#
# Withdraws
#

def getWithdraws(user_id, txs):
    user = {}

    for tx in txs:
        time = datetime.datetime.fromtimestamp(float(tx[0]) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
        account = tx[1]
        amount = round(tx[2], 8)
        address = tx[3]
        txid = tx[4]

        if account not in user:
            user[account] = {}

        if 'withdraws' not in user[account]:
            user[account]['withdraws'] = []

        user[account]['withdraws'].append({ 'time': time, 'amount': amount, 'txid': txid, 'address': address })

    if user_id in user:
        return user[user_id]

    return {}


def removeDuplicates(a, b, user_id):
    txs = []
    duplicates = 0

    for tx in b:
        txs.append(tx)

    for tx1 in a:
        duplicate = False

        for tx2 in b:
            if tx1['time'] == tx2['time'] and tx1['amount'] == tx2['amount'] and tx1['comment'] == tx2['comment']:
                duplicates += 1
                duplicate = True
                break

        if not duplicate:
            txs.append(tx1)

    print(user_id, 'duplicates (removeDuplicates):', duplicates)
    return txs


class User(Resource):
    def get(self, name):
        t = c1.execute("SELECT id FROM account WHERE name = '%s'" % (name,))
        user_id = t.fetchone()

        if user_id is not None and len(user_id) == 1:
            user_id = user_id[0]
        else:
            return "User not found", 404

        print(user_id)

        t = c2.execute("SELECT from_account, to_account, time, comment, amount FROM txs WHERE (from_account = '%s' OR to_account = '%s') AND time BETWEEN 1514764800000 AND 1546300800000" % (user_id, user_id,))
        txs_list_2 = t.fetchall()

        t = c1.execute("SELECT from_account, to_account, time, comment, amount FROM txs WHERE (from_account = '%s' OR to_account = '%s') AND time BETWEEN 1514764800000 AND 1546300800000" % (user_id, user_id,))
        txs_list_1 = t.fetchall()

        x = frozenset(txs_list_1) & frozenset(txs_list_2)

        tx_res_1 = getTransactions(user_id, txs_list_2)
        tx_res_2 = getTransactions2(user_id, txs_list_1, x)

        t = c1.execute("SELECT time, account, amount, address, txid FROM deposit WHERE account = '%s' AND time BETWEEN 1514764800000 AND 1546300800000" % (user_id,))
        deposit_list = t.fetchall()
        deposit_res = getDeposits(user_id, deposit_list)

        t = c1.execute("SELECT time, account, amount, address, txid FROM withdraw WHERE account = '%s' AND time BETWEEN 1514764800000 AND 1546300800000" % (user_id,))
        withdraw_list = t.fetchall()
        withdraw_res = getWithdraws(user_id, withdraw_list)

        txs = []
        if 'transactions' in tx_res_1 and 'transactions' in tx_res_2:
            txs = removeDuplicates(tx_res_1['transactions'], tx_res_2['transactions'], user_id)
        elif 'transactions' in tx_res_1:
            txs = removeDuplicates(tx_res_1['transactions'], {}, user_id)
        elif 'transactions' in tx_res_2:
            txs = removeDuplicates(tx_res_2['transactions'], {}, user_id)

        user = { "transactions": txs, **deposit_res, **withdraw_res }

        return user, 200

api.add_resource(User, "/user/<string:name>")

app.run(debug=False, host='0.0.0.0', port='5005')
