import datetime
import logging
import pickle
import time
from alice_blue import *
import configparser
import os

logging.basicConfig(level = logging.DEBUG,filename='alice_log.txt')

config=configparser.ConfigParser()
config.read('config_alice.ini')

uname = config['DEFAULT']['username']
passw = config['DEFAULT']['pass']
apis = config['DEFAULT']['api_secret']
appid = config['DEFAULT']['app_id']
access_token = AliceBlue.login_and_get_access_token(username=uname, password=passw, twoFA='1995',  api_secret=apis, app_id = appid)

alice = AliceBlue(username=uname,password=passw,access_token=access_token)


def name_to_strike(name):
    return int(name.replace('CE', '').replace('PE', ''))

def process_nifty_tradelist(tradelist,expiry):
    calls = {}
    puts = {}
    for key in tradelist.keys():
        if 'CE' in key:
            calls[name_to_strike(key)] = tradelist[key]
        else:
            puts[name_to_strike(key)] = tradelist[key]
    calls = dict(sorted(calls.items(), key=lambda x: x[1], reverse=True))
    puts = dict(sorted(puts.items(), key=lambda x: x[1], reverse=True))
    orders = []
    for strike in calls.keys():
        ins = alice.get_instrument_for_fno(symbol='NIFTY', expiry_date=expiry, is_fut=False, strike=strike, is_CE=True)
        no_of_lots = abs(calls[strike])
        if calls[strike] > 0:
            trans_type = TransactionType.Buy
        else:
            trans_type = TransactionType.Sell
        order = {'transaction_type': trans_type,
                  'instrument': ins,
                  'quantity': int(ins.lot_size) * no_of_lots, 'order_type': OrderType.Market,
                  'product_type': ProductType.Delivery,
                  # 'price' : None,
                  'is_amo': False
                  }
        orders.append(order)
    for strike in puts.keys():
        ins = alice.get_instrument_for_fno(symbol='NIFTY', expiry_date=expiry, is_fut=False, strike=strike, is_CE=False)
        no_of_lots = abs(puts[strike])
        if puts[strike] > 0:
            trans_type = TransactionType.Buy
        else:
            trans_type = TransactionType.Sell
        order = {'transaction_type': trans_type,
                  'instrument': ins,
                  'quantity': int(ins.lot_size) * no_of_lots, 'order_type': OrderType.Market,
                  'product_type': ProductType.Delivery,
                  # 'price' : None,
                  'is_amo': False
                  }
        orders.append(order)
    results = []
    for order in orders:
        results.append(alice.place_order(order_type=order['order_type'],transaction_type=order['transaction_type'],
                                         instrument=order['instrument'],quantity=order['quantity'],
                                         product_type=order['product_type'],is_amo=order['is_amo']))
        time.sleep(0.1)
    avg_price, status, timestamp = fill_prices(results)
    oids = list(status.keys())
    output = {}
    call_keys = [str(i)+'CE' for i in list(calls.keys())]
    put_keys = [str(i)+'PE' for i in list(puts.keys())]
    tradelist_keys =[]
    for key in call_keys:
        tradelist_keys.append(key)
    for key in put_keys:
        tradelist_keys.append(key)

    for i in range(0,len(oids)):
        output[tradelist_keys[i]] = {'avg_price':avg_price[oids[i]],'status':status[oids[i]],'timestamp':timestamp[oids[i]]}

    return output

def process_banknifty_tradelist(tradelist,expiry):
    calls = {}
    puts = {}
    for key in tradelist.keys():
        if 'CE' in key:
            calls[name_to_strike(key)] = tradelist[key]
        else:
            puts[name_to_strike(key)] = tradelist[key]
    calls = dict(sorted(calls.items(), key=lambda x: x[1], reverse=True))
    puts = dict(sorted(puts.items(), key=lambda x: x[1], reverse=True))
    orders = []
    for strike in calls.keys():
        ins = alice.get_instrument_for_fno(symbol='BANKNIFTY', expiry_date=expiry, is_fut=False, strike=strike, is_CE=True)
        no_of_lots = abs(calls[strike])
        if calls[strike] > 0:
            trans_type = TransactionType.Buy
        else:
            trans_type = TransactionType.Sell
        order = {'transaction_type': trans_type,
                  'instrument': ins,
                  'quantity': int(ins.lot_size) * no_of_lots, 'order_type': OrderType.Market,
                  'product_type': ProductType.Delivery,
                  # 'price' : None,
                  'is_amo': False
                  }
        orders.append(order)
    for strike in puts.keys():
        ins = alice.get_instrument_for_fno(symbol='BANKNIFTY', expiry_date=expiry, is_fut=False, strike=strike, is_CE=False)
        no_of_lots = abs(puts[strike])
        if puts[strike] > 0:
            trans_type = TransactionType.Buy
        else:
            trans_type = TransactionType.Sell
        order = {'transaction_type': trans_type,
                  'instrument': ins,
                  'quantity': int(ins.lot_size) * no_of_lots, 'order_type': OrderType.Market,
                  'product_type': ProductType.Delivery,
                  # 'price' : None,
                  'is_amo': False
                  }
        orders.append(order)
    results = []
    for order in orders:
        results.append(alice.place_order(order_type=order['order_type'],transaction_type=order['transaction_type'],
                                         instrument=order['instrument'],quantity=order['quantity'],
                                         product_type=order['product_type'],is_amo=order['is_amo']))
        time.sleep(0.1)
    avg_price, status, timestamp = fill_prices(results)
    oids = list(status.keys())
    output = {}
    call_keys = [str(i)+'CE' for i in list(calls.keys())]
    put_keys = [str(i)+'PE' for i in list(puts.keys())]
    tradelist_keys =[]
    for key in call_keys:
        tradelist_keys.append(key)
    for key in put_keys:
        tradelist_keys.append(key)

    for i in range(0,len(oids)):
        output[tradelist_keys[i]] = {'avg_price':avg_price[oids[i]],'status':status[oids[i]],'timestamp':timestamp[oids[i]]}

    return output

def fill_prices(results):
    oh = {}
    avg_price = {}
    status = {}
    timestamp = {}
    for item in results:
        key = item['data']['oms_order_id']
        oh[key] = alice.get_order_history(key)
        data = oh[key]['data'][0]
        avg_price[key] = data['average_price']
        status[key] = data['order_status']
        timestamp[key] = data['exchange_time']
    return avg_price,status,timestamp


