import datetime
import json
import logging
import os
import random
import time
# import exception traceback
import traceback
from line_profiler_pycharm import profile
import kiteconnect
import pandas as pd
from furl import furl
from nsetools import Nse
from prettytable import PrettyTable
from pyotp import TOTP
from selenium import webdriver
from selenium.webdriver import ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import gsheet_reporting
from update_chromedriver import main as update_chromedriver
import numpy as np
from zd import login

config = json.load(open("config.json", "r"))

LOG_DIR = config.get("log_dir")
# METADATA_DIR = config.get("meta_dir")
# TICKS_DIR = config.get("tick_dir")
START_TIME = datetime.time(9, 15, 0)
END_TIME = datetime.time(15, 45, 0)


def read_api_key():
    with open("key.json", "r") as f:
        return json.load(f)


def read_credentials():
    with open("credentials.json", "r") as f:
        return json.load(f)


# Waiting for page to load  element
def get_element(driver, xpath):
    return WebDriverWait(driver, 100).until(
        EC.presence_of_element_located((By.XPATH, xpath)))


# Initialize logger
logging.basicConfig(level=logging.INFO)

# Create a custom logger in a file
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
file_handler = logging.FileHandler(os.path.join(
    LOG_DIR, datetime.datetime.now().strftime("%Y-%m-%d") + ".log"))
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

reporter = gsheet_reporting.Reporter()


# def login():
#     if os.path.isfile("access_token.txt") and os.path.getmtime("access_token.txt") > time.time() - 3600:
#         with open("access_token.txt", "r") as f:
#             access_token = json.loads(f.read())
#         return access_token
#
#     update_chromedriver(".", False)
#     credentials = read_credentials()
#     api_key = read_api_key()
#
#     kite = kiteconnect.KiteConnect(api_key=api_key.get("api_key"))
#     options = ChromeOptions()
#     # start headed browser
#     # options.add_argument("--start-maximized")
#     options.add_argument("--headless")
#     options.add_argument("--log-level=NONE")
#     driver = webdriver.Chrome(options=options)
#     driver.maximize_window()
#     driver.get(kite.login_url())
#
#     xpaths = {
#         "username": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input",
#         "password": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input",
#         "login": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button",
#         "totp": "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[1]/input",
#         "click": "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[2]/button"
#     }
#
#     # username = driver.find_element("xpath", xpaths["username"])
#     username = get_element(driver, xpaths["username"])
#     username.send_keys(credentials.get("username"))
#
#     # password = driver.find_element("xpath", xpaths["password"])
#     password = get_element(driver, xpaths["password"])
#     password.send_keys(credentials.get("password"))
#
#     # login = driver.find_element("xpath", xpaths["login"])
#     login = get_element(driver, xpaths["login"])
#     login.click()
#     time.sleep(0.5)
#
#
#     time.sleep(3)
#
#     # totp = driver.find_element("xpath", xpaths["totp"])
#     totp = get_element(driver, xpaths["totp"])
#     totp_token = TOTP(credentials.get("totp")).now()
#     totp.send_keys(totp_token)
#     # click = driver.find_element("xpath", xpaths["click"])
#     click = get_element(driver, xpaths["click"])
#     click.click()
#     time.sleep(0.5)
#     i = 0
#     while i < 10:
#         try:
#             request_token = furl(
#                 driver.current_url).args["request_token"].strip()
#             break
#         except Exception:
#             time.sleep(0.5)
#             i += 1
#
#     data = kite.generate_session(
#         request_token, api_secret=api_key.get("api_secret"))
#     kite.set_access_token(data["access_token"])
#     with open("access_token.txt", "w") as f:
#         json.dump(data['access_token'], f)
#     driver.close()
#     logger.info("Logged in successfully")
#     return data['access_token']


def get_quote(symbol):
    nse = Nse()
    return nse.get_index_quote(symbol)['lastPrice']


def tabulate_dict(dictionary):
    table = PrettyTable()
    table.field_names = ["Name", "Value"]
    for key, value in dictionary.items():
        table.add_row([key, value])
    return table


def send_order(kite, symbol, quantity, transaction_type, order_type, product, price=None, trigger_price=None,
               validity=None, disclosed_quantity=None, squareoff=None, stoploss=None, trailing_stoploss=None, tag=None):
    try:
        order_id = kite.place_order(tradingsymbol=symbol, quantity=quantity, transaction_type=transaction_type,
                                    order_type=order_type, product=product, price=price, trigger_price=trigger_price,
                                    validity=validity, disclosed_quantity=disclosed_quantity, squareoff=squareoff,
                                    stoploss=stoploss, trailing_stoploss=trailing_stoploss, tag=tag)
        logger.info("Order placed. ID is: {}".format(order_id))
        return order_id
    except Exception as e:
        print(traceback.format_exc())
        logger.error("Order placement failed: {}".format(traceback.format_exc()))
        return None


def retrieve_positions(kite):
    try:
        positions = kite.positions()
        logger.info("Positions retrieved successfully")
        return positions
    except Exception as e:
        print(traceback.format_exc())
        logger.error("Position retrieval failed: {}".format(traceback.format_exc()))
        return None


def get_nifty50_futures_symbols(kite):
    # get the nearest expiring nifty50 future contract from kite
    try:
        instruments = kite.instruments("NFO")
        nifty50_futures = [instrument for instrument in instruments if instrument['name'] == 'NIFTY']
        # check if the instrument_type is 'FUT'
        nifty50_futures = [instrument for instrument in nifty50_futures if instrument['instrument_type'] == 'FUT']
        nifty50_futures.sort(key=lambda x: x['expiry'])
        nearest_expiry = nifty50_futures[0]['expiry']
        # get the nearest expiry nifty50 futures
        nearest_expiry_nifty50_futures = [instrument for instrument in nifty50_futures if
                                          instrument['expiry'] == nearest_expiry]
        # get the symbols
        # if only one element, return, else print error
        if len(nearest_expiry_nifty50_futures) == 1:
            logger.info("Nifty50 futures symbols retrieved successfully")
            return nearest_expiry_nifty50_futures[0]
        else:
            logger.error("Could not retrieve nifty50 futures symbols")
            return None
    except Exception as e:
        # print traceback
        print(traceback.format_exc())
        logger.error("Could not retrieve nifty50 futures symbols: {}".format(traceback.format_exc()))
        return None


def get_nearest_nifty_fut_price(kite):
    try:
        nifty50_fut_instrument = get_nifty50_futures_symbols(kite)
        if nifty50_fut_instrument is not None:
            price = kite.quote("NFO:" + nifty50_fut_instrument['tradingsymbol'])
            # create an average of bid and ask to get the mark price
            best_bid = price['NFO:' + nifty50_fut_instrument['tradingsymbol']]['depth']['buy'][0]['price']
            best_ask = price['NFO:' + nifty50_fut_instrument['tradingsymbol']]['depth']['sell'][0]['price']
            mark_price = (best_bid + best_ask) / 2
            if not mark_price:
                # get last traded price
                mark_price = price['NFO:' + nifty50_fut_instrument['tradingsymbol']]['last_price']

            logger.info("Nearest nifty50 futures price retrieved successfully")
            return mark_price
        else:
            logger.error("Could not retrieve nearest nifty50 futures price")
            return None
    except Exception as e:
        print(traceback.format_exc())
        logger.error("Could not retrieve nifty50 futures symbols: {}".format(traceback.format_exc()))
        return None


def nifty_options_expiring_between(kite, min_days, max_days):
    instruments = kite.instruments("NFO")
    nifty50_options = [instrument for instrument in instruments if
                       instrument['name'] == 'NIFTY' and instrument['instrument_type'] in ['CE', 'PE']]
    nifty50_options.sort(key=lambda x: x['expiry'])
    # filter for options expiring between min_days and max_days
    nifty50_options = [instrument for instrument in nifty50_options if
                       (instrument['expiry'] - datetime.datetime.now().date()).days >= min_days and (
                               instrument['expiry'] - datetime.datetime.now().date()).days <= max_days]
    return nifty50_options


# min_buyprice, max_buyprice, min_sellprice, max_sellprice, exp_min_days, exp_max_days = 900,1100,150,300, 10, 20


def filter_options_by_price(kite, options, min_buyprice, max_buyprice, min_sellprice, max_sellprice):
    # distance is expressed in percentage from current nifty price (nearest nifty50 futures price)
    # filter for options with mark price between min_buyprice and max_buyprice
    # filter for options with mark price between min_sellprice and max_sellprice
    # return a dict containing buy and sell options
    # only consider call options

    # get the nearest nifty50 futures price
    nearest_nifty_fut_price = get_nearest_nifty_fut_price(kite)
    if nearest_nifty_fut_price is not None:
        # add mark price as a key to the options dict
        # get quotes for all options
        options = [option for option in options if option['instrument_type'] == 'CE']
        quotes = kite.quote(['NFO:' + option['tradingsymbol'] for option in options])
        sell_candidates, buy_candidates = [], []
        for option in options:
            # get mark price as average of best bid and best ask
            best_bid = quotes['NFO:' + option['tradingsymbol']]['depth']['buy'][0]['price']
            best_ask = quotes['NFO:' + option['tradingsymbol']]['depth']['sell'][0]['price']
            mark_price = (best_bid + best_ask) / 2
            if not mark_price:
                # get last traded price
                mark_price = quotes['NFO:' + option['tradingsymbol']]['last_price']
            option['mark_price'] = mark_price
            # add best bid and best ask to the option dict
            option['best_bid'] = best_bid
            option['best_ask'] = best_ask
            # calculate bid/ask spread in percentage of mark price
            option['bid_ask_spread'] = (best_ask - best_bid)
            # check if mark price is between min_buyprice and max_buyprice
            if min_buyprice <= mark_price <= max_buyprice:
                buy_candidates.append(option)
            # check if mark price is between min_sellprice and max_sellprice
            if min_sellprice <= mark_price <= max_sellprice:
                sell_candidates.append(option)

        logger.info("Options filtered by price successfully")
        return {'buy': buy_candidates, 'sell': sell_candidates}
    else:
        logger.error("Could not filter options by price")
        return None


def check_available_margin(kite):
    # check if available cash is greater than 10,000
    # check if available margin is greater than 10,000
    # return True if both are greater than 10,000
    # return False if either is less than 10,000
    try:
        # get available cash and margin
        available_cash = kite.margins()['equity']['available']['live_balance']
        if available_cash >= 130000:
            logger.info("Available cash and margin are greater than 130,000")
            return True
        else:
            logger.error("Available cash or margin is less than 130,000")
            return False
    except Exception as e:
        print(traceback.format_exc())
        logger.error("Could not check available cash and margin: {}".format(traceback.format_exc()))
        return False


def execute_orders(kite, multiplier, exp_min_days, exp_max_days, min_buyprice, max_buyprice, min_sellprice,
                   max_sellprice):
    # get buy and sell options, pick one buy and one sell at random
    # send limit buy order for buy options
    # check the status of fill, and keep updating limit price according to the new mark price every 15 seconds
    # send limit sell order for the best offer on sell option when the buy order is filled
    # check the status of fill, and keep updating limit price according to the new offer price every 15 seconds
    # if the sell order is filled, report
    logger.info("Executing orders")

    options_dict = filter_options_by_price(kite, nifty_options_expiring_between(kite, exp_min_days, exp_max_days),
                                           min_buyprice, max_buyprice, min_sellprice, max_sellprice)
    if options_dict is not None:
        buy_options = options_dict['buy']
        sell_options = options_dict['sell']
        # get 3 options with lowest bid/ask spread

        buy_options.sort(key=lambda x: x['bid_ask_spread'])
        sell_options.sort(key=lambda x: x['bid_ask_spread'])
        buy_options = buy_options[:3]

        # pick one buy and one sell option at random
        buy_option = random.choice(buy_options)
        # filter sell options for the selected buy expiry
        sell_options = [option for option in sell_options if option['expiry'] == buy_option['expiry']]
        sell_option = random.choice(sell_options)
        # pull new quotes for the selected buy option
        logger.info("Selected buy option: %s" % buy_option['tradingsymbol'] + ' and sell option: %s' % sell_option[
            'tradingsymbol'])
        buy_quotes = kite.quote(['NFO:' + buy_option['tradingsymbol']])
        # get the best bid and best ask for the selected buy option
        best_bid = buy_quotes['NFO:' + buy_option['tradingsymbol']]['depth']['buy'][0]['price']
        best_ask = buy_quotes['NFO:' + buy_option['tradingsymbol']]['depth']['sell'][0]['price']
        # truncate limit price to one decimal place
        limit_price = (best_bid + best_ask) / 2
        limit_price = int(limit_price * 10) / 10

        buy_order_id = kite.place_order(tradingsymbol=buy_option['tradingsymbol'], exchange=kite.EXCHANGE_NFO,
                                        transaction_type=kite.TRANSACTION_TYPE_BUY,
                                        quantity=buy_option['lot_size'] * 2 * multiplier,
                                        order_type=kite.ORDER_TYPE_LIMIT, product=kite.PRODUCT_NRML,
                                        price=limit_price, variety=kite.VARIETY_REGULAR)
        # check the status of fill, and keep updating limit price according to the new mark price every 15 seconds
        # update log with order ID, limit price, best bid, best ask, nifty price
        instrument_name = buy_option['name'] + ' ' + buy_option['expiry'].strftime("%d %b %Y").upper() + ' ' + str(
            int(buy_option['strike'])) + buy_option['instrument_type'].upper()
        logger.info("Inititated buy order for " + instrument_name + ' at ' + str(limit_price) + ' with order id ' + str(
            buy_order_id))
        logger.info("Best bid: " + str(best_bid) + ", Best ask: " + str(best_ask))
        print(instrument_name)
        filled = 0
        while filled == 0:
            # get the order status
            orders = kite.orders()
            # get the order with the order id
            buy_order = [order for order in orders if order['order_id'] == buy_order_id][0]
            tabulate_dict(buy_order)
            if buy_order['status'] == 'COMPLETE':
                filled = 1
                # get price of fill
                fill_price = buy_order['average_price']
                trade_time = buy_order['exchange_update_timestamp']
                logger.info("Buy order filled successfully at " + str(fill_price))
                # sample         order_details = {'instrument':'SBIN', 'qty':100, 'entry_timestamp':datetime.datetime.now().strftime("%Y-%b-%d %HH:%MM"), 'expiry':datetime.datetime.now().strftime("%Y-%b-%d %HH:%MM"), 'fill_price':100}
                order_details = {'instrument': instrument_name, 'qty': buy_option['lot_size'] * 2 * multiplier,
                                 'entry_timestamp': trade_time, 'expiry': buy_option['expiry'],
                                 'fill_price': fill_price}
                try:
                    reporter.buy_order_filled(order_details)
                except:
                    logger.info("Error in reporting buy order fill")
                    logger.info(order_details)


            else:
                # get the new mark price
                new_mark_price = kite.quote(['NFO:' + buy_option['tradingsymbol']])['NFO:' + buy_order['tradingsymbol']]
                depth = new_mark_price['depth']
                new_mark_price = (depth['buy'][0]['price'] * 0.7 + depth['sell'][0]['price'] * 0.3)
                # new_mark_price = (new_mark_price['depth']['buy'][0]['price'] + new_mark_price['depth']['sell'][0][
                #     'price']) / 2
                new_mark_price = int(new_mark_price * 10) / 10
                # update the limit price
                kite.modify_order(order_id=buy_order_id, variety=kite.VARIETY_REGULAR, price=new_mark_price)
                logger.info("Updated limit price to " + str(new_mark_price) + ' Current bid ask spread: ' + str(
                    buy_option['bid_ask_spread']))
                print("Waiting for the buy order to fill, bid_ask_spread: ", buy_option['bid_ask_spread'],
                      "new mark price: ", new_mark_price)
                time.sleep(15)
        # send limit sell order for the best offer on sell option when the buy order is filled
        new_offer_price = \
            kite.quote(['NFO:' + sell_option['tradingsymbol']])['NFO:' + sell_option['tradingsymbol']]['depth']['sell'][
                0][
                'price']
        sell_order_id = kite.place_order(tradingsymbol=sell_option['tradingsymbol'], exchange=kite.EXCHANGE_NFO,
                                         transaction_type=kite.TRANSACTION_TYPE_SELL,
                                         quantity=sell_option['lot_size'] * multiplier,
                                         order_type=kite.ORDER_TYPE_LIMIT, product=kite.PRODUCT_NRML,
                                         price=new_offer_price, variety=kite.VARIETY_REGULAR)

        # check the status of fill, and keep updating limit price according to the new offer price every 15 seconds
        # update log with order ID, limit price, best bid, best ask, nifty price
        instrument_name = sell_option['name'] + ' ' + sell_option['expiry'].strftime("%d %b %Y").upper() + ' ' + str(
            int(sell_option['strike'])) + sell_option['instrument_type'].upper()
        logger.info(
            "Initiated sell order for " + instrument_name + ' at ' + str(new_offer_price) + ' with order id ' + str(
                sell_order_id))
        logger.info("Best bid: " + str(
            kite.quote(['NFO:' + sell_option['tradingsymbol']])['NFO:' + sell_option['tradingsymbol']]['depth']['buy'][
                0]['price']) + ", Best ask: " + str(
            kite.quote(['NFO:' + sell_option['tradingsymbol']])['NFO:' + sell_option['tradingsymbol']]['depth']['sell'][
                0]['price']))
        print(instrument_name, "sell order placed at ", new_offer_price)
        filled = 0
        while filled == 0:
            # get the order status
            orders = kite.orders()
            # get the order with the order id
            sell_order = [order for order in orders if order['order_id'] == sell_order_id][0]
            tabulate_dict(sell_order)
            if sell_order['status'] == 'COMPLETE':
                filled = 1
                # get price of fill
                fill_price = sell_order['average_price']
                logger.info("Sell order filled successfully at " + str(fill_price))
                # trigger gsheet update
                # sample order_details = {'instrument':'SBIN', 'qty':100, 'entry_timestamp':datetime.datetime.now().strftime("%Y-%b-%d %HH:%MM"), 'expiry':datetime.datetime.now().strftime("%Y-%b-%d %HH:%MM"), 'fill_price':100}
                order_details = {'instrument': instrument_name, 'qty': sell_option['lot_size'] * multiplier,
                                 'entry_timestamp': trade_time, 'expiry': sell_option['expiry'],
                                 'fill_price': fill_price}
                try:
                    reporter.sell_order_filled(order_details)
                except:
                    # send order_details to log file and continue
                    logger.info("Error in updating gsheet with order details")
                    logger.info(order_details)


            else:
                # get the new offer price
                new_offer_price = kite.quote(['NFO:' + sell_option['tradingsymbol']])[
                    'NFO:' + sell_order['tradingsymbol']]['depth']['sell'][0]['price']
                # update the limit price
                kite.modify_order(order_id=sell_order_id, variety=kite.VARIETY_REGULAR, price=new_offer_price)
                logger.info("Updated limit price to " + str(new_offer_price) + ' Current bid ask spread: ' + str(
                    sell_option['bid_ask_spread']))
                print("Waiting for the sell order to fill, bid_ask_spread: ", sell_option['bid_ask_spread'],
                      "new offer price: ", new_offer_price)
                time.sleep(15)
        # if the sell order is filled, report
        logger.info("Trade executed successfully")
        # save order IDs to a file
        with open('order_ids.txt', 'a') as f:
            f.write(str(buy_order_id) + ',' + str(sell_order_id) + '\n')
        return True
    else:
        logger.error("Could not execute orders")
        return False

def ledger_live_fixed_risk(max_loss, avg_max_loss_per_position, days_to_expiry, max_loss_limit=750000, natd=4):
    if max_loss > max_loss_limit:
        return 0
    else:
        spreads = max_loss / avg_max_loss_per_position
        # how many positions can fit in 40% of the capital?
        positions_that_fit = int(max_loss_limit / avg_max_loss_per_position) - spreads
        prob = min(1, positions_that_fit / (natd * days_to_expiry))
        # generate 1 with probability prob
        return np.random.choice([0, 1], p=[1 - prob, prob])

def test_ledger_live_fixed_risk():
    max_loss = 400000
    avg_max_loss_per_position = 50000
    days_to_expiry = 10
    natd = 4
    max_loss_limit = 500000
    print(ledger_live_fixed_risk(max_loss, avg_max_loss_per_position, days_to_expiry, max_loss_limit, natd))




def execute_default():
    api_key = read_api_key()
    access_token = login()
    kite = kiteconnect.KiteConnect(api_key=api_key.get("api_key"), access_token=access_token)
    print(tabulate_dict(kite.profile()))
    # buy_strike_distance_min, buy_strike_distance_max, sell_strike_distance_min, sell_strike_distance_max = 0.65 / 17, 1.1 / 17, 0.1 / 17, 0.25 / 17
    buy_strike_distance_min, buy_strike_distance_max, sell_strike_distance_min, sell_strike_distance_max = 0.6 / 17, 0.9 / 17, 0.1 / 17, 0.2 / 17

    nifty_price = kite.quote(['NSE:NIFTY 50'])['NSE:NIFTY 50']['last_price']
    min_buyprice = int(nifty_price * buy_strike_distance_min)
    max_buyprice = int(nifty_price * buy_strike_distance_max)
    min_sellprice = int(nifty_price * sell_strike_distance_min)
    max_sellprice = int(nifty_price * sell_strike_distance_max)
    exp_min_days, exp_max_days = 10, 30
    multiplier = 1
    if not check_available_margin(kite):
        logger.error("Not enough available margin")
        return False
    execute_orders(kite, multiplier, exp_min_days, exp_max_days, min_buyprice, max_buyprice, min_sellprice,
                   max_sellprice)


def check_if_executed_today():
    try:
        # if today is sunday or saturday, return true
        if datetime.datetime.today().weekday() in [5, 6]:
            return True
        with open('order_ids.txt', 'r') as f:
            order_ids = f.read()
        last_orders = order_ids.split('\n')[-2].split(',')
        buy_order_id = last_orders[0]
        sell_order_id = last_orders[1]
        # match the first 6 digits of the order id with the current date
        today = datetime.datetime.now().strftime("%y%m%d")
        if buy_order_id.startswith(today) and sell_order_id.startswith(today):
            return True
    except:
        print('Error in reading order ids')
        return False


if __name__ == '__main__':
    # check if the script has already been executed today
    reporter = gsheet_reporting.Reporter()
    reporter.update_pnl(reporter.get_prices_from_broker())
    reporter.update_expired_pnl()
    reporter.update_live_sheet_pnl_from_positions()
    max_loss, average_loss, min_days_to_expiry = reporter.get_open_max_loss_and_days_to_expiry()
    print("Max loss: ", max_loss, "Average loss: ", average_loss, "Min days to expiry: ", min_days_to_expiry)
    logger.info("Max loss: " + str(max_loss) + " Average loss: " + str(average_loss) + " Min days to expiry: " + str(
        min_days_to_expiry))
    natd = 4
    seconds_till_15 = int((datetime.datetime.now().replace(hour=15, minute=00, second=0,
                                                           microsecond=0) - datetime.datetime.now()).total_seconds())
    execution_times = []
    if seconds_till_15 > 0:
        for i in range(natd):
            random_time = random.randint(0, int(seconds_till_15))
            execution_time = datetime.datetime.now() + datetime.timedelta(seconds=random_time)
            logger.info("Execution time "+ str(i) + ': ' + str(execution_time))
            # check with risk manager if the trade is allowed
            if ledger_live_fixed_risk(max_loss, average_loss, min_days_to_expiry, natd=natd, max_loss_limit=750000):
                execution_times.append(execution_time)
                logger.info("Execution time " + str(i) + ' allowed')
            else:
                logger.info("Execution time " + str(i) + ' not allowed')

    else:
        logger.error("Not enough time to execute")

    while True:
        if len(execution_times):
            for execution_time in execution_times:
                if datetime.datetime.now().hour == execution_time.hour and datetime.datetime.now().minute in range(
                        execution_time.minute - 1, execution_time.minute + 1):
                    logger.info("Executing orders")
                    try:
                        execute_default()
                        # remove the execution time from the list
                        execution_times.remove(execution_time)
                    except:
                        logger.error("Error in executing orders, Attempting 2 of 3")
                        try:
                            execute_default()
                            execution_times.remove(execution_time)
                        except:
                            logger.error("Error in executing orders, Attempting 3 of 3")
                            try:
                                execute_default()
                                execution_times.remove(execution_time)
                            except:
                                logger.error("Error in executing orders, Aborting")
                    time.sleep(5)
                    reporter.update_pnl(reporter.get_prices_from_broker())
                    reporter.update_live_sheet_pnl_from_positions()
                    time.sleep(60)
        # if the time is divisible by 5, update the pnl
        if datetime.datetime.now().minute % 5 == 0:
            try:
                reporter.update_pnl(reporter.get_prices_from_broker())
                reporter.update_live_sheet_pnl_from_positions()
            except Exception as e:
                logger.error("Error in updating pnl, reinitiating gsheet reporter")
                logger.error(e)
                import gsheet_reporting

                reporter = gsheet_reporting.Reporter()
            time.sleep(50)
        time.sleep(2)
