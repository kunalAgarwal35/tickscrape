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

from update_chromedriver import main as update_chromedriver

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


def login():
    if os.path.isfile("access_token.txt") and os.path.getmtime("access_token.txt") > time.time() - 3600:
        with open("access_token.txt", "r") as f:
            access_token = json.loads(f.read())
        return access_token

    update_chromedriver(".", False)
    credentials = read_credentials()
    api_key = read_api_key()

    kite = kiteconnect.KiteConnect(api_key=api_key.get("api_key"))
    options = ChromeOptions()
    # start headed browser
    # options.add_argument("--start-maximized")
    options.add_argument("--headless")
    options.add_argument("--log-level=NONE")
    driver = webdriver.Chrome(options=options)
    driver.maximize_window()
    driver.get(kite.login_url())
    xpaths = {
        "username": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[1]/input",
        "password": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input",
        "login": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button",
        "totp": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[2]/input",
        "click": "/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button"
    }

    # username = driver.find_element("xpath", xpaths["username"])
    username = get_element(driver, xpaths["username"])
    username.send_keys(credentials.get("username"))

    # password = driver.find_element("xpath", xpaths["password"])
    password = get_element(driver, xpaths["password"])
    password.send_keys(credentials.get("password"))

    # login = driver.find_element("xpath", xpaths["login"])
    login = get_element(driver, xpaths["login"])
    login.click()
    time.sleep(0.5)

    totp_token = TOTP(credentials.get("totp")).now()
    time.sleep(3)
    new_totp_token = TOTP(credentials.get("totp")).now()
    if totp_token == new_totp_token:
        totp_token = new_totp_token

    # totp = driver.find_element("xpath", xpaths["totp"])
    totp = get_element(driver, xpaths["totp"])
    totp.send_keys(totp_token)
    # click = driver.find_element("xpath", xpaths["click"])
    click = get_element(driver, xpaths["click"])
    click.click()
    time.sleep(0.5)
    i = 0
    while i < 10:
        try:
            request_token = furl(
                driver.current_url).args["request_token"].strip()
            break
        except Exception:
            time.sleep(0.5)
            i += 1

    data = kite.generate_session(
        request_token, api_secret=api_key.get("api_secret"))
    kite.set_access_token(data["access_token"])
    with open("access_token.txt", "w") as f:
        json.dump(data['access_token'], f)
    driver.close()
    logger.info("Logged in successfully")
    return data['access_token']


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


def execute_orders(kite, multiplier, exp_min_days, exp_max_days, min_buyprice, max_buyprice, min_sellprice,
                   max_sellprice):
    # get buy and sell options, pick one buy and one sell at random
    # send limit buy order for buy options
    # check the status of fill, and keep updating limit price according to the new mark price every 15 seconds
    # send limit sell order for the best offer on sell option when the buy order is filled
    # check the status of fill, and keep updating limit price according to the new offer price every 15 seconds
    # if the sell order is filled, report

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
        filled = 0
        while filled == 0:
            # get the order status
            orders = kite.orders()
            # get the order with the order id
            buy_order = [order for order in orders if order['order_id'] == buy_order_id][0]
            tabulate_dict(buy_order)
            if buy_order['status'] == 'COMPLETE':
                filled = 1
                logger.info("Buy order filled successfully")
            else:
                # get the new mark price
                new_mark_price = kite.quote(['NFO:' + buy_option['tradingsymbol']])['NFO:' + buy_order['tradingsymbol']]
                depth = new_mark_price['depth']
                new_mark_price = (depth['buy'][0]['price']*0.7 + depth['sell'][0]['price']*0.3)
                # new_mark_price = (new_mark_price['depth']['buy'][0]['price'] + new_mark_price['depth']['sell'][0][
                #     'price']) / 2
                new_mark_price = int(new_mark_price * 10) / 10
                # update the limit price
                kite.modify_order(order_id=buy_order_id, variety=kite.VARIETY_REGULAR, price=new_mark_price)
                logger.info("Buy order updated successfully")
                # wait for 15 seconds
                print("Waiting for the buy order to fill, bid_ask_spread: ", buy_option['bid_ask_spread'], "new mark price: ", new_mark_price)
                time.sleep(15)
        # send limit sell order for the best offer on sell option when the buy order is filled
        new_offer_price = \
        kite.quote(['NFO:' + sell_option['tradingsymbol']])['NFO:' + sell_option['tradingsymbol']]['depth']['sell'][0][
            'price']
        sell_order_id = kite.place_order(tradingsymbol=sell_option['tradingsymbol'], exchange=kite.EXCHANGE_NFO,
                                         transaction_type=kite.TRANSACTION_TYPE_SELL,
                                         quantity=sell_option['lot_size'] * multiplier,
                                         order_type=kite.ORDER_TYPE_LIMIT, product=kite.PRODUCT_NRML,
                                         price=new_offer_price, variety=kite.VARIETY_REGULAR)

        # check the status of fill, and keep updating limit price according to the new offer price every 15 seconds
        filled = 0
        while filled == 0:
            # get the order status
            orders = kite.orders()
            # get the order with the order id
            sell_order = [order for order in orders if order['order_id'] == sell_order_id][0]
            tabulate_dict(sell_order)
            if sell_order['status'] == 'COMPLETE':
                filled = 1
                logger.info("Sell order filled successfully")
            else:
                # get the new offer price
                new_offer_price = kite.quote(['NFO:' + sell_option['tradingsymbol']])[
                    'NFO:' + sell_order['tradingsymbol']]['depth']['sell'][0]['price']
                # update the limit price
                kite.modify_order(order_id=sell_order_id, variety=kite.VARIETY_REGULAR, price=new_offer_price)
                logger.info("Sell order updated successfully")
                # wait for 15 seconds
                print("Waiting for the sell order to fill, bid_ask_spread: ", sell_option['bid_ask_spread'], "new offer price: ", new_offer_price)
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


def main():
    # if not os.path.exists(METADATA_DIR):
    #     os.mkdir(METADATA_DIR)
    # if not os.path.exists(TICKS_DIR):
    #     os.mkdir(TICKS_DIR)
    # indices_of_interest = [
    #     'NIFTY 50', 'NIFTY BANK', 'INDIA VIX'
    # ]
    #
    # symbols_of_interest = [
    #     'NIFTY', 'BANKNIFTY'
    # ]

    api_key = read_api_key()
    access_token = login()
    kite = kiteconnect.KiteConnect(api_key=api_key.get("api_key"), access_token=access_token)
    print(tabulate_dict(kite.profile()))
    # print positions
    positions = retrieve_positions(kite)['net']
    # position vs pnl df
    position_vs_pnl_df = pd.DataFrame()
    for position in positions:
        position_vs_pnl_df = position_vs_pnl_df.append(position, ignore_index=True)
    print(position_vs_pnl_df)
    # keep only 'symbol' and 'pnl' columns
    position_vs_pnl_df = position_vs_pnl_df[['tradingsymbol', 'pnl']]
    # net pnl
    net_pnl = position_vs_pnl_df['pnl'].sum()

    # kws = KiteTicker(api_key=api_key.get("api_key"), access_token=access_token)

    # Read the list of stocks to watch
    # with open('watchlist.txt', 'r') as f:
    #     watchlist = f.read().splitlines()
    #
    # _interim = pd.DataFrame(kite.instruments("NSE"))
    # index_instruments = _interim[_interim["name"].isin(indices_of_interest)]
    # equity_instruments = _interim[_interim['segment'] == 'NSE']
    #
    # _interim = pd.DataFrame(kite.instruments("NFO"))
    # index_fno_instruments = _interim[_interim["name"].isin(
    #     symbols_of_interest)]
    # equity_fno_instruments = _interim[~_interim['name'].isin(
    #     symbols_of_interest)]
    #
    # # Get equity instruments for the watchlist
    # equity_instruments = equity_instruments[equity_instruments['tradingsymbol'].isin(
    #     watchlist)]
    #
    # # Get last day's trading price
    # ld_nifty = get_quote("NIFTY 50")
    # ld_nifty_bank = get_quote("NIFTY BANK")
    #
    # # Get tradable strikes
    # nifty_strike_range = (ld_nifty - 0.1 * ld_nifty, ld_nifty + 0.1 * ld_nifty)
    # nifty_bank_strike_range = (
    #     ld_nifty_bank - 0.1 * ld_nifty_bank, ld_nifty_bank + 0.1 * ld_nifty_bank)
    #
    # # Get the list of tradable strikes
    # nifty_strikes = index_fno_instruments[(index_fno_instruments['tradingsymbol'].str.contains('NIFTY')) & (
    #     index_fno_instruments['strike'] >= nifty_strike_range[0]) & (index_fno_instruments['strike'] <= nifty_strike_range[1])]
    #
    # nifty_bank_strikes = index_fno_instruments[(index_fno_instruments['tradingsymbol'].str.contains('BANKNIFTY')) & (
    #     index_fno_instruments['strike'] >= nifty_bank_strike_range[0]) & (index_fno_instruments['strike'] <= nifty_bank_strike_range[1])]
    #
    # # Concatenate all dataframes
    # # TODO: Add FNO Equity instruments, Nifty Strikes and Nifty Bank strikes
    # instruments = pd.concat([index_instruments, equity_instruments], axis=0)
    #
    # # Save the instruments to a file
    # instruments.to_csv(os.path.join(METADATA_DIR, 'instruments-{}.csv'.format(
    #     datetime.datetime.now().strftime("%Y-%m-%d")
    # )), index=False)
    #
    # print("Total instruments: {}".format(instruments.shape[0]))
    # print("Save Directory: {}".format(TICKS_DIR))
    # print("Metadata Directory: {}".format(METADATA_DIR))
    #
    # logger.info("Total instruments: {}".format(instruments.shape[0]))
    # logger.info("Save Directory: {}".format(TICKS_DIR))
    # logger.info("Metadata Directory: {}".format(METADATA_DIR))
    #
    # # Tokens to subscribe
    # tokens = instruments['instrument_token'].unique().tolist()
    #
    # instruments = instruments.set_index('instrument_token')
    #
    # def dump_ticks(ticks, save_dir):
    #
    #     dt = datetime.datetime.now()
    #     dt_time = dt.time()
    #
    #     # If current time is between START_TIME and END_TIME, save the ticks
    #     if not (dt_time >= START_TIME and dt_time <= END_TIME):
    #         print("Not saving ticks. Time = {}".format(dt))
    #         return
    #
    #     fname = dt.strftime("%Y-%m-%d-%H-%M-%S-%f.pickle")
    #     ticks = pd.DataFrame(ticks)
    #     ticks = ticks.set_index('instrument_token')
    #     ticks = ticks.join(instruments, how='left',
    #                        lsuffix='_ticks', rsuffix='_instruments')
    #     with open(os.path.join(save_dir, fname), "wb") as f:
    #         pickle.dump(ticks.to_dict(orient='records'), f)
    #     # print("Received: {}. Size = {} rows, {} instruments".format(fname, ticks.shape[0], ticks['tradingsymbol'].nunique()))
    #     logger.info("Received: {}. Size = {} rows, {} instruments".format(
    #         fname, ticks.shape[0], ticks['tradingsymbol'].nunique()))
    #
    # def on_ticks(ws, ticks):
    #     # Callback to receive ticks.
    #     thread.start_new_thread(dump_ticks, (ticks, TICKS_DIR))
    #
    # def on_connect(ws, response):
    #     # Callback on successful connect.
    #     # Subscribe to a list of instrument_tokens
    #     print("Subscribing to tokens")
    #     kws.subscribe(tokens)
    #     kws.set_mode(kws.MODE_FULL, tokens)
    #     print("Connected to Kite")
    #     logger.info("Connected to Kite")
    #
    # print("Connecting to Kite")
    # kws.on_ticks = on_ticks
    # kws.on_connect = on_connect
    # kws.connect()
    # print("Connection ended")

if __name__ == "__main__":
    api_key = read_api_key()
    access_token = login()
    kite = kiteconnect.KiteConnect(api_key=api_key.get("api_key"), access_token=access_token)
    print(tabulate_dict(kite.profile()))
    min_buyprice, max_buyprice, min_sellprice, max_sellprice, exp_min_days, exp_max_days, multiplier = 750, 1000, 100, 300, 10, 20, 1
    execute_orders(kite, multiplier, exp_min_days, exp_max_days, min_buyprice, max_buyprice, min_sellprice, max_sellprice)
