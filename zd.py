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
        "totp": "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[1]/input",
        "click": "/html/body/div[1]/div/div[2]/div[1]/div[2]/div/div[2]/form/div[2]/button"
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

    time.sleep(3)

    # totp = driver.find_element("xpath", xpaths["totp"])
    totp = get_element(driver, xpaths["totp"])
    totp_token = TOTP(credentials.get("totp")).now()
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


def get_historical(instrument_token,fdate,tdate,interv,oi):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    if filename in os.listdir('get_historical'):
        df = pd.read_csv('get_historical/' + filename)
        df['date'] = df[['date']].apply(pd.to_datetime)
        return df
    if interv == "day" and (tdate-fdate).days > 1500:
        fdates=[fdate]
        newtdate=fdate+day1500
        tdates=[newtdate]

        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day1500)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day70 = datetime.timedelta(days=70)
    day1 = datetime.timedelta(days=1)
    if interv == '5minute':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day50 = datetime.timedelta(days=55)
    if interv == 'minute':
        fdates = [fdate]
        newtdate = fdate + day50
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day50)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
    if interv == 'hour':
        fdates = [fdate]
        newtdate = fdate + day50
        tdates = [newtdate]
        while (tdate > newtdate):
            newfdate = newtdate + day1
            newtdate = min(tdate, newfdate + day50)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs = []
        for i in range(0, len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token, from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv,
                                                         oi=oi)))
        df = pd.concat(dfs, ignore_index=True)
        df = df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/' + filename, index=False)
        return df

def get_historical_force(instrument_token,fdate,tdate,interv,oi):
    day1500=datetime.timedelta(days=1500)
    day1=datetime.timedelta(days=1)
    dateformat = '%Y-%m-%d'
    filename=fdate.strftime(dateformat)+tdate.strftime(dateformat)+'('+str(instrument_token)+')'+interv+'.csv'
    # if filename in os.listdir('get_historical'):
    #     df = pd.read_csv('get_historical/' + filename)
    #     df['date'] = df[['date']].apply(pd.to_datetime)
    #     return df

    if interv == "day" and (tdate-fdate).days > 1500:
        fdates=[fdate]
        newtdate=fdate+day1500
        tdates=[newtdate]

        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day1500)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date']=[item.date() for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day70 = datetime.timedelta(days=70)
    day1 = datetime.timedelta(days=1)
    if interv == 'hour':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            try:
                dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                             to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
            except:
                continue
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    if interv == '5minute':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    day50 = datetime.timedelta(days=55)
    if interv == 'minute':
        fdates = [fdate]
        newtdate = fdate + day50
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day50)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                         to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df
    if interv == 'hour':
        fdates = [fdate]
        newtdate = fdate + day70
        tdates = [newtdate]
        while (tdate>newtdate):
            newfdate=newtdate+day1
            newtdate=min(tdate,newfdate+day70)
            fdates.append(newfdate)
            tdates.append(newtdate)
        dfs=[]
        for i in range(0,len(fdates)):
            try:
                dfs.append(pd.DataFrame(kite.historical_data(instrument_token,from_date=fdates[i].strftime(dateformat),
                                                             to_date=tdates[i].strftime(dateformat), interval=interv, oi=oi)))
            except:
                continue
        df=pd.concat(dfs,ignore_index=True)
        df=df.reset_index(drop=True)
        pd.to_datetime(df['date'])
        df['date'] = [item.replace(tzinfo=None) for item in df['date'].to_list()]
        df.to_csv('get_historical/'+filename,index=False)
        return df