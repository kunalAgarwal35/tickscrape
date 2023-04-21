import time
import pandas as pd
import zd
import os
import numpy as np
import parquet as pq
import pyarrow as pa

class ohlcs:

        def __init__(self):
            api_key = zd.read_api_key()
            access_token = zd.login()
            self.kite = zd.kiteconnect.KiteConnect(api_key=api_key.get("api_key"), access_token=access_token)
            self.instruments = self.kite.instruments()
            self.data_dir = 'D:/OHLCs_pq/'
            self.csv_dir = 'D:/OHLCs_csv/'
            pass


        def get_all_futures_underlyings(self):
            # returns: list of all futures underlyings
            # the list is sorted by name
            # the list contains only unique underlyings
            # the list contains only underlyings that have futures contracts
            all_futures = [instrument for instrument in self.instruments if instrument.get('tradingsymbol').endswith('FUT') and instrument.get('exchange') == 'NFO']
            all_underlyings = [instrument.get('name') for instrument in all_futures]
            all_underlyings = list(set(all_underlyings))
            all_underlyings = sorted(all_underlyings)
            return all_underlyings

        def get_instrument_token_by_underlying(self, underlying):
            # underlying: underlying name
            # returns: instrument token of the underlying
            # the instrument token is the equity instrument token for the underlying trading on nse:
            # filter instruments for nse and underlying
            potential_instruments = [instrument for instrument in self.instruments if instrument.get('exchange') == 'NSE' and instrument.get('tradingsymbol') == underlying]

            # if not found in nse, check in index segment
            if len(potential_instruments) == 0:
                # if underlying is 'NIFTY', try 'NIFTY 50' and 'NIFTY50'
                if underlying == 'NIFTY':
                    potential_instruments = [instrument for instrument in self.instruments if instrument.get('exchange') == 'NSE' and instrument.get('tradingsymbol') in ['NIFTY 50', 'NIFTY50']]

                # if underlying is 'BANKNIFTY', try 'NIFTY BANK' and 'NIFTYBANK'
                if underlying == 'BANKNIFTY':
                    potential_instruments = [instrument for instrument in self.instruments if instrument.get('exchange') == 'NSE' and instrument.get('tradingsymbol') in ['NIFTY BANK', 'NIFTYBANK']]

            if len(potential_instruments) == 1:
                return potential_instruments[0].get('instrument_token')
            else:
                print('Error: could not find instrument token for underlying: {}'.format(underlying))
                return None

        def test_get_instrument_token_by_underlying(self):
            # test get_instrument_token_by_underlying
            # get all futures underlyings
            all_underlyings = self.get_all_futures_underlyings()
            # for each underlying, get the instrument token
            for underlying in all_underlyings:
                print(underlying)
                print(self.get_instrument_token_by_underlying(underlying))

        def get_ohlc_by_instrument_token(self, instrument_token, start_date, end_date):
            # instrument_token: instrument token of the underlying
            # start_date: start date of the data
            # end_date: end date of the data
            # returns: ohlc data for the underlying
            # the data is sorted by date
            # the data is in the form of a pandas dataframe

            # get ohlc data
            try:
                ohlc = self.kite.historical_data(instrument_token, start_date, end_date, 'day') # day, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute
            # check for the following exception kiteconnect.exceptions.InputException: invalid to date
            except zd.kiteconnect.exceptions.InputException as e:
                return None
            # check if data is empty
            if len(ohlc) == 0:
                print('Error: no data for instrument token: {}'.format(instrument_token), 'start date: {}'.format(start_date), 'end date: {}'.format(end_date))
                return None
            # convert to pandas dataframe
            ohlc = pd.DataFrame(ohlc)
            # sort by date
            ohlc = ohlc.sort_values(by='date')
            # convert date to datetime
            ohlc['date'] = pd.to_datetime(ohlc['date'], format='%Y-%m-%d')
            # check max and min date, and do another query if data is missing
            # print('min date: {}'.format(ohlc['date'].min()))
            # print('max date: {}'.format(ohlc['date'].max()))
            #localise timezone of start_date and end_date (GMT+5:30)

            if ohlc['date'].min() > start_date.tz_localize('Asia/Kolkata'):
                print('Error: data is missing between {} and {}'.format(start_date, ohlc['date'].min()))
                missing_data = self.get_ohlc_by_instrument_token(instrument_token, start_date.tz_localize(None), ohlc['date'].min() - pd.Timedelta(days=1))
                if missing_data is not None:
                    ohlc = pd.concat([missing_data, ohlc])
            if ohlc['date'].max() < end_date.tz_localize('Asia/Kolkata'):
                print('Error: data is missing between {} and {}'.format(ohlc['date'].max(), end_date))
                missing_data = self.get_ohlc_by_instrument_token(instrument_token, ohlc['date'].max().tz_localize(None) + pd.Timedelta(days=1), end_date)
                if missing_data is not None:
                    ohlc = pd.concat([ohlc, missing_data])

            # drop duplicates
            ohlc = ohlc.drop_duplicates(subset='date', keep='last')
            # reset index
            ohlc = ohlc.reset_index(drop=True)

            return ohlc

        def save_ohlc_to_data_dir(self, ohlc, name):
            # write ohlc as parquet in data_dir and as csv in csv_dir
            if ohlc is None:
                print('Error: no data to save for {}'.format(name))
                return
            if not os.path.exists(self.data_dir):
                os.makedirs(self.data_dir)
            if not os.path.exists(self.csv_dir):
                os.makedirs(self.csv_dir)
            ohlc.to_parquet(os.path.join(self.data_dir, '{}.parquet'.format(name)), index=False)
            ohlc.to_csv(os.path.join(self.csv_dir, '{}.csv'.format(name)), index=False)

        def get_5min_ohlc_by_instrument_token(self, instrument_token, start_date, end_date):
            # instrument_token: instrument token of the underlying
            # start_date: start date of the data
            # end_date: end date of the data
            # returns: ohlc data for the underlying
            # the data is sorted by date
            # the data is in the form of a pandas dataframe

            # get ohlc data
            # break timeframes into 99 day chunks
            query_start_dates = [start_date + pd.Timedelta(days=99 * i) for i in range(int((end_date - start_date).days / 99) + 1)]
            query_end_dates = [start_date + pd.Timedelta(days=99 * (i + 1)) for i in range(int((end_date - start_date).days / 99) + 1) if start_date + pd.Timedelta(days=99 * (i + 1)) < end_date]
            query_end_dates.append(end_date)

            ohlc = pd.DataFrame()
            for query_start_date, query_end_date in zip(query_start_dates, query_end_dates):
                try:
                    output = self.kite.historical_data(instrument_token, query_start_date, query_end_date, '5minute') # day, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute
                    # check the length of the output
                    if len(output) == 0:
                        print('Error: no data for instrument token: {}'.format(instrument_token), 'start date: {}'.format(query_start_date), 'end date: {}'.format(query_end_date))
                        print('Retrying...')
                        output = self.kite.historical_data(instrument_token, query_start_date, query_end_date, '5minute') # day, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute
                        if len(output) == 0:
                            print('Still no data. Skipping...')
                            continue
                    if len(output):
                        output = pd.DataFrame(output)
                        output = output.sort_values(by='date')
                        output['date'] = pd.to_datetime(output['date'], format='%Y-%m-%d %H:%M:%S')
                        ohlc = pd.concat([ohlc, output])
                    else:
                        continue
                except Exception as e:
                    print('Error: {}'.format(e))
            if not len(ohlc):
                return None
            # reset index
            ohlc = ohlc.reset_index(drop=True)
            # check for missing data
            if len(ohlc) > 0:
                # check max and min date, and do another query if data is missing
                # print('min date: {}'.format(ohlc['date'].min()))
                # print('max date: {}'.format(ohlc['date'].max()))
                #localise timezone of start_date and end_date (GMT+5:30)
                # ideal start timestamp time = 9:15 AM, ideal end timestamp time = 3:30 PM
                ideal_start_timestamp = pd.Timestamp(year=start_date.year, month=start_date.month, day=start_date.day, hour=9, minute=15, tz='Asia/Kolkata')
                ideal_end_timestamp = pd.Timestamp(year=end_date.year, month=end_date.month, day=end_date.day, hour=15, minute=30, tz='Asia/Kolkata')
                if ohlc['date'].min() > ideal_start_timestamp:
                    print('Error: data is missing between {} and {}'.format(start_date, ohlc['date'].min()))
                    missing_data = self.get_5min_ohlc_by_instrument_token(instrument_token, start_date.tz_localize(None), ohlc['date'].min().tz_localize(None) - pd.Timedelta(days=1))
                    if missing_data is not None:
                        ohlc = pd.concat([missing_data, ohlc])
                if ohlc['date'].max() < ideal_end_timestamp:
                    print('Error: data is missing between {} and {}'.format(ohlc['date'].max(), end_date))
                    missing_data = self.get_5min_ohlc_by_instrument_token(instrument_token, ohlc['date'].max().tz_localize(None) + pd.Timedelta(days=1), end_date.tz_localize(None))
                    if missing_data is not None:
                        ohlc = pd.concat([ohlc, missing_data])
            # drop duplicates
            ohlc = ohlc.drop_duplicates(subset=['date'])
            # sort by date
            ohlc = ohlc.sort_values(by='date')
            # reset index
            ohlc = ohlc.reset_index(drop=True)

            return ohlc













if __name__ == '__main__':
    o = ohlcs()
    all_underlyings = o.get_all_futures_underlyings()
    for underlying in all_underlyings:
        # check if underlying is already downloaded
        if os.path.exists(os.path.join(o.data_dir, '{}.parquet'.format(underlying))):
            print('Already downloaded {}'.format(underlying))
            continue
        print(underlying)
        instrument_token = o.get_instrument_token_by_underlying(underlying)
        start_date = pd.to_datetime('2019-01-01', format='%Y-%m-%d')
        end_date = pd.to_datetime('2023-03-24', format='%Y-%m-%d')
        ohlc = o.get_5min_ohlc_by_instrument_token(instrument_token, start_date, end_date)
        o.save_ohlc_to_data_dir(ohlc, underlying)
        print(ohlc)
        time.sleep(1) # sleep for 1 second to avoid 429 Too Many Requests error
        # break



