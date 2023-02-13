import calendar
import datetime
import itertools
import logging
import os
import re
import time
import gspread
import pandas as pd
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials
import zd
import threading


class Reporter():
    '''
    Sample Entry in Live Sheet: NIFTY 23 Feb 2023 17200CE - NIFTY 23 Feb 2023 17900CE	1	2023-02-09 13:08:44	0	62275	12


    '''

    def __init__(self):
        self.key_filename = 'scale-361622-ea8fd3b634fe.json'
        self.sheet_name = 'Zerodha Live'
        self.scope = ['https://spreadsheets.google.com/feeds',
                      'https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.key_filename, self.scope)
        self.client = gspread.authorize(self.credentials)
        self.sheet = self.client.open(self.sheet_name).sheet1
        self.buyorder, self.sellorder = None, None
        self.log_dir = 'logs'
        self.log_filename = 'reporter.log'
        # Initialize logger
        logging.basicConfig(level=logging.INFO)

        # Create a custom logger in a file
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(message)s')
        self.file_handler = logging.FileHandler(
            os.path.join(self.log_dir, datetime.datetime.now().strftime("%Y-%m-%d") + ".log"))
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        try:
            self.livesheetdf = self.client.open(self.sheet_name).worksheet('Live').get_all_values()
            self.positionsdf = self.client.open(self.sheet_name).worksheet('Positions').get_all_values()
        except:
            self.logger.error('Error reading live and positions sheets')
            self.livesheetdf = pd.DataFrame()
            self.positionsdf = pd.DataFrame()
        self.api_key = zd.read_api_key()
        self.access_token = zd.login()
        self.kite = zd.kiteconnect.KiteConnect(api_key=self.api_key.get("api_key"), access_token=self.access_token)
        self.expiry_time =  datetime.time(15,30)

    def buy_order_filled(self, order_details):
        # order details is a dict containing instrument, qty, entry timestamp, expiry, fill price
        # this function will store buy order details in memory and exit
        self.buyorder = order_details
        self.logger.info('Buy order filled: ' + str(order_details))

    def sell_order_filled(self, order_details):
        # order details is a dict containing instrument, qty, entry timestamp, expiry, fill price
        # this function will store sell order details in memory and trigger the spread create functon and exit
        self.sellorder = order_details
        self.create_spread()
        self.logger.info('Sell order filled: ' + str(order_details))

    def create_spread(self):
        # create spread from buy and sell order details, send to gsheet, reset buy and sell orders, and exit
        spread = self.buyorder['instrument'] + ' - ' + self.sellorder['instrument']
        qty = self.buyorder['qty'] / 100
        pnl = 0
        # if type is datetime, convert to string

        entry_time = self.buyorder['entry_timestamp']
        if type(entry_time) == type(datetime.datetime.now()):
            entry_time = entry_time.strftime('%Y-%m-%d %H:%M:%S')
        elif type(entry_time) == type('string'):
            entry_time = entry_time
        else:
            print('entry time is not datetime or string')
        max_loss = self.buyorder['fill_price'] * 100 * qty - self.sellorder['fill_price'] * 50 * qty
        spread_number = self.get_spread_number()
        self.logger.info(
            'Updating Sheet for: ' + str(spread) + ' ' + str(qty) + ' ' + str(entry_time) + ' ' + str(pnl) + ' ' + str(
                max_loss) + ' ' + str(spread_number))
        self.update_live_sheet(spread, qty, entry_time, pnl, max_loss, spread_number)
        list_of_additions = [
            [self.buyorder['instrument'], qty * 100, entry_time, float(self.buyorder['fill_price']), 0, int(spread_number)],
            [self.sellorder['instrument'], -qty * 50, entry_time, float(self.sellorder['fill_price']), 0, int(spread_number)]]
        self.update_positions_sheet(list_of_additions)
        self.logger.info('Resetting buy and sell orders')
        self.buyorder, self.sellorder = None, None

    def get_spread_number(self):
        # get the last spread number from gsheet and return it
        # if no spread number exists, return 1
        # Check the length of the dataframe on sheet named "positions"
        try:
            dataframe = self.client.open(self.sheet_name).worksheet('Positions').get_all_values()
            no_rows = len(dataframe) - 1
            if no_rows == 0:
                return 1
            else:
                return int(no_rows / 2 + 1)
        except APIError as e:
            self.logger.error('APIError (get_spread_number): ' + str(e) + '\nRetrying in 60 seconds')
            time.sleep(60)
            return self.get_spread_number()

    def check_duplicates(self, sheet_df, row_elements):
        symbolorspread = row_elements[0]
        entry_time = str(row_elements[2])
        for row in sheet_df:
            if symbolorspread in row and entry_time in row:
                return True
        return False

    def update_live_sheet(self, spread, qty, entry_time, pnl, max_loss, spread_num):
        # update the live sheet with the latest spread details
        # Columns: Instrument	Qty	Entry Timestamp	P/L	Max Loss Spread Number
        # Find out the last row number, and update the next row, and exit
        sheet_name = 'Live'
        row_elements = [spread, int(qty), entry_time, float(pnl), float(max_loss), int(spread_num)]
        if self.check_duplicates(self.livesheetdf, row_elements):
            print('Duplicate row found, not updating sheet')
            return
        try:
            sheet_df = self.client.open(self.sheet_name).worksheet(sheet_name).get_all_values()
            self.livesheetdf = sheet_df
            last_row = len(sheet_df)
            first_empty_row = last_row + 1
            if self.check_duplicates(sheet_df, row_elements):
                print('Duplicate row found, not updating sheet')
                return
            self.client.open(self.sheet_name).worksheet(sheet_name).insert_row(row_elements, first_empty_row)
        except APIError as e:
            # handle gspread.exceptions.APIError: {'code': 429}
            print('APIError (update_live_sheet): ', e)
            self.logger.error('APIError (update_live_sheet): ' + str(e) + '\nRetrying in 60 seconds')
            time.sleep(60)
            self.update_live_sheet(spread, qty, entry_time, pnl, max_loss, spread_num)
        except Exception as e:
            print('Exception (update_live_sheet): ', e)
            self.logger.error('Exception (update_live_sheet): ' + str(e))
            self.update_live_sheet(spread, qty, entry_time, pnl, max_loss, spread_num)

    def update_positions_sheet(self, list_of_additions):
        # update the positions sheet with the latest position details, each element in the list_of_additions is a list
        # Columns: Instrument	Qty	Entry Timestamp	Entry Price	Current Price	Spread Number
        # Find out the last row number, and update the next row, and exit
        sheet_name = 'Positions'
        for item in list_of_additions:
            if self.check_duplicates(self.positionsdf, item):
                list_of_additions.remove(item)
        try:
            sheet_df = self.client.open(self.sheet_name).worksheet(sheet_name).get_all_values()
            self.positionsdf = sheet_df
            last_row = len(sheet_df)
            first_empty_row = last_row + 1
            for item in list_of_additions:
                if self.check_duplicates(sheet_df, item):
                    list_of_additions.remove(item)
            if len(list_of_additions) == 0:
                print('No new rows to add to sheet')
                return
            self.client.open(self.sheet_name).worksheet(sheet_name).insert_rows(list_of_additions, first_empty_row)

        except APIError as e:
            # handle gspread.exceptions.APIError: {'code': 429}
            print('APIError (update_positions_sheet): ', e)
            self.logger.error('APIError (update_positions_sheet): ' + str(e) + '\nRetrying in 60 seconds')
            time.sleep(60)
            self.update_positions_sheet(list_of_additions)
        except Exception as e:
            print('Exception (update_positions_sheet): ', e)
            self.logger.error('Exception (update_positions_sheet): ' + str(e))
            self.update_positions_sheet(list_of_additions)

    def clean_lists(self, input_lists):
        # this function takes in a list of lists, and splits each element list at the first empty element
        # it then returns a list of lists
        # Example input: ll = [['k','l','n','','']] -> [['k','l','n']]
        output_lists = []
        for input_list in input_lists:
            try:
                index = input_list.index('')
                output_lists.append(input_list[:index])
            except:
                output_lists.append(input_list)
        return output_lists

    def update_updated_at(self, sheet_name):
        try:
            worksheet = self.client.open(self.sheet_name).worksheet(sheet_name)
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            worksheet.update('I1', 'Updated at: \n' + current_time)
        except APIError as e:
            # handle gspread.exceptions.APIError: {'code': 429}
            print('APIError (update_updated_at): ', e)
            self.logger.error('APIError (update_updated_at): ' + str(e) + '\nRetrying in 60 seconds')
            time.sleep(60)
            self.update_updated_at(sheet_name)

    def transaction_cost_options(self,buy_price, sell_price, qty):
        qty = abs(qty)
        # Zerodha Transaction Cost Options
        brokerage = 20
        # stt is on sell side (on premium)
        stt = 0.0005
        transaction_charges = 0.00053
        # gst is on brokerage + transaction charges
        gst = 0.18
        sebi_charges_per_10mil = 10
        stamp_charges_per_10mil = 300
        turnover = qty * (buy_price + sell_price)
        brokerage = brokerage * 2
        stt_total = sell_price * qty * stt
        exchange_txn_charge = turnover * transaction_charges
        tot_gst = (brokerage + exchange_txn_charge) * gst
        sebi_charges = turnover / 10000000 * sebi_charges_per_10mil
        stamp_duty = buy_price * qty / 10000000 * stamp_charges_per_10mil
        total_cost = brokerage + stt_total + exchange_txn_charge + tot_gst + sebi_charges + stamp_duty
        return total_cost
    #broker function
    def get_prices_from_broker(self):
        # get instrument list from positions sheet
        # get prices from the broker for unexpired instruments
        # return a dictionary of prices
        self.positionsdf = self.client.open(self.sheet_name).worksheet('Positions').get_all_values()
        instrument_list = [item[0] for item in self.positionsdf[1:]]
        instrument_list = list(set(instrument_list))
        # example item: 'NIFTY 23 FEB 2023 17800CE')
        expiries = [datetime.datetime.strptime(('').join(item.split(' ')[1:4]), '%d%b%Y') for item in instrument_list]
        expiries = [datetime.datetime.combine(item.date(), self.expiry_time) for item in expiries]
        current_time = datetime.datetime.now()
        instrument_list = [instrument_list[i] for i in range(len(instrument_list)) if expiries[i] > current_time]
        if len(instrument_list) > 0:
            prices = self.prices(instrument_list)

        return prices
    # Broker Function
    def prices(self, instrument_list):
        '''
        Returns Current Prices of instruments
        '''
        symbols = [item.split(' ')[0] for item in instrument_list]
        expiries = [datetime.datetime.strptime(('').join(item.split(' ')[1:4]), '%d%b%Y').date() for item in instrument_list]
        strikes = [int(item.split(' ')[4][:-2]) for item in instrument_list]
        option_types = [item.split(' ')[4][-2:] for item in instrument_list]

        list_of_dicts = []
        for i in range(len(instrument_list)):
            list_of_dicts.append({'symbol': symbols[i], 'expiry': expiries[i], 'strike': strikes[i], 'option_type': option_types[i]})

        tokens = {}
        try:
            all_instruments = [x for x in self.kite.instruments() if x['name'] in symbols and x['strike'] in strikes and x['expiry'] in expiries and x['instrument_type'] in option_types]
        except:
            self.access_token = zd.login()
            self.kite = zd.kiteconnect.KiteConnect(api_key=self.api_key.get("api_key"), access_token=self.access_token)
            all_instruments = [x for x in self.kite.instruments() if x['name'] in symbols and x['strike'] in strikes and x['expiry'] in expiries and x['instrument_type'] in option_types]
        for i in range(0,len(list_of_dicts)):
            for item in all_instruments:
                if item['name'] == list_of_dicts[i]['symbol'] and item['strike'] == list_of_dicts[i]['strike'] and item['expiry'] == list_of_dicts[i]['expiry'] and item['instrument_type'] == list_of_dicts[i]['option_type']:
                    tokens[instrument_list[i]] = item['instrument_token']
                    break

        prices = {}
        for key, value in tokens.items():
            quote = self.kite.quote(value)[str(value)]
            # update with mark price if bids and asks are available
            if quote['buy_quantity'] > 0 and quote['sell_quantity'] > 0:
                prices[key] = (quote['depth']['buy'][0]['price'] + quote['depth']['sell'][0]['price'])/2
            else:
                prices[key] = quote['last_price']
        return prices

    def get_closing_prices(self,symbols,dates):
        set_of_symbols = list(set(symbols))
        from_to_dict = {}
        for symbol in set_of_symbols:
            from_to_dict[symbol] = [min([dates[i] for i in range(len(dates)) if symbols[i] == symbol]),
                                    max([dates[i] for i in range(len(dates)) if symbols[i] == symbol])]



        for symbol in set_of_symbols:
            if symbol == 'NIFTY':
                try:
                    df = pd.DataFrame(self.kite.historical_data(instrument_token=[n for n in self.kite.instruments("NSE") if n['tradingsymbol'] == 'NIFTY 50'][0]['instrument_token'],
                                                    from_date=from_to_dict[symbol][0].strftime('%Y-%m-%d'),
                                                    to_date=from_to_dict[symbol][1].strftime('%Y-%m-%d'),
                                                    interval='day'))
                except:
                    self.access_token = zd.login()
                    self.kite = zd.kiteconnect.KiteConnect(api_key=self.api_key.get("api_key"),
                                                           access_token=self.access_token)
                    df = pd.DataFrame(self.kite.historical_data(instrument_token=[n for n in self.kite.instruments("NSE") if n['tradingsymbol'] == 'NIFTY 50'][0]['instrument_token'],
                                                    from_date=from_to_dict[symbol][0].strftime('%Y-%m-%d'),
                                                    to_date=from_to_dict[symbol][1].strftime('%Y-%m-%d'),
                                                    interval='day'))
                df['date'] = [datetime.datetime.strptime(str(item)[:str(item).index(' ')], '%Y-%m-%d').date() for item in df['date']]
                df = df[['date','close']]
                df.columns = ['date','NIFTY']
                df.reset_index(drop=True,inplace=True)

                return df

    def option_expiry_from_underlying_expiry(self,underlying_expiry,opt_type,strike):
        if opt_type == 'CE':
            if strike > underlying_expiry:
                return 0
            else:
                return underlying_expiry - strike
        elif opt_type == 'PE':
            if strike < underlying_expiry:
                return 0
            else:
                return strike - underlying_expiry


    def get_expired_prices(self):
        try:
            self.positionsdf = self.client.open(self.sheet_name).worksheet('Positions').get_all_values()
        except APIError as e:
            self.logger.error('Error in updating expired pnl: {}'.format(e), '\nRetrying in 60 Seconds')
            time.sleep(60)
            self.update_expired_pnl()
        instrument_list = [item[0] for item in self.positionsdf[1:]]
        instrument_list = list(set(instrument_list))
        # example item: 'NIFTY 23 FEB 2023 17800CE')
        expiries = [datetime.datetime.strptime(('').join(item.split(' ')[1:4]), '%d%b%Y') for item in instrument_list]
        expiries = [datetime.datetime.combine(item.date(), self.expiry_time) for item in expiries]
        current_time = datetime.datetime.now()
        expired_instruments = list(set([instrument_list[i] for i in range(len(instrument_list)) if expiries[i] <= current_time]))
        expiries = [datetime.datetime.strptime(('').join(item.split(' ')[1:4]), '%d%b%Y').date() for item in expired_instruments]

        # get closing price of the symbol on expiry dates from broker
        symbols = [item.split(' ')[0] for item in expired_instruments]
        closing_prices = self.get_closing_prices(symbols,expiries)
        strikes = [int(item.split(' ')[4][:-2]) for item in expired_instruments]
        option_types = [item.split(' ')[4][-2:] for item in expired_instruments]

        # get option expiries by underlying expiry
        prices = [self.option_expiry_from_underlying_expiry(closing_prices[closing_prices['date'] == expiries[i]][symbols[i]].values[0],option_types[i],strikes[i]) for i in range(len(expired_instruments))]
        return dict(zip(expired_instruments,prices))

    def update_expired_pnl(self):
        self.logger.info('Updating expired pnl')
        expired_prices = self.get_expired_prices()
        self.update_pnl(expired_prices)


    def update_pnl(self, prices):
        '''
        This function takes in prices (dictionary) as input and updates the Current Price column in the positions sheet
        Sample input prices: {'SBIN': 100, 'TCS': 200}
        It then updates the merged cell I1:J2 to Updated at: <current time>
        It then goes on to the Live sheet and updates the P/L column, to update the P/L, it needs to get P/L from the positions sheet
        For each spread, it gets the P/L of its components, and then adds them up to get the P/L of the spread
        It then updates the P/L column in the Live sheet, updates the merged cell I1:J2 to Updated at: <current time> and exits
        '''
        sheet_name = 'Positions'
        try:
            worksheet = self.client.open(self.sheet_name).worksheet(sheet_name)
        except APIError as e:
            self.logger.error('Error in updating pnl: {}'.format(e), '\nRetrying in 60 Seconds')
            time.sleep(60)
            self.update_pnl(prices)
        dataframe = worksheet.get_all_values()
        # split lists at the first empty element
        dataframe = self.clean_lists(dataframe)
        # convert to dataframe
        dataframe = pd.DataFrame(dataframe[1:], columns=dataframe[0])
        # columns  = ['Instrument', 'Qty', 'Entry Timestamp', 'Entry Price', 'Current Price', 'Spread Number']
        # update the current price column
        for index, row in dataframe.iterrows():
            if row['Instrument'] in prices.keys():
                dataframe.loc[index, 'Current Price'] = prices[row['Instrument']]
        # set datatypes
        dataframe['Qty'] = dataframe['Qty'].astype(int)
        dataframe['Entry Price'] = dataframe['Entry Price'].astype(float)
        dataframe['Current Price'] = dataframe['Current Price'].astype(float)

        # convert to list of lists
        dataframe = dataframe.values.tolist()
        # add the header back
        dataframe.insert(0, ['Instrument', 'Qty', 'Entry Timestamp', 'Entry Price', 'Current Price', 'Spread Number'])
        # update the sheet
        worksheet.update('A1:F' + str(len(dataframe)), dataframe)
        # update the merged cell I1:J2 to Updated at: <current time>
        self.update_updated_at('Positions')
        # get the spread numbers from the positions sheet

    def update_live_sheet_pnl_from_positions(self):
        # columns  = [Instrument	Qty	Entry Timestamp	P/L	Max Loss	Spread Number]
        try:
            positions_dataframe = self.client.open(self.sheet_name).worksheet('Positions').get_all_values()
            positions_dataframe = self.clean_lists(positions_dataframe)
            positions_dataframe = pd.DataFrame(positions_dataframe[1:], columns=positions_dataframe[0])
            # columns  = ['Instrument', 'Qty', 'Entry Timestamp', 'Entry Price', 'Current Price', 'Spread Number']
            # get the spread numbers from the positions sheet
            spread_numbers = positions_dataframe['Spread Number'].unique().tolist()
            # get the spreads from the spreads sheet
            spreads_dataframe = self.client.open(self.sheet_name).worksheet('Live').get_all_values()
            # remove first row (title)
            spreads_dataframe = spreads_dataframe[1:]
            # clean
            spreads_dataframe = self.clean_lists(spreads_dataframe)
            # convert to dataframe
            spreads_dataframe = pd.DataFrame(spreads_dataframe[1:], columns=spreads_dataframe[0])
            # For each spread in live sheet, pnl is the sum of its component pnl (qty * (current price - entry price))
            # get the spread numbers from the positions sheet
            for spread_number in spread_numbers:
                spread_positions = positions_dataframe[positions_dataframe['Spread Number'] == spread_number]
                # calculate the pnl
                pnl = 0
                for index, row in spread_positions.iterrows():
                    pnl += int(row['Qty']) * (float(row['Current Price']) - float(row['Entry Price']))

                # update the pnl in the live sheet
                for index, row in spreads_dataframe.iterrows():
                    if row['Spread Number'] == spread_number:
                        spreads_dataframe.loc[index, 'P/L'] = pnl
                        break
            # set datatypes
            spreads_dataframe['Qty'] = spreads_dataframe['Qty'].astype(int)
            spreads_dataframe['P/L'] = spreads_dataframe['P/L'].astype(float)
            spreads_dataframe['Max Loss'] = spreads_dataframe['Max Loss'].astype(float)

            # convert to list of lists
            spreads_dataframe = spreads_dataframe.values.tolist()
            # add the header back
            spreads_dataframe.insert(0, ['Instrument', 'Qty', 'Entry Timestamp', 'P/L', 'Max Loss', 'Spread Number'])

            # update the sheet
            # update from A2 to F<length of dataframe>

            self.client.open(self.sheet_name).worksheet('Live').update('A2:F' + str(len(spreads_dataframe) + 1),spreads_dataframe)
            # update the merged cell I1:J2 to Updated at: <current time>
            self.update_updated_at('Live')
        except APIError as e:
            self.logger.error('Error in updating pnl: {}'.format(e), '\nRetrying in 60 Seconds')
            time.sleep(60)
            self.update_live_sheet_pnl_from_positions()


    def load_zerodha_tradesheet(self):
        filename = 'tradebook-LC2351-FO (2).xlsx'
        tradesheet = pd.read_excel(filename)
        # column names in 14th row
        tradesheet.columns = tradesheet.iloc[13]
        tradesheet = tradesheet.drop(tradesheet.index[0:14])
        tradesheet = tradesheet.reset_index(drop=True)
        # rename last column as expiry
        tradesheet = tradesheet.rename(columns={tradesheet.columns[-1]: 'Expiry'})
        # filter for segment : FO
        tradesheet = tradesheet[tradesheet['Segment'] == 'FO']
        columns_to_keep = ['Symbol', 'Trade Date', 'Trade Type', 'Quantity', 'Price', 'Order Execution Time', 'Expiry']
        tradesheet = tradesheet[columns_to_keep]
        tradesheet = tradesheet.reset_index(drop=True)
        return tradesheet

    def spreads_from_day_sheet(self, day_df):
        # condition for a couple of trades to qualify as a call ratio spread:
        # 1. both trades are of the same instrument (NIFTY, BANKNIFTY, etc)
        # 2. both trades are of the same expiry
        # 3. both trades are calls
        # 4. buy quantity is double of sell quantity (sell quantity is negative)
        # 5. buy price is higher than sell price
        day_df['Symbol'] = day_df['Symbol'].apply(lambda x: self.disintegrate_tradesheet_symbol(x))
        day_df['Instrument'] = day_df['Symbol'].apply(lambda x: x['instrument'])
        day_df['Option Type'] = day_df['Symbol'].apply(lambda x: x['option_type'])

        # Save all possible pairs of trades as tuples and check them for the conditions
        spreads = list(itertools.combinations(day_df.index, 2))

        # filter for call ratio spreads
        call_ratio_spreads = []
        for spread in spreads:
            # buy spread is positive quantity and sell spread is negative quantity
            filtered_df = day_df.loc[list(spread)].reset_index(drop=True)
            buy_spread = filtered_df[filtered_df['Quantity'] > 0]
            sell_spread = filtered_df[filtered_df['Quantity'] < 0]
            if not len(buy_spread) == 1 or not len(sell_spread) == 1:
                continue
            if buy_spread['Instrument'].values[0] == sell_spread['Instrument'].values[0] and \
                    buy_spread['Expiry'].values[0] == sell_spread['Expiry'].values[0] and \
                    buy_spread['Option Type'].values[0] == 'CE' and sell_spread['Option Type'].values[0] == 'CE' and \
                    buy_spread['Quantity'].values[0] == -2 * sell_spread['Quantity'].values[0] and \
                    buy_spread['Price'].values[0] > sell_spread['Price'].values[0]:
                # create a dictionary with the buy and sell spreads
                call_ratio_spreads.append({'buy_spread': buy_spread, 'sell_spread': sell_spread})
        if len(call_ratio_spreads) == 1:
            return call_ratio_spreads[0]
        elif len(call_ratio_spreads) > 1:
            print('Multiple Potential spreads, please sort manually')
            return 1
        else:
            return 0

    def add_tradesheet_spread(self, spread):
        # this function adds the spread to the positions and live sheets
        # trigger buy fill and sell fill
        # Sample         order_details = {'instrument':random.choice(potential_instruments), 'qty':random.choice(potential_quantities), 'entry_timestamp':random.choice(potential_entry_timestamps), 'expiry':random.choice(potential_expiries), 'fill_price':random.choice(potential_fill_prices)}
        buy_order_details = {
            'instrument': spread['buy_spread']['Instrument'].values[0] + ' ' + spread['buy_spread']['Symbol'].values[0][
                'expiry'].strftime('%d %b %Y').upper() + ' ' + str(
                spread['buy_spread']['Symbol'].values[0]['strike_price']) + spread['buy_spread']['Symbol'].values[0][
                              'option_type'],
            'qty': spread['buy_spread']['Quantity'].values[0],
            'fill_price': spread['buy_spread']['Price'].values[0],
            'entry_timestamp': self.numpy_datetime64_to_datetime(spread['buy_spread']['Order Execution Time'].values[0])
        }
        sell_order_details = {'instrument': spread['sell_spread']['Instrument'].values[0] + ' ' +
                                            spread['sell_spread']['Symbol'].values[0]['expiry'].strftime(
                                                '%d %b %Y').upper() + ' ' + str(
            spread['sell_spread']['Symbol'].values[0]['strike_price']) + spread['sell_spread']['Symbol'].values[0][
                                                'option_type'],
                              'qty': spread['sell_spread']['Quantity'].values[0],
                              'fill_price': spread['sell_spread']['Price'].values[0],
                              'entry_timestamp': self.numpy_datetime64_to_datetime(
                                  spread['sell_spread']['Order Execution Time'].values[0])
                              }
        self.buy_order_filled(buy_order_details)
        self.sell_order_filled(sell_order_details)
        return 1

    def disintegrate_tradesheet_symbol(self, symbol):
        # sample symbol formats: NIFTY2320217250CE : NIFTY 2023, 2 Feb, 17250 CE | NIFTY2320917000CE : NIFTY 2023, 9 Feb, 17000 CE | NIFTY23FEB17800CE : NIFTY 2023, 23 Feb, 17800 CE | NIFTY23D1717800CE : NIFTY 2023, 17 Dec, 17800 CE
        # get the expiry date
        # instrument name is all the characters before the first digit
        instrument_name = re.search(r'[a-zA-Z]+', symbol).group()
        # next two digts are the expiry year
        expiry_year = int(re.search(r'[0-9]{2}', symbol).group())
        # remove the instrument name and expiry year from the symbol
        symbol = symbol.replace(instrument_name + str(expiry_year), '')
        # next digit is the expiry month, it is either the month name (JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC) for monthly expiries (last thursday of the month) or the month number (1, 2, 3, 4, 5, 6, 7, 8, 9, O,N,D) for weekly expiries followed by a date
        # check if the first character is a digit
        if symbol[0].isdigit():
            # if it is a digit, then it is a weekly expiry
            # get the expiry month
            expiry_month = int(symbol[0])
            # convert the month number to month name (format: JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC)
            expiry_month = datetime.date(1900, expiry_month, 1).strftime('%b').upper()
            # get the expiry date
            expiry_date = int(symbol[1:3])
            # remove the expiry month and date from the symbol
            symbol = symbol.replace(symbol[0:3], '')
        elif len(re.search(r'[a-zA-Z]+', symbol).group()) == 1:
            if symbol[0] == 'O':
                expiry_month = 'OCT'
            elif symbol[0] == 'N':
                expiry_month = 'NOV'
            elif symbol[0] == 'D':
                expiry_month = 'DEC'
            # remove the expiry month from the symbol
            symbol = symbol.replace(symbol[0], '')
            # get the expiry date
            expiry_date = int(symbol[0:2])
            # remove the expiry date from the symbol
            symbol = symbol.replace(symbol[0:2], '')
        elif len(re.search(r'[a-zA-Z]+', symbol).group()) == 3:
            expiry_month = re.search(r'[a-zA-Z]+', symbol).group()
            # remove the expiry month from the symbol
            symbol = symbol.replace(expiry_month, '')
            # expiry date is the last thursday of the month
            # all thursday of the month:
            thursdays = [d for d in calendar.Calendar().itermonthdates(2000 + expiry_year,
                                                                       datetime.datetime.strptime(expiry_month,
                                                                                                  '%b').month) if
                         d.weekday() == 3]
            # filter for expiry month
            thursdays = [d for d in thursdays if d.month == datetime.datetime.strptime(expiry_month, '%b').month]
            # get the date of last thursday of the month
            expiry_date = thursdays[-1].day
        # get the strike price
        strike_price = int(re.search(r'[0-9]+', symbol).group())
        # remove the strike price from the symbol
        symbol = symbol.replace(str(strike_price), '')
        # get the option type
        option_type = symbol

        # send out dict
        expiry = datetime.date(2000 + expiry_year, datetime.datetime.strptime(expiry_month, '%b').month, expiry_date)
        details = {'instrument': instrument_name, 'expiry': expiry, 'strike_price': strike_price,
                   'option_type': option_type}
        return details

    def numpy_datetime64_to_datetime(self, numpy_datetime):
        txt = str(numpy_datetime)
        year = int(txt[0:4])
        month = int(txt[5:7])
        day = int(txt[8:10])
        hour = int(txt[11:13])
        minute = int(txt[14:16])
        second = int(txt[17:19])
        return datetime.datetime(year, month, day, hour, minute, second)

    def tradesheet_to_positions(self):
        '''
        This function takes the tradesheet and extracts bull call ratio spreads to send on to the positions sheet
        '''

        tradesheet = self.load_zerodha_tradesheet()
        # get unqiue dates from the col Trade Date
        dates = tradesheet['Trade Date'].unique()
        # if Trade Type is sell, change the sign of the quantity
        tradesheet['Quantity'] = tradesheet.apply(
            lambda row: -row['Quantity'] if row['Trade Type'] == 'sell' else row['Quantity'], axis=1)
        # loop through each date
        for date in dates:
            trades_on_date = tradesheet[tradesheet['Trade Date'] == date]
            trades_on_date = trades_on_date.groupby('Symbol')
            trades_on_date = trades_on_date.agg(
                {'Quantity': 'sum', 'Price': 'mean', 'Order Execution Time': 'last', 'Expiry': 'first'})
            trades_on_date = trades_on_date.reset_index()
            # add Trade Type according to quantity sign
            trades_on_date['Trade Type'] = trades_on_date.apply(lambda row: 'sell' if row['Quantity'] < 0 else 'buy',
                                                                axis=1)
            # add to grouped tradesheet
            if len(trades_on_date):
                oput = self.spreads_from_day_sheet(trades_on_date.copy())
                if type(oput) == type(dict()):
                    if self.add_tradesheet_spread(oput):
                        print('Added spread on {}'.format(date))
                elif type(oput) == int():
                    if oput == 1:
                        print('No spreads found on {}'.format(date))
                    elif oput == 2:
                        print('Not enough data on {}'.format(date))


if __name__ == '__main__':
    reporter = Reporter()
    # reporter.tradesheet_to_positions()
    # reporter.update_expired_pnl()
    reporter.update_pnl(reporter.get_prices_from_broker())
    reporter.update_live_sheet_pnl_from_positions()


    # reporter.tradesheet_to_positions()
