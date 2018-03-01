
import boto3 as bt3
import pandas as pd
import numpy as np
import datetime as dt
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import shared.directory_names as dn
import ta.trade_fill_loader as tfl
import math as m
import os

aws_access_key_id='AKIAJ3Z5XUAOJ5IS25BA'
aws_secret_access_key='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'

tickerhead_dict_from_db_2qg = {'LC': 'LE','LN': 'HE', 'FC': 'GF',
                               'C': 'ZC', 'S': 'ZS', 'SM': 'ZM', 'BO': 'ZL', 'W': 'ZW', 'KW': 'KE',
                               'ED': 'GE', 'EC': '6E', 'JY': '6J', 'AD': '6A', 'CD': '6C', 'BP': '6B',
                               'TY': 'ZN', 'US': 'ZB', 'FV': 'ZF', 'TU': 'ZT'}

price_multiplier = {'ED': 100,
                    'JY': 1e5}

def get_tick_data(**kwargs):

    ticker = kwargs['ticker']
    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = tickerhead_dict_from_db_2qg.get(contract_specs_output['ticker_head'],contract_specs_output['ticker_head'])

    output_file = 'D:/Research/test_file.csv.gz'

    try:
        os.remove(output_file)
    except OSError:
        pass

    if 'boto_client' in kwargs.keys():
        boto_client = kwargs['boto_client']
    else:
        boto_client = bt3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

    utc_doubledate = kwargs['utc_doubledate']
    utc_year = m.floor(utc_doubledate/10000)

    key_aux = ticker_head + '/' + ticker_head + contract_specs_output['ticker_month_str'] + str(contract_specs_output['ticker_year']%10) + '.csv.gz'

    if utc_year in [2017, 2018]:
        bucket = 'us-futures-taq-' + str(utc_year)
        key = str(utc_doubledate) + '/' + key_aux
    else:
        bucket = 'us-futures-taq'
        key = str(utc_year) + '/' + str(utc_doubledate) + '/' + key_aux

    try:
        boto_client.download_file(bucket, key, output_file,{'RequestPayer': 'requester'})
        data_out = pd.read_table(output_file, compression='gzip', sep=',')
    except Exception as e:
        print(ticker + ', ' + str(utc_doubledate))
        print(e)
        data_out = pd.DataFrame()

    return data_out

def get_book_snapshot(**kwargs):

    ticker = kwargs['ticker']
    utc_doubledate = kwargs['utc_doubledate']

    if 'freq_str' in kwargs.keys():
        freq_str = kwargs['freq_str']
    else:
        freq_str = '5T'

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']

    output_dir = dn.get_dated_directory_extension(ext='book_snapshot_data', folder_date=utc_doubledate)
    file_name = output_dir + '/' + ticker + '_' + freq_str + '.pkl'

    if os.path.isfile(file_name):
        return pd.read_pickle(file_name)

    data_out = get_tick_data(**kwargs)

    if len(data_out.index)==0:
        return pd.DataFrame(columns=['mid_p','buy_volume','sell_volume','volume'])

    data_out['hour'] = data_out['LocalTime'] / 1e7
    data_out['hour'] = data_out['hour'].apply(np.floor)

    data_out['minute'] = data_out['LocalTime'] / 1e5
    data_out['minute'] = data_out['minute'].apply(np.floor)
    data_out['minute'] = data_out['minute'] - 1e2 * data_out['hour']

    data_out['second'] = data_out['LocalTime'] / 1e3
    data_out['second'] = data_out['second'].apply(np.floor)
    data_out['second'] = data_out['second'] - 1e4 * data_out['hour']-1e2*data_out['minute']

    data_out['year'] = data_out['LocalDate'] / 1e4
    data_out['year'] = data_out['year'].apply(np.floor)

    data_out['month'] = data_out['LocalDate'] / 1e2
    data_out['month'] = data_out['month'].apply(np.floor)
    data_out['month'] = data_out['month'] - 1e2 * data_out['year']

    data_out['day'] = data_out['LocalDate'] - 1e4 * data_out['year'] - 1e2 * data_out['month']

    data_out['date_time'] = [dt.datetime(int(data_out['year'].iloc[x]), int(data_out['month'].iloc[x]), int(data_out['day'].iloc[x]),
                    int(data_out['hour'].iloc[x]), int(data_out['minute'].iloc[x]), int(data_out['second'].iloc[x])) for x in range(len(data_out.index))]

    merged_index = pd.date_range(start=data_out['date_time'].iloc[0].replace(second=0), end=data_out['date_time'].iloc[-1].replace(second=0), freq='S')
    data_out.set_index('date_time', inplace=True, drop=True)

    bid_data = data_out[data_out['Type'] == 'QUOTE BID']
    bid_data = bid_data.groupby(bid_data.index).last()
    bid_data = bid_data.reindex(merged_index, method='pad')

    ask_data = data_out[data_out['Type'] == 'QUOTE SELL']
    ask_data = ask_data.groupby(ask_data.index).last()
    ask_data = ask_data.reindex(merged_index, method='pad')

    buy_trade_data = data_out[data_out['Type'] == 'TRADE AGRESSOR ON BUY']
    buy_trade_data['CumQuantity'] = buy_trade_data['Quantity'].cumsum()
    buy_trade_data = buy_trade_data.groupby(buy_trade_data.index).last()
    buy_trade_data = buy_trade_data.reindex(merged_index, method='pad')
    buy_trade_data['Quantity'] = buy_trade_data['CumQuantity'].diff()

    sell_trade_data = data_out[data_out['Type'] == 'TRADE AGRESSOR ON SELL']
    sell_trade_data['CumQuantity'] = sell_trade_data['Quantity'].cumsum()
    sell_trade_data = sell_trade_data.groupby(sell_trade_data.index).last()
    sell_trade_data = sell_trade_data.reindex(merged_index, method='pad')
    sell_trade_data['Quantity'] = sell_trade_data['CumQuantity'].diff()

    trade_data = data_out[data_out['Type'] == 'TRADE']
    trade_data['CumQuantity'] = trade_data['Quantity'].cumsum()
    trade_data = trade_data.groupby(trade_data.index).last()
    trade_data = trade_data.reindex(merged_index, method='pad')
    trade_data['Quantity'] = trade_data['CumQuantity'].diff()

    book_snapshot = pd.DataFrame(index=merged_index)

    book_snapshot['bid_p'] = bid_data['Price']
    book_snapshot['bid_p'] = book_snapshot['bid_p']*price_multiplier.get(ticker_head,1)
    book_snapshot['bid_q'] = bid_data['Quantity']

    book_snapshot['ask_p'] = ask_data['Price']
    book_snapshot['ask_p'] = book_snapshot['ask_p'] * price_multiplier.get(ticker_head, 1)
    book_snapshot['ask_q'] = ask_data['Quantity']

    book_snapshot['buy_volume'] = buy_trade_data['Quantity']
    book_snapshot['sell_volume'] = sell_trade_data['Quantity']
    book_snapshot['volume'] = trade_data['Quantity']

    book_snapshot['mid_p'] = (book_snapshot['bid_p'] * book_snapshot['bid_q'] + book_snapshot['ask_p'] * book_snapshot['ask_q']) / (
            book_snapshot['bid_q'] + book_snapshot['ask_q'])
    bar_data = book_snapshot['mid_p'].resample('5T').ohlc()
    bar_data['buy_volume'] = book_snapshot['buy_volume'].resample('5T').sum()
    bar_data['sell_volume'] = book_snapshot['sell_volume'].resample('5T').sum()
    bar_data['volume'] = book_snapshot['volume'].resample('5T').sum()

    bar_data['hour_minute'] = 100 * bar_data.index.hour + bar_data.index.minute

    bar_data.to_pickle(file_name)

    return bar_data

def get_continuous_bar_data(**kwargs):

    date_to = kwargs['date_to']
    num_days_back = kwargs['num_days_back']
    ticker = kwargs['ticker']

    if 'boto_client' in kwargs.keys():
        boto_client = kwargs['boto_client']
    else:
        boto_client = bt3.client('s3',aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to, shift_in_days=x) for x in reversed(range(1, num_days_back))]
    date_list.append(date_to)

    bar_data_list = [get_book_snapshot(ticker=ticker,utc_doubledate=x,boto_client=boto_client) for x in date_list]
    bar_data = pd.concat(bar_data_list)


    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_class = contract_specs_output['ticker_class']

    if ticker_class == 'Livestock':
        data_out = bar_data[(bar_data['hour_minute'] >= 830) & (bar_data['hour_minute'] < 1305)]
    elif ticker_class == 'Ag':
        data_out = bar_data[((bar_data['hour_minute'] >= 1900) & (bar_data['hour_minute'] <= 2359)) |
                            (bar_data['hour_minute'] < 745) |
                            ((bar_data['hour_minute'] >= 830) & (bar_data['hour_minute'] < 1320))]
    elif ticker_class in ['Energy', 'STIR', 'Index', 'FX', 'Treasury', 'Metal']:
        data_out = bar_data[(bar_data['hour_minute'] < 1600) | (bar_data['hour_minute'] >= 1700)]

    return data_out






























