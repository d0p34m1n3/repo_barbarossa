
import os.path
import numpy as np
import pandas as pd
import shared.directory_names as dn
import shared.calendar_utilities as cu
import signals.technical_indicators as ti
import datetime as dt

def get_stock_mrl_signals(**kwargs):

    symbol = kwargs['symbol']
    settle_date = kwargs['settle_date']

    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    data_dir = dn.get_directory_name(ext='stock_data')

    file_name = data_dir + '/' + symbol + '.pkl'

    output_dictionary = {}
    output_dictionary['symbol'] = symbol
    output_dictionary['file_success'] = 0
    output_dictionary['data_success'] = 0
    output_dictionary['volume_success'] = 0
    output_dictionary['price_success'] = 0
    output_dictionary['dollar_volume_success'] = 0
    output_dictionary['trend_success'] = 0
    output_dictionary['adx_success'] = 0
    output_dictionary['atr_success'] = 0
    output_dictionary['rsi_success'] = 0

    output_dictionary['close_price'] = np.nan
    output_dictionary['ma150'] = np.nan
    output_dictionary['adx7'] = np.nan
    output_dictionary['atr_percent10'] = np.nan
    output_dictionary['rsi3'] = np.nan
    output_dictionary['atr10'] = np.nan

    try:
        price_data = pd.read_pickle(file_name)
    except:
        return output_dictionary

    output_dictionary['file_success'] = 1

    if sum(price_data['settle_datetime'] == settle_datetime)==0:
        return output_dictionary

    price_data = price_data[price_data['settle_datetime']<=settle_datetime]

    if len(price_data.index)<200:
        return output_dictionary

    output_dictionary['data_success'] = 1
    output_dictionary['close_price'] = price_data['close'].iloc[-1]

    if np.mean(price_data['volume'].iloc[-50:]) < 500000:
        return output_dictionary

    output_dictionary['volume_success'] = 1

    if price_data['close'].iloc[-1]<1:
        return output_dictionary

    output_dictionary['price_success'] = 1

    price_data['dollar_volume'] = price_data['volume'] * price_data['close']

    if np.mean(price_data['dollar_volume'].iloc[-50:])<2500000:
        return output_dictionary

    output_dictionary['dollar_volume_success'] = 1

    split_envents = price_data[price_data['split_coefficient'] != 1]
    split_index_list = split_envents.index

    for i in range(len(split_index_list)):
        price_data['close'].iloc[:split_index_list[i]] = \
            price_data['close'].iloc[:split_index_list[i]] / price_data['split_coefficient'].iloc[split_index_list[i]]

    price_data['ma150'] = pd.rolling_mean(price_data['close'], 150)
    output_dictionary['ma150'] = price_data['ma150'].iloc[-1]

    if price_data['close'].iloc[-1]<price_data['ma150'].iloc[-1]:
        return output_dictionary

    output_dictionary['trend_success'] = 1

    data_out = ti.get_adx(data_frame_input=price_data, period=7)
    output_dictionary['adx7'] = data_out['adx_7'].iloc[-1]

    if output_dictionary['adx7']<=45:
        return output_dictionary

    output_dictionary['adx_success'] = 1

    data_out = ti.get_atr(data_frame_input=price_data, period=10,percent_q=True)
    output_dictionary['atr_percent10'] = data_out['atr_10'].iloc[-1]

    if output_dictionary['atr_percent10']<=4:
        return output_dictionary

    output_dictionary['atr_success'] = 1

    price_data['change_1'] = price_data['close'].diff(1)

    data_out = ti.rsi(data_frame_input=price_data,change_field='change_1',period=3)
    output_dictionary['rsi3'] = data_out['rsi_3'].iloc[-1]

    if output_dictionary['rsi3']>=30:
        return output_dictionary

    output_dictionary['rsi_success'] = 1

    data_out = ti.get_atr(data_frame_input=price_data, period=10, percent_q=False)
    output_dictionary['atr10'] = data_out['atr_10'].iloc[-1]

    return output_dictionary

def get_stock_mrs_signals(**kwargs):

    symbol = kwargs['symbol']
    settle_date = kwargs['settle_date']

    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    data_dir = dn.get_directory_name(ext='stock_data')

    file_name = data_dir + '/' + symbol + '.pkl'

    output_dictionary = {}
    output_dictionary['symbol'] = symbol
    output_dictionary['file_success'] = 0
    output_dictionary['data_success'] = 0
    output_dictionary['volume_success'] = 0
    output_dictionary['price_success'] = 0
    output_dictionary['adx_success'] = 0
    output_dictionary['atr_success'] = 0
    output_dictionary['rsi_success'] = 0

    output_dictionary['close_price'] = np.nan
    output_dictionary['adx7'] = np.nan
    output_dictionary['atr_percent10'] = np.nan
    output_dictionary['rsi3'] = np.nan
    output_dictionary['atr10'] = np.nan

    try:
        price_data = pd.read_pickle(file_name)
    except:
        return output_dictionary

    output_dictionary['file_success'] = 1

    if sum(price_data['settle_datetime'] == settle_datetime)==0:
        return output_dictionary

    price_data = price_data[price_data['settle_datetime']<=settle_datetime]

    if len(price_data.index)<200:
        return output_dictionary

    output_dictionary['data_success'] = 1
    output_dictionary['close_price'] = price_data['close'].iloc[-1]

    if np.mean(price_data['volume'].iloc[-50:]) < 500000:
        return output_dictionary

    output_dictionary['volume_success'] = 1

    if price_data['close'].iloc[-1]<10:
        return output_dictionary

    output_dictionary['price_success'] = 1

    split_envents = price_data[price_data['split_coefficient'] != 1]
    split_index_list = split_envents.index

    for i in range(len(split_index_list)):
        price_data['close'].iloc[:split_index_list[i]] = \
            price_data['close'].iloc[:split_index_list[i]] / price_data['split_coefficient'].iloc[split_index_list[i]]

    data_out = ti.get_adx(data_frame_input=price_data, period=7)
    output_dictionary['adx7'] = data_out['adx_7'].iloc[-1]

    if output_dictionary['adx7']<=50:
        return output_dictionary

    output_dictionary['adx_success'] = 1

    data_out = ti.get_atr(data_frame_input=price_data, period=10,percent_q=True)
    output_dictionary['atr_percent10'] = data_out['atr_10'].iloc[-1]

    if output_dictionary['atr_percent10']<=5:
        return output_dictionary

    output_dictionary['atr_success'] = 1

    price_data['change_1'] = price_data['close'].diff(1)

    data_out = ti.rsi(data_frame_input=price_data,change_field='change_1',period=3)
    output_dictionary['rsi3'] = data_out['rsi_3'].iloc[-1]

    if output_dictionary['rsi3']<=85:
        return output_dictionary

    output_dictionary['rsi_success'] = 1

    data_out = ti.get_atr(data_frame_input=price_data, period=10, percent_q=False)
    output_dictionary['atr10'] = data_out['atr_10'].iloc[-1]

    return output_dictionary










