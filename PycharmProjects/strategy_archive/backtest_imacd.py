
import get_price.quantgo_data as qd
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import ta.strategy as ts
import shared.calendar_utilities as cu
import pandas as pd
import numpy as np
import os

# I've backtested this from 20151209 to 20180131 for the hourly bars
# the results were negative for insignificant for all of the tickers


def get_results_4ticker(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    freq_str = '1H'

    aux_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=35)

    candle_frame = pd.DataFrame()
    candle_frame['open'] = aux_frame['open'].resample(freq_str).first()
    candle_frame['high'] = aux_frame['high'].resample(freq_str).max()
    candle_frame['low'] = aux_frame['low'].resample(freq_str).min()
    candle_frame['close'] = aux_frame['close'].resample(freq_str).last()

    candle_frame['hour_minute'] = 100 * candle_frame.index.hour + candle_frame.index.minute

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_class = contract_specs_output['ticker_class']
    ticker_head = contract_specs_output['ticker_head']
    tick_size = cmi.tick_size[ticker_head]

    if ticker_class == 'Livestock':
        data_out = candle_frame[(candle_frame['hour_minute'] >= 830) & (candle_frame['hour_minute'] < 1305)]
    elif ticker_class == 'Ag':
        data_out = candle_frame[((candle_frame['hour_minute'] >= 1900) & (candle_frame['hour_minute'] <= 2359)) |
                            (candle_frame['hour_minute'] < 745) |
                            ((candle_frame['hour_minute'] >= 830) & (candle_frame['hour_minute'] < 1320))]
    elif ticker_class in ['Energy', 'STIR', 'Index', 'FX', 'Treasury', 'Metal']:
        data_out = candle_frame[(candle_frame['hour_minute'] < 1600) | (candle_frame['hour_minute'] >= 1700)]

    data_out = data_out[data_out['close'].notnull()]

    data_out['ewma12'] = candle_frame['close'].ewm(span=12, min_periods=12, adjust=True, ignore_na=False).mean()
    data_out['ewma26'] = candle_frame['close'].ewm(span=26, min_periods=26, adjust=True, ignore_na=False).mean()

    data_out['macd'] = data_out['ewma12']-data_out['ewma26']
    data_out['macds'] = data_out['macd'].rolling(window=9, center=False).mean()
    data_out['hist'] = data_out['macd']-data_out['macds']

    data_out['ma5'] = data_out['close'].rolling(window=5, center=False).mean()
    data_out['ma55'] = data_out['close'].rolling(window=55, center=False).mean()

    data_out = data_out[data_out['ma55'].notnull()]
    date_from = cu.doubledate_shift(date_to, 28)
    datetime_from = cu.convert_doubledate_2datetime(date_from)

    ticker_frame = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=date_from)
    ticker_frame['close_diff'] = ticker_frame['close_price'].diff()
    daily_sd = np.std(ticker_frame['close_diff'].iloc[-41:-1])

    data_out = data_out[data_out.index >= datetime_from]

    current_position = 0
    direction_list = []
    entry_hour_minute_list = []
    stop_price_list = []
    target_price_list = []
    entry_price_list = []
    entry_index_list = []

    exit_price_list = []
    exit_index_list = []

    target_price = np.nan
    stop_price = np.nan

    for i in range(1,len(data_out.index)):

        hist = data_out['hist'].iloc[i]
        hist_1 = data_out['hist'].iloc[i-1]
        madiff = data_out['ma5'].iloc[i]-data_out['ma55'].iloc[i]
        hour_minute = data_out['hour_minute'].iloc[i]
        close = data_out['close'].iloc[i]
        high = data_out['high'].iloc[i]
        low = data_out['low'].iloc[i]

        if (current_position==0) and (hist>0) and (hist_1<0) and (madiff>0) and (i<len(data_out.index)-4):

            current_position = 1
            direction_list.append(current_position)
            entry_hour_minute_list.append(hour_minute)

            for j in range(i):
                if j == 0:
                    swing_low = data_out['low'].iloc[i]
                elif data_out['low'].iloc[i-j] <= swing_low:
                    swing_low = data_out['low'].iloc[i-j]
                elif data_out['low'].iloc[i-j] > swing_low:
                    break

            entry_price = close+tick_size
            stop_price = swing_low-tick_size
            stop_price_list.append(stop_price)
            entry_price_list.append(entry_price)
            entry_index_list.append(i)
            target_price = entry_price + 2 *(entry_price - swing_low)
            target_price_list.append(target_price)
            continue


        if (current_position==0) and (hist<0) and (hist_1>0) and (madiff<0) and (i<len(data_out.index)-4):

            current_position = -1
            direction_list.append(current_position)
            entry_hour_minute_list.append(hour_minute)

            for j in range(i):
                if j == 0:
                    swing_high = data_out['high'].iloc[i]
                elif data_out['high'].iloc[i-j] >= swing_high:
                    swing_high = data_out['high'].iloc[i - j]
                elif data_out['high'].iloc[i - j] < swing_high:
                    break

            entry_price = close - tick_size
            stop_price = swing_high+tick_size
            stop_price_list.append(stop_price)
            entry_price_list.append(entry_price)
            entry_index_list.append(i)
            target_price = entry_price - 2 * (swing_high - entry_price)
            target_price_list.append(target_price)
            continue

        if (current_position>0) and (high>target_price):
            current_position = 0
            exit_price_list.append(target_price-tick_size)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position>0) and (low<stop_price):
            current_position = 0
            exit_price_list.append(stop_price-tick_size)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position>0) and ((hist<0) or (i==(len(data_out.index)-1))):
            current_position = 0
            exit_price_list.append(close-tick_size)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position < 0) and (low < target_price):
            current_position = 0
            exit_price_list.append(target_price + tick_size)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position < 0) and (high > stop_price):
            current_position = 0
            exit_price_list.append(stop_price + tick_size)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position < 0) and ((hist > 0) or (i == (len(data_out.index) - 1))):
            current_position = 0
            exit_price_list.append(close + tick_size)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

    trades_frame = pd.DataFrame.from_items([('direction', direction_list),
                                            ('entry_price', entry_price_list),
                                            ('exit_price', exit_price_list),
                                            ('entry_index', entry_index_list),
                                            ('exit_index', exit_index_list),
                                            ('stop_price', stop_price_list),
                                            ('entry_hour_minute', entry_hour_minute_list)])

    trades_frame['pnl'] = (trades_frame['exit_price'] - trades_frame['entry_price']) * trades_frame['direction']

    trades_frame['pnl_dollar'] = cmi.contract_multiplier[ticker_head] * trades_frame['pnl']
    trades_frame['stop_loss'] = abs(trades_frame['entry_price'] - trades_frame['stop_price'])
    trades_frame['daily_sd'] = daily_sd
    trades_frame['stop_loss_norm'] = trades_frame['stop_loss']/daily_sd
    trades_frame['pnl_norm'] = trades_frame['pnl'] / daily_sd
    trades_frame['ticker_head'] = ticker_head
    trades_frame['ticker'] = ticker
    trades_frame['trade_date'] = date_to

    return {'trades_frame':trades_frame, 'price_frame': data_out }

def get_results_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='imacd', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        trades_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'trades_frame': trades_frame, 'success': True}

    ticker_head_list = cmi.cme_futures_tickerhead_list

    data_list = [gfp.get_futures_price_preloaded(ticker_head=x, settle_date=date_to) for x in
                 ticker_head_list]
    ticker_frame = pd.concat(data_list)
    ticker_frame = ticker_frame[~((ticker_frame['ticker_head'] == 'ED') & (ticker_frame['tr_dte'] < 250))]

    ticker_frame.sort_values(['ticker_head', 'volume'], ascending=[True, False], inplace=True)
    ticker_frame.drop_duplicates(subset=['ticker_head'], keep='first', inplace=True)

    result_list = [get_results_4ticker(ticker=x, date_to=date_to) for x in ticker_frame['ticker']]
    trades_frame = pd.concat([x['trades_frame'] for x in result_list])

    trades_frame.to_pickle(output_dir + '/summary.pkl')

    return {'trades_frame': trades_frame, 'success': True}


















