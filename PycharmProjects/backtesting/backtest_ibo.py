
import get_price.quantgo_data as qd
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import numpy as np
import pandas as pd
import ta.strategy as ts
import os

latest_trade_entry_hour_minute = 1100
latest_livestock_exit_hour_minute = 1255
latest_ag_exit_hour_minute = 1310
latest_macro_exit_hour_minute = 1455


def get_latest_trade_exit_hour_minute(ticker_head):

    if ticker_head in ['LN', 'LC', 'FC']:
        return latest_livestock_exit_hour_minute
    elif ticker_head in ['C', 'S', 'SM', 'BO', 'W', 'KW']:
        return latest_ag_exit_hour_minute
    else:
        return latest_macro_exit_hour_minute

def get_results_4ticker(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    ticker_frame = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=date_to)

    ticker_frame['daily_range'] = ticker_frame['high_price'] - ticker_frame['low_price']
    ticker_frame['min_range'] = ticker_frame['daily_range'].rolling(window=7, center=False).min()

    if ticker_frame['daily_range'].iloc[-2]>ticker_frame['min_range'].iloc[-2]:
        return {'trades_frame': pd.DataFrame(), 'daily_frame': pd.DataFrame()}

    yesterdays_high = ticker_frame['high_price'].iloc[-2]
    yesterdays_low = ticker_frame['low_price'].iloc[-2]

    ticker_frame['close_diff'] = ticker_frame['close_price'].diff()
    daily_sd = np.std(ticker_frame['close_diff'].iloc[-41:-1])

    candle_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=20)

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    tick_size = cmi.tick_size[ticker_head]
    latest_trade_exit_hour_minute = get_latest_trade_exit_hour_minute(ticker_head)

    candle_frame['ewma300'] = candle_frame['close'].ewm(span=300, min_periods=250, adjust=True, ignore_na=False).mean()
    candle_frame['ewma50'] = candle_frame['close'].ewm(span=50, min_periods=40, adjust=True, ignore_na=False).mean()
    candle_frame['datetime'] = [x.replace(hour=0, second=0, minute=0) for x in candle_frame.index]

    candle_frame = candle_frame.dropna().reset_index(drop=True, inplace=False)
    date_timeto = cu.convert_doubledate_2datetime(date_to)

    daily_frame = candle_frame[(candle_frame['datetime'] == date_timeto)&(candle_frame['hour_minute'] >= 830)]
    daily_frame.reset_index(drop=True, inplace=True)

    daily_frame['running_max'] = daily_frame['high'].rolling(window=20, min_periods=10, center=False).max()

    if len(daily_frame.index)==0:
        return {'trades_frame': pd.DataFrame(), 'daily_frame': pd.DataFrame()}

    trading_data = daily_frame

    if daily_frame['ewma50'].iloc[0]>daily_frame['ewma300'].iloc[0]:
        trend = 1
    else:
        trend = -1

    current_position = 0
    entry_price_list  = []
    exit_price_list = []
    entry_index_list = []
    exit_index_list = []
    direction_list = []
    hour_minute_list = []

    for i in range(len(trading_data.index)):

        hour_minute = daily_frame['hour_minute'].iloc[i]

        if (current_position==0) and (trading_data['close'].iloc[i]>=yesterdays_high) \
                and (hour_minute<=latest_trade_entry_hour_minute):
            current_position = 1
            direction_list.append(current_position)
            entry_price_list.append(trading_data['close'].iloc[i]+tick_size)
            entry_index_list.append(i)
            hour_minute_list.append(hour_minute)

        if (current_position==0) and (trading_data['close'].iloc[i]<=yesterdays_low) \
                and (hour_minute<=latest_trade_entry_hour_minute):
            current_position = -1
            direction_list.append(current_position)
            entry_price_list.append(trading_data['close'].iloc[i]-tick_size)
            entry_index_list.append(i)
            hour_minute_list.append(hour_minute)

        if (current_position>0) and ((hour_minute>=latest_trade_exit_hour_minute) or (i==len(trading_data.index)-1)):
            exit_price_list.append(trading_data['open'].iloc[i] - tick_size)
            exit_index_list.append(i)
            break

        if (current_position<0) and ((hour_minute>=latest_trade_exit_hour_minute) or (i==len(trading_data.index)-1)):
            exit_price_list.append(trading_data['open'].iloc[i] + tick_size)
            exit_index_list.append(i)
            break

    trades_frame = pd.DataFrame.from_items([('direction', direction_list),
                                            ('hour_minute',hour_minute_list),
                                            ('entry_price', entry_price_list),
                                            ('exit_price', exit_price_list),
                                            ('entry_index', entry_index_list),
                                            ('exit_index', exit_index_list)])

    trades_frame['pnl'] = (trades_frame['exit_price'] - trades_frame['entry_price']) * trades_frame['direction']
    trades_frame['pnl_dollar'] = cmi.contract_multiplier[ticker_head] * trades_frame['pnl']
    trades_frame['pnl_normalized'] = trades_frame['pnl'] / daily_sd
    trades_frame['trend'] = trend
    trades_frame['ticker_head'] = ticker_head
    trades_frame['ticker'] = ticker
    trades_frame['trade_date'] = date_to


    return {'trades_frame': trades_frame, 'daily_frame': daily_frame}

def get_results_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        trades_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'trades_frame': trades_frame,'success': True}

    ticker_head_list = cmi.cme_futures_tickerhead_list
    #ticker_head_list = ['ES', 'NQ']

    data_list = [gfp.get_futures_price_preloaded(ticker_head=x, settle_date=date_to) for x in
                 ticker_head_list]

    ticker_frame = pd.concat(data_list)
    ticker_frame = ticker_frame[~((ticker_frame['ticker_head'] == 'ED') & (ticker_frame['tr_dte'] < 250))]

    ticker_frame.sort_values(['ticker_head', 'volume'], ascending=[True, False], inplace=True)
    ticker_frame.drop_duplicates(subset=['ticker_head'], keep='first', inplace=True)

    result_list = [get_results_4ticker(ticker=x,date_to=date_to) for x in ticker_frame['ticker']]
    trades_frame = pd.concat([x['trades_frame']  for x in result_list])

    trades_frame.to_pickle(output_dir + '/summary.pkl')

    return {'trades_frame': trades_frame, 'success': True}

