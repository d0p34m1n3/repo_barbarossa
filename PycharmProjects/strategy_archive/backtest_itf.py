

import get_price.quantgo_data as qd
import get_price.get_futures_price as gfp
import pandas as pd
import numpy as np
import shared.calendar_utilities as cu
import datetime as dt
import contract_utilities.contract_meta_info as cmi
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
    ticker_frame['close_diff'] = ticker_frame['close_price'].diff()
    daily_sd = np.std(ticker_frame['close_diff'].iloc[-41:-1])

    ticker_frame['ma50'] = ticker_frame['close_price'].rolling(window=50, center=False).mean()
    ticker_frame['ma200'] = ticker_frame['close_price'].rolling(window=200, center=False).mean()

    if ticker_frame['close_price'].iloc[-2]>ticker_frame['ma50'].iloc[-2]:
        trend1 = 1
    else:
        trend1 = -1

    if ticker_frame['ma50'].iloc[-2]>ticker_frame['ma200'].iloc[-2]:
        trend2 = 1
    else:
        trend2 = -1

    candle_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=20)

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']

    candle_frame['ewma300'] = candle_frame['close'].ewm(span=300, min_periods=250, adjust=True, ignore_na=False).mean()
    candle_frame['ewma50'] = candle_frame['close'].ewm(span=50, min_periods=40, adjust=True, ignore_na=False).mean()
    candle_frame['ma5'] = candle_frame['close'].rolling(window=5, center=False).mean()

    candle_frame['ewma300D'] = candle_frame['ewma300'] - candle_frame['ewma300'].shift(60)
    candle_frame['ewma300DN'] = candle_frame['ewma300D']/daily_sd
    candle_frame['ewma50D'] = candle_frame['ewma50'] - candle_frame['ewma50'].shift(10)

    candle_frame['min14'] = candle_frame['low'].rolling(window=14, center=False).min()
    candle_frame['max14'] = candle_frame['high'].rolling(window=14, center=False).max()

    candle_frame['william'] = -100 * (candle_frame['max14'] - candle_frame['close']) / (
                candle_frame['max14'] - candle_frame['min14'])
    candle_frame['datetime'] = [x.replace(hour=0, second=0, minute=0) for x in candle_frame.index]

    candle_frame['obs_no'] = range(len(candle_frame.index))

    candle_frame['bullishW'] = candle_frame['william'] > -20
    candle_frame['bearishW'] = candle_frame['william'] < -80

    candle_frame['bullishW'] = candle_frame['bullishW'].astype(int)
    candle_frame['bearishW'] = candle_frame['bearishW'].astype(int)

    candle_frame = candle_frame[np.isfinite(candle_frame['ewma300D'])]
    candle_frame['ewma300DS'] = np.sign(candle_frame['ewma300D'])
    candle_frame['ewma300DSDiff'] = abs(candle_frame['ewma300DS'].diff())

    turning_points = candle_frame[candle_frame['ewma300DSDiff'] != 0]
    turning_points['turning_points'] = turning_points['obs_no']

    merged_frame = pd.concat([candle_frame, turning_points['turning_points']], axis=1)
    merged_frame['turning_points'].iloc[0] = 0
    merged_frame['turning_points'] = merged_frame['turning_points'].fillna(method='ffill')
    merged_frame['trend_age'] = merged_frame['obs_no'] - merged_frame['turning_points']

    merged_frame['bullishWCumsum'] = merged_frame.groupby('turning_points')['bullishW'].transform(pd.Series.cumsum)
    merged_frame['bearishWCumsum'] = merged_frame.groupby('turning_points')['bearishW'].transform(pd.Series.cumsum)
    candle_frame = merged_frame

    candle_frame = candle_frame.dropna().reset_index(drop=True, inplace=False)
    date_timeto = cu.convert_doubledate_2datetime(date_to)

    daily_frame = candle_frame[candle_frame['datetime'] == date_timeto]
    daily_frame.reset_index(drop=True,inplace=True)

    tick_size = cmi.tick_size[ticker_head]
    latest_trade_exit_hour_minute = get_latest_trade_exit_hour_minute(ticker_head)

    current_position = 0

    direction_list = []
    entry_price_list = []
    exit_price_list = []
    entry_index_list = []
    exit_index_list = []
    entry_hour_minute_list = []
    stop_price_list = []
    target_price_list = []

    ewma300DN_list = []
    trend_age_list = []
    bullishWCumsum_list = []
    bearishWCumsum_list = []

    stop_adjustment_possible_Q = False
    long_trade_possible_Q = False
    short_trade_possible_Q = False
    breakout_price = np.nan

    for i in range(2, len(daily_frame.index)):

        if (daily_frame['ewma300D'].iloc[i]<0)|(daily_frame['william'].iloc[i]<=-80):
            long_trade_possible_Q = False

        if (daily_frame['ewma300D'].iloc[i]>0)|(daily_frame['william'].iloc[i]>=-20):
            short_trade_possible_Q = False

        if long_trade_possible_Q and (daily_frame['high'].iloc[i] > breakout_price + tick_size) \
                and (daily_frame['hour_minute'].iloc[i] <= 1100):

            long_trade_possible_Q = False
            current_position = 1
            direction_list.append(current_position)
            ewma300DN_list.append(daily_frame['ewma300DN'].iloc[i-1])
            trend_age_list.append(daily_frame['trend_age'].iloc[i - 1])
            bullishWCumsum_list.append(daily_frame['bullishWCumsum'].iloc[i - 1])
            bearishWCumsum_list.append(daily_frame['bearishWCumsum'].iloc[i - 1])
            entry_index_list.append(i)
            entry_hour_minute_list.append(daily_frame['hour_minute'].iloc[i])
            entry_price = breakout_price + 1.5 * tick_size
            entry_price_list.append(entry_price)
            stop_price_list.append(swing_low - tick_size)
            target_price_list.append(2 * entry_price - swing_low)
            breakout_price = np.nan
            continue

        if (current_position == 0) and (daily_frame['ewma300D'].iloc[i] > 0) and (daily_frame['william'].iloc[i - 1] <= -80) \
                and (daily_frame['william'].iloc[i] > -80) and (daily_frame['hour_minute'].iloc[i] <= 1100) and (daily_frame['hour_minute'].iloc[i] >= 830):
            long_trade_possible_Q = True
            breakout_price = daily_frame['high'].iloc[i]

            for j in range(len(daily_frame.index)):
                if j == 0:
                    swing_low = daily_frame['low'].iloc[i - 1]
                elif daily_frame['low'].iloc[i - 1 - j] <= swing_low:
                    swing_low = daily_frame['low'].iloc[i - 1 - j]
                elif daily_frame['low'].iloc[i - 1 - j] > swing_low:
                    break

        if short_trade_possible_Q and (daily_frame['low'].iloc[i] < breakout_price - tick_size) \
            and (daily_frame['hour_minute'].iloc[i] <= 1100):

            short_trade_possible_Q = False
            current_position = -1
            direction_list.append(current_position)
            ewma300DN_list.append(daily_frame['ewma300DN'].iloc[i - 1])
            trend_age_list.append(daily_frame['trend_age'].iloc[i - 1])
            bullishWCumsum_list.append(daily_frame['bullishWCumsum'].iloc[i - 1])
            bearishWCumsum_list.append(daily_frame['bearishWCumsum'].iloc[i - 1])
            entry_index_list.append(i)
            entry_hour_minute_list.append(daily_frame['hour_minute'].iloc[i])
            entry_price = breakout_price - 1.5 * tick_size
            entry_price_list.append(entry_price)
            stop_price_list.append(swing_high + tick_size)
            target_price_list.append(2 * entry_price - swing_high)
            breakout_price = np.nan
            continue


        if (current_position == 0) and (daily_frame['ewma300D'].iloc[i] < 0) and (daily_frame['william'].iloc[i - 1] >= -20) \
            and (daily_frame['william'].iloc[i] < -20) and (daily_frame['hour_minute'].iloc[i] <= 1100) and (daily_frame['hour_minute'].iloc[i] >= 830):

            short_trade_possible_Q = True
            breakout_price = daily_frame['high'].iloc[i]

            for j in range(len(daily_frame.index)):
                if j == 0:
                    swing_high = daily_frame['high'].iloc[i - 1]
                elif daily_frame['high'].iloc[i - 1 - j] >= swing_high:
                    swing_high = daily_frame['high'].iloc[i - 1 - j]
                elif daily_frame['high'].iloc[i - 1 - j] < swing_high:
                    break

        if (current_position > 0) and stop_adjustment_possible_Q and (
                daily_frame['close'].iloc[i-2] < daily_frame['ma5'].iloc[i-2]) \
                and (daily_frame['close'].iloc[i-1] < daily_frame['ma5'].iloc[i-1]):
            stop_price_list[-1] = min(daily_frame['low'].iloc[i-2],daily_frame['low'].iloc[i-1])

        if (current_position < 0) and stop_adjustment_possible_Q and (
            daily_frame['close'].iloc[i-2] > daily_frame['ma5'].iloc[i-2]) \
            and (daily_frame['close'].iloc[i-1] > daily_frame['ma5'].iloc[i-1]):
            stop_price_list[-1] = max(daily_frame['high'].iloc[i-2], daily_frame['high'].iloc[i-1])

        if (current_position > 0) and ((daily_frame['hour_minute'].iloc[i] >= latest_trade_exit_hour_minute)|(i==len(daily_frame.index)-1)):
            current_position = 0
            exit_price_list.append(daily_frame['open'].iloc[i] - 0.5 * tick_size)
            exit_index_list.append(i)
            stop_adjustment_possible_Q = False

        if (current_position < 0) and ((daily_frame['hour_minute'].iloc[i] >= latest_trade_exit_hour_minute)|(i==len(daily_frame.index)-1)):
            current_position = 0
            exit_price_list.append(daily_frame['open'].iloc[i] + 0.5 * tick_size)
            exit_index_list.append(i)
            stop_adjustment_possible_Q = False

        if (current_position > 0) and (daily_frame['low'].iloc[i] <= stop_price_list[-1]):
            current_position = 0
            exit_price_list.append(stop_price_list[-1] - 0.5 * tick_size)
            exit_index_list.append(i)
            stop_adjustment_possible_Q = False

        if (current_position < 0) and (daily_frame['high'].iloc[i] >= stop_price_list[-1]):
            current_position = 0
            exit_price_list.append(stop_price_list[-1] + 0.5 * tick_size)
            exit_index_list.append(i)
            stop_adjustment_possible_Q = False

        if (current_position > 0) and (daily_frame['high'].iloc[i] >= target_price_list[-1]):
            stop_adjustment_possible_Q = True

        if (current_position < 0) and (daily_frame['low'].iloc[i] <= target_price_list[-1]):
            stop_adjustment_possible_Q = True

    trades_frame = pd.DataFrame.from_items([('direction', direction_list),
                                            ('entry_price',entry_price_list),
                                            ('exit_price',exit_price_list),
                                            ('entry_index', entry_index_list),
                                            ('exit_index', exit_index_list),
                                            ('entry_hour_minute',entry_hour_minute_list),
                                            ('target_price',target_price_list),
                                            ('ewma300DN', ewma300DN_list),
                                            ('trend_age', trend_age_list),
                                            ('bullishWCumsum', bullishWCumsum_list),
                                            ('bearishWCumsum', bearishWCumsum_list)])

    trades_frame['pnl'] = (trades_frame['exit_price']-trades_frame['entry_price'])*trades_frame['direction']

    trades_frame['pnl_dollar'] = cmi.contract_multiplier[ticker_head]*trades_frame['pnl']
    trades_frame['stop_loss'] = abs(trades_frame['entry_price']-trades_frame['target_price'])
    trades_frame['daily_sd'] = daily_sd
    trades_frame['normalized_stop_loss'] = trades_frame['stop_loss']/daily_sd
    trades_frame['pnl_normalized'] = trades_frame['pnl'] / daily_sd

    trades_frame['ticker_head'] = ticker_head
    trades_frame['ticker'] = ticker
    trades_frame['trend1'] = trend1
    trades_frame['trend2'] = trend2
    trades_frame['trade_date'] = date_to

    return {'trades_frame': trades_frame, 'daily_frame': daily_frame}

def get_results_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='itf', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        trades_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'trades_frame': trades_frame,'success': True}

    ticker_head_list = cmi.cme_futures_tickerhead_list


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















