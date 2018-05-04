
import get_price.quantgo_data as qd
import shared.calendar_utilities as cu
import shared.directory_names as dn
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import backtesting.utilities as bu
import numpy as np
import pandas as pd
import os



def get_results_4ticker(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']
    trend_detection_period = kwargs['trend_detection_period']
    trigger_period = kwargs['trigger_period']
    stop_multiplier = kwargs['stop_multiplier']
    target_multiplier = kwargs['target_multiplier']

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    tick_size = cmi.tick_size[ticker_head]

    candle_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=20)

    candle_frame['trend_ma'] = candle_frame['close'].rolling(window=trend_detection_period, center=False).mean()
    candle_frame['trigger_ma'] = candle_frame['close'].rolling(window=trigger_period, center=False).mean()

    candle_frame['trend_maD'] = candle_frame['trend_ma'] - candle_frame['trend_ma'].shift(5)
    candle_frame['trigger_maD'] = candle_frame['trigger_ma'] - candle_frame['trigger_ma'].shift(1)

    ticker_frame = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=cu.doubledate_shift(date_to, 30))
    ticker_frame['close_diff'] = ticker_frame['close_price'].diff()
    daily_sd = np.std(ticker_frame['close_diff'].iloc[-41:-1])

    stop_distance = stop_multiplier*daily_sd
    target_distance = target_multiplier * daily_sd

    current_position = 0
    direction_list = []
    entry_price_list = []
    exit_price_list = []
    entry_index_list = []
    exit_index_list = []
    stop_price = np.nan
    target_price = np.nan

    for i in range(50, len(candle_frame.index)):

        trend_maD = candle_frame['trend_maD'].iloc[i]
        trigger_maD = candle_frame['trigger_maD'].iloc[i]
        close = candle_frame['close'].iloc[i]
        close_1 = candle_frame['close'].iloc[i-1]
        open = candle_frame['open'].iloc[i]

        high = candle_frame['high'].iloc[i]
        low = candle_frame['low'].iloc[i]

        trigger_ma = candle_frame['trigger_ma'].iloc[i]
        trigger_ma_1 = candle_frame['trigger_ma'].iloc[i-1]

        if (current_position == 0) and (close_1<trigger_ma_1) and (close>trigger_ma) and (trend_maD>0) and (trigger_maD<0):
            current_position = 1
            direction_list.append(current_position)
            entry_price_list.append(close+tick_size)
            entry_index_list.append(i)
            stop_price = close-stop_distance
            target_price = close + target_distance

        if (current_position == 0) and (close_1>trigger_ma_1) and (close<trigger_ma) and (trend_maD<0) and (trigger_maD>0):
            current_position = -1
            direction_list.append(current_position)
            entry_price_list.append(close-tick_size)
            entry_index_list.append(i)
            stop_price = close+stop_distance
            target_price = close - target_distance

        if (current_position>0) and (high>target_price+tick_size):
            current_position = 0
            exit_price_list.append(target_price)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position>0) and (low<stop_price+tick_size):
            current_position = 0
            exit_price_list.append(stop_price)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position>0) and (i==len(candle_frame.index)-1):
            exit_price_list.append(open-tick_size)
            exit_index_list.append(i)
            break

        if (current_position<0) and (high>stop_price-tick_size):
            current_position = 0
            exit_price_list.append(stop_price)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan

        if (current_position<0) and (low<target_price-tick_size):
            current_position = 0
            exit_price_list.append(target_price)
            exit_index_list.append(i)
            stop_price = np.nan
            target_price = np.nan


        if (current_position<0) and (i==len(candle_frame.index)-1):
            exit_price_list.append(open+tick_size)
            exit_index_list.append(i)
            break

    trades_frame = pd.DataFrame.from_items([('direction', direction_list),
                                                ('entry_price', entry_price_list),
                                                ('exit_price', exit_price_list),
                                                ('entry_index', entry_index_list),
                                                ('exit_index', exit_index_list)])

    trades_frame['pnl'] = (trades_frame['exit_price'] - trades_frame['entry_price']) * trades_frame['direction']

    trades_frame['pnl_dollar'] = cmi.contract_multiplier[ticker_head] * trades_frame['pnl']

    return {'trades_frame': trades_frame, 'candle_frame': candle_frame}

def get_results_4given_inputs(**kwargs):

    ticker_head = kwargs['ticker_head']
    trend_detection_period = kwargs['trend_detection_period']
    trigger_period = kwargs['trigger_period']
    stop_multiplier = kwargs['stop_multiplier']
    target_multiplier = kwargs['target_multiplier']

    backtest_dates_out = bu.get_backtesting_dates(date_to=20160131, years_back=2)
    double_dates = backtest_dates_out['double_dates']
    backtesting_dates = double_dates[0::4]

    output_dir = dn.get_directory_name(ext='optimization')

    file_name = output_dir + '/' + ticker_head + '_' + str(trend_detection_period) + '_' + str(trigger_period) + '_' + str(stop_multiplier) + '_' + str(target_multiplier) + '.pkl'

    if os.path.isfile(file_name):
        trades_frame = pd.read_pickle(file_name)
        return {'trades_frame': trades_frame, 'success': True}

    trades_frame_list = []

    for i in range(len(backtesting_dates)):
        daily_frame = gfp.get_futures_price_preloaded(ticker_head = ticker_head,settle_date=backtesting_dates[i])
        daily_frame.sort_values('volume',ascending=False,inplace=True)

        results_out = get_results_4ticker(ticker=daily_frame['ticker'].iloc[0], date_to=backtesting_dates[i],
                                              trend_detection_period=trend_detection_period,
                                              trigger_period=trigger_period,
                                              stop_multiplier=stop_multiplier, target_multiplier=target_multiplier)

        trades_frame_list.append(results_out['trades_frame'])

    trades_frame = pd.concat(trades_frame_list)

    trades_frame.to_pickle(file_name)

    return {'trades_frame': trades_frame, 'success': True}

def optimize_4_tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    trend_detection_list = [50, 100, 150, 200, 250]
    trigger_list = [3, 4, 5, 7, 10]
    stop_list = [0.2, 0.5, 0.7, 1, 1.5]
    target_list = [0.5, 1, 1.5, 2, 2.5, 3]

    best_trend_detection = trend_detection_list[0]
    best_trigger = trigger_list[0]
    best_stop = stop_list[0]
    best_target = target_list[0]
    best_pnl = np.nan

    for i in range(len(trend_detection_list)):
        trend_detection_period = trend_detection_list[i]
        for j in range(len(trigger_list)):
            trigger_period = trigger_list[j]
            for k in range(len(stop_list)):
                stop_multiplier = stop_list[k]
                for m in range(len(target_list)):
                    target_multiplier = target_list[m]
                    results_out = get_results_4given_inputs(ticker_head=ticker_head,
                                             trend_detection_period=trend_detection_period,
                                             trigger_period=trigger_period,
                                             stop_multiplier=stop_multiplier, target_multiplier=target_multiplier)

                    if results_out['trades_frame']['pnl_dollar'].sum()<best_pnl:
                        continue

                    best_pnl = results_out['trades_frame']['pnl_dollar'].sum()
                    best_trend_detection = trend_detection_period
                    best_trigger = trigger_period
                    best_stop = stop_multiplier
                    best_target = target_multiplier

    return {'best_pnl': best_pnl, 'best_trend_detection': best_trend_detection, 'best_trigger': best_trigger,
            'best_stop': best_stop, 'best_target': best_target}















