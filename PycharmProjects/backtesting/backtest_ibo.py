
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py

import get_price.quantgo_data as qd
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import signals.ibo as sibo
import numpy as np
import pandas as pd
import ta.strategy as ts
import os

latest_trade_entry_hour_minute = 1100
latest_livestock_exit_hour_minute = 1255
latest_ag_exit_hour_minute = 1310
latest_macro_exit_hour_minute = 1555


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

    wait4_eoc_volume_filter = kwargs['wait4_eoc_volume_filter']
    wait4_eoc = wait4_eoc_volume_filter[0]
    volume_filter = wait4_eoc_volume_filter[1]
    trend_filter_type = kwargs['trend_filter_type']
    stop_loss_multiplier = kwargs['stop_loss_multiplier']  # 0.3
    distance_multiplier = kwargs['distance_multiplier']  # 0.5
    time_section_no = kwargs['time_section_no']  # 1

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    tick_size = cmi.tick_size[ticker_head]
    latest_trade_exit_hour_minute = get_latest_trade_exit_hour_minute(ticker_head)

    settle_frame = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=date_to)
    settle_frame = settle_frame.iloc[:-1]

    settle_frame['ewma_3'] = settle_frame['close_price'].ewm(span=3,min_periods=3,adjust=True,ignore_na=False).mean()
    settle_frame['ewma_5'] = settle_frame['close_price'].ewm(span=5,min_periods=5,adjust=True,ignore_na=False).mean()
    settle_frame['ewma_10'] = settle_frame['close_price'].ewm(span=10,min_periods=10,adjust=True,ignore_na=False).mean()

    trend_direction = 0

    if trend_filter_type in ['3', '5', '10']:
        if settle_frame['close_price'].iloc[-1] > settle_frame['ewma_' + trend_filter_type].iloc[-1]:
            trend_direction = 1
        else:
            trend_direction = -1

    avg_volume = np.mean(settle_frame['volume'].iloc[-40:])

    settle_frame['close_diff'] = settle_frame['close_price'].diff()
    daily_sd = np.std(settle_frame['close_diff'].iloc[-40:])

    distance_out = sibo.get_distance(settle_frame=settle_frame,distance_type='daily_sd',distance_multiplier=distance_multiplier)

    candle_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=10)

    time_filter_output = sibo.get_time_filter(ticker_head=ticker_head,section_no=time_section_no)
    start_hour_minute = time_filter_output['start_hour_minute']
    end_hour_minute = time_filter_output['end_hour_minute']

    candle_frame['datetime'] = [x.replace(hour=0, second=0, minute=0) for x in candle_frame.index]
    candle_frame['total_volume'] = candle_frame['buy_volume'] + candle_frame['sell_volume'] + candle_frame['volume']

    candle_frame = candle_frame.dropna().reset_index(drop=True, inplace=False)
    date_timeto = cu.convert_doubledate_2datetime(date_to)

    hist_frame = candle_frame[(candle_frame['hour_minute'] >= start_hour_minute) &
                              (candle_frame['hour_minute'] <= end_hour_minute)&
                              (candle_frame['datetime'] < date_timeto)]

    hist_frame['normalized_volume'] = hist_frame['total_volume'] / avg_volume
    volume_filter_level = hist_frame['normalized_volume'].quantile(0.7)

    price_frame = candle_frame[(candle_frame['datetime'] == date_timeto) & (candle_frame['hour_minute'] >= 830)]
    price_frame.reset_index(drop=True, inplace=True)

    poi_out = sibo.get_poi(candle_frame=price_frame, poi_type='open')

    poi = poi_out['poi']

    if not poi_out['success']:
        return {'trades_frame': pd.DataFrame(), 'price_frame': pd.DataFrame()}

    current_position = 0
    entry_price_list = []
    exit_price_list = []
    entry_index_list = []
    exit_index_list = []
    direction_list = []
    hour_minute_list = []
    volume_list = []

    for i in range(1,len(price_frame.index)):

        hour_minute = price_frame['hour_minute'].iloc[i]

        if wait4_eoc:
            long_breakoutQ = (price_frame['close'].iloc[i] >= poi + distance_out + tick_size) and \
            (price_frame['close'].iloc[i-1] < poi + distance_out + tick_size)
            short_breakoutQ = (price_frame['close'].iloc[i] <= poi - distance_out - tick_size) and \
                              (price_frame['close'].iloc[i-1] > poi - distance_out - tick_size)

            long_entry_price = price_frame['close'].iloc[i] + tick_size
            short_entry_price = price_frame['close'].iloc[i] - tick_size
        else:
            long_breakoutQ = (price_frame['high'].iloc[i] >= poi + distance_out + tick_size) and \
                             (price_frame['close'].iloc[i - 1] < poi + distance_out + tick_size)
            short_breakoutQ = (price_frame['low'].iloc[i] <= poi - distance_out - tick_size) and \
                              (price_frame['close'].iloc[i - 1] > poi - distance_out - tick_size)

            long_entry_price = poi+distance_out+2*tick_size
            short_entry_price = poi-distance_out-2*tick_size

        if volume_filter:
            if (price_frame['total_volume'].iloc[i] / avg_volume)<volume_filter_level:
                long_breakoutQ = False
                short_breakoutQ = False

        # long breakout
        if (current_position==0) and long_breakoutQ \
                and (hour_minute>=start_hour_minute) and (hour_minute<=end_hour_minute) and (trend_direction>=0):
            current_position = 1
            entry_price = long_entry_price
            stop_price = entry_price-stop_loss_multiplier*distance_out
            entry_price_list.append(entry_price)
            direction_list.append(current_position)
            entry_index_list.append(i)
            hour_minute_list.append(hour_minute)
            volume_list.append(price_frame['total_volume'].iloc[i]/avg_volume)
            continue

        # short breakout
        if (current_position==0) and (short_breakoutQ) \
                and (hour_minute>=start_hour_minute) and (hour_minute<=end_hour_minute)and (trend_direction<=0):
            current_position = -1
            entry_price = short_entry_price
            stop_price = entry_price+stop_loss_multiplier*distance_out
            entry_price_list.append(entry_price)
            direction_list.append(current_position)
            entry_index_list.append(i)
            hour_minute_list.append(hour_minute)
            volume_list.append(price_frame['total_volume'].iloc[i] / avg_volume)
            continue

        # long stop out
        if (current_position==1) and (price_frame['low'].iloc[i]<=stop_price):
            exit_price_list.append(stop_price-tick_size)
            exit_index_list.append(i)
            current_position = 0

        # short stop out
        if (current_position==-1) and (price_frame['high'].iloc[i]>=stop_price):
            exit_price_list.append(stop_price+tick_size)
            exit_index_list.append(i)
            current_position = 0

        if (current_position>0) and ((hour_minute>=latest_trade_exit_hour_minute) or (i==len(price_frame.index)-1)):
            exit_price_list.append(price_frame['open'].iloc[i] - tick_size)
            exit_index_list.append(i)
            break

        if (current_position<0) and ((hour_minute>=latest_trade_exit_hour_minute) or (i==len(price_frame.index)-1)):
            exit_price_list.append(price_frame['open'].iloc[i] + tick_size)
            exit_index_list.append(i)
            break

    trades_frame = pd.DataFrame.from_dict({'direction': direction_list,
                                            'hour_minute': hour_minute_list,
                                            'entry_price': entry_price_list,
                                             'exit_price': exit_price_list,
                                            'entry_index': entry_index_list,
                                            'exit_index': exit_index_list,
                                               'volume':volume_list})

    trades_frame['pnl'] = (trades_frame['exit_price'] - trades_frame['entry_price']) * trades_frame['direction']
    trades_frame['pnl_dollar'] = cmi.contract_multiplier[ticker_head] * trades_frame['pnl']
    trades_frame['pnl_normalized'] = trades_frame['pnl'] / daily_sd
    trades_frame['ticker_head'] = ticker_head
    trades_frame['ticker'] = ticker
    trades_frame['trade_date'] = date_to



    return {'trades_frame': trades_frame, 'price_frame': price_frame}



def get_results_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=date_to)

    wait4_eoc_volume_filter = kwargs['wait4_eoc_volume_filter']
    trend_filter_type = kwargs['trend_filter_type']
    stop_loss_multiplier = kwargs['stop_loss_multiplier']  # 0.3
    distance_multiplier = kwargs['distance_multiplier']  # 0.5
    time_section_no = kwargs['time_section_no']  # 1
    use_filesQ = kwargs['use_filesQ']

    file_name = output_dir + '/summary.pkl'

    if use_filesQ and os.path.isfile(file_name):
        trades_frame = pd.read_pickle(file_name)
        return {'trades_frame': trades_frame,'success': True}

    ticker_head_list = cmi.cme_futures_tickerhead_list
    #ticker_head_list = ['CL']

    data_list = [gfp.get_futures_price_preloaded(ticker_head=x, settle_date=date_to) for x in
                 ticker_head_list]

    ticker_frame = pd.concat(data_list)
    ticker_frame = ticker_frame[~((ticker_frame['ticker_head'] == 'ED') & (ticker_frame['tr_dte'] < 250))]

    ticker_frame.sort_values(['ticker_head', 'volume'], ascending=[True, False], inplace=True)
    ticker_frame.drop_duplicates(subset=['ticker_head'], keep='first', inplace=True)

    result_list = [get_results_4ticker(ticker=x,date_to=date_to,
                                        wait4_eoc_volume_filter=wait4_eoc_volume_filter,
                                        trend_filter_type=trend_filter_type,
                                        stop_loss_multiplier=stop_loss_multiplier,
                                        distance_multiplier=distance_multiplier,
                                        time_section_no=time_section_no) for x in ticker_frame['ticker']]

    trades_frame = pd.concat([x['trades_frame']  for x in result_list])

    if use_filesQ:
        trades_frame.to_pickle(file_name)

    return {'trades_frame': trades_frame, 'success': True}


def optimizer(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=date_to)

    file_name = output_dir + '/optimizer.pkl'

    if os.path.isfile(file_name):
        optimizer_frame = pd.read_pickle(file_name)
        return {'optimizer_frame': optimizer_frame, 'success': True}

    wait4_eoc_volume_filter_list = [[True,True],[True,False],[False,False]]
    trend_filter_type_list = ['no_filter', '3', '5', '10']
    stop_loss_multiplier_list = [0.25, 0.5, 0.75, 1, 1.5]
    distance_multiplier_list = [0.25, 0.5, 0.75, 1, 1.5]
    time_section_no_list = [1, 2, 3]

    trades_frame_list = []

    for i in range(len(wait4_eoc_volume_filter_list)):

        wait4_eoc_volume_filter = wait4_eoc_volume_filter_list[i]

        for j in range(len(trend_filter_type_list)):
            trend_filter_type = trend_filter_type_list[j]
            for k in range(len(stop_loss_multiplier_list)):
                stop_loss_multiplier = stop_loss_multiplier_list[k]
                for l in range(len(distance_multiplier_list)):
                    distance_multiplier = distance_multiplier_list[l]
                    for m in range(len(time_section_no_list)):
                        time_section_no = time_section_no_list[m]

                        output = get_results_4date(date_to=date_to,wait4_eoc_volume_filter=wait4_eoc_volume_filter,
                                              trend_filter_type=trend_filter_type,
                                              stop_loss_multiplier=stop_loss_multiplier,
                                              distance_multiplier=distance_multiplier,
                                              time_section_no=time_section_no,
                                                   use_filesQ=False)
                        trades_frame = output['trades_frame']

                        if len(trades_frame.index)>0:
                            trades_frame['wait4_eoc'] = wait4_eoc_volume_filter[0]
                            trades_frame['volume_filter'] = wait4_eoc_volume_filter[1]
                            trades_frame['trend_filter_type'] = trend_filter_type
                            trades_frame['stop_loss_multiplier'] = stop_loss_multiplier
                            trades_frame['distance_multiplier'] = distance_multiplier
                            trades_frame['time_section_no'] = time_section_no
                            trades_frame_list.append(trades_frame)

    if len(trades_frame_list)>0:
        optimizer_frame = pd.concat(trades_frame_list)
    else:
        optimizer_frame = pd.DataFrame()

    optimizer_frame.to_pickle(file_name)

    return {'optimizer_frame': optimizer_frame, 'success': True}

