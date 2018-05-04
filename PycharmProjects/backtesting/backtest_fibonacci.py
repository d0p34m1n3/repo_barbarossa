
import get_price.quantgo_data as qd
import shared.calendar_utilities as cu
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import numpy as np
import pandas as pd

def get_results_4ticker(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    tick_size = cmi.tick_size[ticker_head]

    candle_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=2)
    candle_frame['datetime'] = [x.replace(hour=0, second=0, minute=0) for x in candle_frame.index]

    fractal_high = np.nan
    fractal_low = np.nan

    date_timeto = cu.convert_doubledate_2datetime(date_to)

    candle_frame['running_max'] = candle_frame['high'].rolling(window=10, min_periods=10, center=False).max()
    candle_frame['running_min'] = candle_frame['low'].rolling(window=10, min_periods=10, center=False).min()
    candle_frame['fib50'] = (candle_frame['running_max'] + candle_frame['running_min']) / 2

    candle_frame['ma20'] = candle_frame['close'].rolling(window=20, center=False).mean()
    candle_frame['ma20D'] = candle_frame['ma20'] - candle_frame['ma20'].shift(2)

    daily_frame = candle_frame[(candle_frame['datetime'] == date_timeto) & (candle_frame['hour_minute'] >= 830)]
    daily_frame.reset_index(drop=True, inplace=True)

    ticker_frame = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=date_to)
    ticker_frame['close_diff'] = ticker_frame['close_price'].diff()
    daily_sd = np.std(ticker_frame['close_diff'].iloc[-41:-1])

    profit_target = daily_sd/4
    stop_loss = daily_sd/4

    trend_bias = 0
    current_position = 0

    direction_list = []
    entry_price_list = []
    exit_price_list = []
    entry_index_list = []
    exit_index_list = []

    for i in range(10,len(daily_frame.index)):

        high = daily_frame['high'].iloc[i]
        low = daily_frame['low'].iloc[i]
        running_max = daily_frame['running_max'].iloc[i]
        running_min = daily_frame['running_min'].iloc[i]
        fib50 = daily_frame['fib50'].iloc[i]
        fib50_1 = daily_frame['fib50'].iloc[i-1]

        close_1 = daily_frame['close'].iloc[i-1]
        close = daily_frame['close'].iloc[i]
        ma20D =daily_frame['ma20D'].iloc[i]

        if (current_position==0) and (ma20D>0) and (close_1<fib50_1) and (close>fib50) :
            current_position = 1
            direction_list.append(current_position)
            entry_price_list.append(close+tick_size)
            entry_index_list.append(i)
            target = close+tick_size+profit_target
            stop = close+tick_size-stop_loss

        if (current_position == 0) and (ma20D < 0) and (close_1 > fib50_1) and (close < fib50):
            current_position = -1
            direction_list.append(current_position)
            entry_price_list.append(close - tick_size)
            entry_index_list.append(i)
            target = close - tick_size - profit_target
            stop = close - tick_size + stop_loss

        if (current_position>0):
            if high>=target+tick_size:
                exit_price_list.append(target)
                exit_index_list.append(i)
                target = np.nan
                stop = np.nan
                current_position = 0

            if ma20D<0:
                exit_price_list.append(close-tick_size)
                exit_index_list.append(i)
                target = np.nan
                stop = np.nan
                current_position = 0

            if i==len(daily_frame)-1:
                exit_price_list.append(daily_frame['open'].iloc[i]-tick_size)
                exit_index_list.append(i)
                target = np.nan
                stop = np.nan
                current_position = 0

        if (current_position < 0):
            if low <= target - tick_size:
                exit_price_list.append(target)
                exit_index_list.append(i)
                target = np.nan
                stop = np.nan
                current_position = 0

            if ma20D>0:
                exit_price_list.append(close+tick_size)
                exit_index_list.append(i)
                target = np.nan
                stop = np.nan
                current_position = 0

            if i == len(daily_frame) - 1:
                exit_price_list.append(daily_frame['open'].iloc[i] + tick_size)
                exit_index_list.append(i)
                target = np.nan
                stop = np.nan
                current_position = 0

    trades_frame = pd.DataFrame.from_items([('direction', direction_list),
                                            ('entry_price', entry_price_list),
                                            ('exit_price', exit_price_list),
                                            ('entry_index', entry_index_list),
                                            ('exit_index', exit_index_list)])

    trades_frame['pnl'] = (trades_frame['exit_price'] - trades_frame['entry_price']) * trades_frame['direction']
    trades_frame['pnl_dollar'] = cmi.contract_multiplier[ticker_head] * trades_frame['pnl']

    return {'trades_frame': trades_frame, 'daily_frame': daily_frame}
