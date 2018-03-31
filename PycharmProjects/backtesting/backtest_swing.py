
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None


def get_results_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']

    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head)

    data_out = data_out[['settle_date','ticker','tr_dte','close_price','open_price', 'high_price', 'low_price']]

    ticker_list = data_out['ticker'].unique()
    ticker_data_list = []

    for i in range(len(ticker_list)):
        ticker_data = data_out[data_out['ticker'] == ticker_list[i]]
        ticker_data['ema20'] = pd.ewma(ticker_data['close_price'], span=20)
        ticker_data['ema50'] = pd.ewma(ticker_data['close_price'], span=50)
        ticker_data['ma60'] = pd.rolling_mean(ticker_data['close_price'], window=60)
        ticker_data['close_price_diff'] = ticker_data['close_price'].diff()
        ticker_data['noise'] = pd.rolling_std(ticker_data['close_price_diff'], window=40)

        ticker_data_list.append(ticker_data)

    data_out = pd.concat(ticker_data_list)

    settle_date_list = data_out['settle_date'].unique()

    current_position = 0
    stop_price = np.nan
    target_price = np.nan

    position_list = []
    entry_price_list = []
    entry_index_list = []
    entry_date_list = []
    entry_ticker_list = []
    exit_index_list = []
    exit_price_list = []
    pnl_path = []
    buy_hold_pnl_path = []
    ema_20_list = []
    ema_50_list  = []
    ma_60_list = []
    close_price_list = []
    noise_list = []

    for i in range(100, len(settle_date_list)):

        date_4_date = data_out[data_out['settle_date'] == settle_date_list[i]]
        liquid_data = date_4_date[date_4_date['tr_dte']>15]
        if len(liquid_data.index) > 0:
            liquid_ticker = liquid_data['ticker'].iloc[0]
            ticker_past_data = data_out[(data_out['ticker']==liquid_ticker)&(data_out['settle_date']<=settle_date_list[i])]
            ticker_forward_data = data_out[(data_out['ticker']==liquid_ticker)&(data_out['settle_date']>settle_date_list[i])]
            ema_20_list.append(ticker_past_data['ema20'].iloc[-2])
            ema_50_list.append(ticker_past_data['ema50'].iloc[-2])
            ma_60_list.append(ticker_past_data['ma60'].iloc[-2])
            buy_hold_pnl_path.append(ticker_past_data['close_price_diff'].iloc[-1])

        else:
            buy_hold_pnl_path.append(0)
            ema_20_list.append(np.nan)
            ema_50_list.append(np.nan)
            ma_60_list.append(np.nan)

        if current_position == 0:

            daily_pnl = 0

            if len(liquid_data.index) > 0:
                # (ticker_past_data['high_price'].iloc[-2]-ticker_past_data['low_price'].iloc[-2]<(ticker_past_data['noise'].iloc[-2])/2) and \
                #if (ticker_past_data['price_ma11min'].iloc[-1] == ticker_past_data['price_ma11'].iloc[-1]) and (ticker_past_data['ema12'].iloc[-1]>ticker_past_data['ema26'].iloc[-1]):
                if (ticker_past_data['ema20'].iloc[-2]>ticker_past_data['ema50'].iloc[-2]) and (ticker_past_data['ema50'].iloc[-2]>ticker_past_data['ma60'].iloc[-2]) and \
                   (ticker_past_data['high_price'].iloc[-1]>ticker_past_data['high_price'].iloc[-2]):
                    current_position = 1
                    position_list.append(1)
                    entry_index_list.append(i)
                    entry_ticker_list.append(liquid_ticker)
                    entry_date_list.append(settle_date_list[i])
                    stop_price = ticker_past_data['low_price'].iloc[-2]
                    target_price = 2*(ticker_past_data['high_price'].iloc[-2]-ticker_past_data['low_price'].iloc[-2])+ticker_past_data['high_price'].iloc[-2]
                    entry_price_list.append(ticker_past_data['high_price'].iloc[-2])
                    daily_pnl = ticker_past_data['close_price'].iloc[-1]-ticker_past_data['high_price'].iloc[-2]

            pnl_path.append(daily_pnl)

        elif current_position == 1:

            ticker_current_data = data_out[(data_out['ticker']==entry_ticker_list[-1])&(data_out['settle_date']<=settle_date_list[i])]
            pnl_path.append(ticker_current_data['close_price'].iloc[-1]-ticker_current_data['close_price'].iloc[-2])

            if ticker_current_data['high_price'].iloc[-1]>target_price:
                exit_index_list.append(i)
                exit_price_list.append(target_price)
                current_position = 0
                stop_price = np.nan
                target_price = np.nan
            elif ticker_current_data['low_price'].iloc[-1]<stop_price:
                exit_index_list.append(i)
                exit_price_list.append(stop_price)
                current_position = 0
                stop_price = np.nan
                target_price = np.nan
            elif ticker_current_data['tr_dte'].iloc[-1]<=5:
                exit_index_list.append(i)
                exit_price_list.append(ticker_current_data['close_price'].iloc[-1])
                current_position = 0
                stop_price = np.nan
                target_price = np.nan

    if current_position != 0:
        exit_index_list.append(len(settle_date_list)-1)
        ticker_current_data = data_out[(data_out['ticker']==entry_ticker_list[-1])&(data_out['settle_date']==settle_date_list[-1])]
        if len(ticker_current_data.index)>=1:
            exit_price_list.append(ticker_current_data['close_price'].iloc[-1])
        else:
            exit_price_list.append(np.nan)

    trades_frame = pd.DataFrame.from_items([('ticker',entry_ticker_list),('entry_date',entry_date_list),('entry_price', entry_price_list), ('exit_price', exit_price_list),
                                            ('entry_index', entry_index_list), ('exit_index', exit_index_list)])
    trades_frame['pnl'] = trades_frame['exit_price']-trades_frame['entry_price']

    daily_frame = pd.DataFrame.from_items([('pnl', pnl_path), ('buy_hold', buy_hold_pnl_path)])

    return {'daily_frame': daily_frame, 'trades_frame': trades_frame}

