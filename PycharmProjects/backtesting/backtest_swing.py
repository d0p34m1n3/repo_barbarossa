
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None


def get_results_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']

    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head)

    data_out = data_out[['settle_date','ticker','tr_dte','close_price','open_price']]
    settle_date_list = data_out['settle_date'].unique()

    current_position = 0

    position_list = []
    entry_price_list = []
    entry_index_list = []
    entry_ticker_list = []
    exit_index_list = []
    exit_price_list = []
    pnl_path = []
    buy_hold_pnl_path = []

    for i in range(20, len(settle_date_list)):

        date_4_date = data_out[data_out['settle_date']==settle_date_list[i]]
        liquid_data = date_4_date[date_4_date['tr_dte']>15]
        if len(liquid_data.index) > 0:
            liquid_ticker = liquid_data['ticker'].iloc[0]
            ticker_past_data = data_out[(data_out['ticker']==liquid_ticker)&(data_out['settle_date']<=settle_date_list[i])]

            if len(ticker_past_data.index)>=2:
                buy_hold_pnl_path.append(ticker_past_data['close_price'].iloc[-1]-ticker_past_data['close_price'].iloc[-2])
            else:
                buy_hold_pnl_path.append(np.nan)
        else:
            buy_hold_pnl_path.append(np.nan)

        if current_position == 0:

            pnl_path.append(0)

            if len(liquid_data.index)>0:
                liquid_ticker = liquid_data['ticker'].iloc[0]
                ticker_past_data = data_out[(data_out['ticker']==liquid_ticker)&(data_out['settle_date']<=settle_date_list[i])]

                ticker_past_data['ma10'] = pd.rolling_mean(ticker_past_data['close_price'], 11)

                ticker_past_data['signal'] = (ticker_past_data['close_price']-ticker_past_data['ma10'])/ticker_past_data['ma10']
                ticker_past_data['signal_min'] = pd.rolling_min(ticker_past_data['signal'],11)

                if ticker_past_data['signal_min'].iloc[-1]==ticker_past_data['signal'].iloc[-1]:
                    current_position = 1
                    position_list.append(1)
                    entry_index_list.append(i)
                    entry_ticker_list.append(liquid_ticker)
                    ticker_forward_data = data_out[(data_out['ticker']==liquid_ticker)&(data_out['settle_date']>settle_date_list[i])]
                    entry_price_list.append(ticker_forward_data['open_price'].iloc[0])
                    daily_pnl = ticker_forward_data['close_price'].iloc[0]-ticker_forward_data['open_price'].iloc[0]
                    pnl_path.append(daily_pnl)

        elif (current_position==1) and (i-entry_index_list[-1]>1):

            ticker_current_data = data_out[(data_out['ticker']==entry_ticker_list[-1])&(data_out['settle_date']<=settle_date_list[i])]
            pnl_path.append(ticker_current_data['close_price'].iloc[-1]-ticker_current_data['close_price'].iloc[-2])

            if ticker_current_data['close_price'].iloc[-1]>entry_price_list[-1]:
                exit_index_list.append(i)
                exit_price_list.append(ticker_current_data['close_price'].iloc[-1])
                current_position = 0
            elif i-entry_index_list[-1]>10:
                exit_index_list.append(i)
                exit_price_list.append(ticker_current_data['close_price'].iloc[-1])
                current_position = 0



    trades_frame = pd.DataFrame.from_items([('entry_price', entry_price_list), ('exit_price', exit_price_list)])
    trades_frame['pnl'] = trades_frame['exit_price']-trades_frame['entry_price']

    daily_frame = pd.DataFrame.from_items([('pnl', pnl_path), ('buy_hold', buy_hold_pnl_path)])

