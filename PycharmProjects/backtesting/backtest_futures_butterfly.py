__author__ = 'kocat_000'


import pandas as pd
import shared.directory_names as dn
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import opportunity_constructs.futures_butterfly as opfb
pd.options.mode.chained_assignment = None
import pickle as pick
import numpy as np
import os
import ta.strategy as ts
import signals.futures_filters as sf


def construct_futures_butterfly_portfolio(**kwargs):

    rule_no = kwargs['rule_no']
    backtest_output = kwargs['backtest_output']
    pnl_field = kwargs['pnl_field']

    if rule_no in [1, 3, 4, 5, 6, 7, 8, 9]:
        stop_loss = -100000000000
    elif rule_no == 2:
        stop_loss = -1000

    backtest_results_folder = dn.get_directory_name(ext='backtest_results')

    if os.path.isfile(backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '.pkl'):
        return pd.read_pickle(backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '.pkl')
    elif not os.path.exists(backtest_results_folder + '/futures_butterfly'):
        os.makedirs(backtest_results_folder + '/futures_butterfly')

    date_list = kwargs['date_list']
    ticker_head_list = cmi.futures_butterfly_strategy_tickerhead_list

    total_pnl_frame = pd.DataFrame({'report_date': date_list})
    total_pnl_frame['portfolio'] = 0

    for i in range(len(ticker_head_list)):
        total_pnl_frame[ticker_head_list[i]] = 0

    for i in range(len(date_list)):
        pnl_tickerhead_frame = pd.DataFrame({'ticker_head': ticker_head_list})
        pnl_tickerhead_frame['buy_mean_pnl'] = 0
        pnl_tickerhead_frame['sell_mean_pnl'] = 0
        pnl_tickerhead_frame['total_pnl'] = 0
        daily_sheet = backtest_output[i]

        for j in range(len(ticker_head_list)):

            ticker_head_results = daily_sheet[daily_sheet['tickerHead'] == ticker_head_list[j]]

            filter_output_long = sf.get_futures_butterfly_filters(data_frame_input=ticker_head_results, filter_list=['long'+str(rule_no)])
            filter_output_short = sf.get_futures_butterfly_filters(data_frame_input=ticker_head_results, filter_list=['short'+str(rule_no)])

            selected_short_trades = ticker_head_results[filter_output_short['selection_indx'] &
                                             (np.isfinite(ticker_head_results[pnl_field]))]

            selected_long_trades = ticker_head_results[filter_output_long['selection_indx'] &
                                           (np.isfinite(ticker_head_results[pnl_field]))]

            if len(selected_short_trades.index) > 0:
                selected_short_trades.loc[selected_short_trades['hold_pnl1short'] < stop_loss, pnl_field]= \
                    selected_short_trades.loc[selected_short_trades['hold_pnl1short'] < stop_loss, 'hold_pnl2short']

                pnl_tickerhead_frame['sell_mean_pnl'][j] = selected_short_trades[pnl_field].mean()

            if len(selected_long_trades.index) > 0:

                selected_long_trades.loc[selected_long_trades['hold_pnl1long'] <stop_loss, pnl_field] = \
                    selected_long_trades.loc[selected_long_trades['hold_pnl1long'] <stop_loss, 'hold_pnl2long']

                pnl_tickerhead_frame['buy_mean_pnl'][j] = selected_long_trades[pnl_field].mean()

            pnl_tickerhead_frame['total_pnl'][j] = pnl_tickerhead_frame['buy_mean_pnl'][j] + pnl_tickerhead_frame['sell_mean_pnl'][j]
            total_pnl_frame[ticker_head_list[j]][i] = pnl_tickerhead_frame['total_pnl'][j]

        total_pnl_frame['portfolio'][i] = pnl_tickerhead_frame['total_pnl'].sum()
    total_pnl_frame.to_pickle(backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '.pkl')

    return total_pnl_frame


def analyze_portfolio_contributors(**kwargs):
    date_list = kwargs['date_list']
    backtest_output = kwargs['backtest_output']
    date_from = kwargs['date_from']
    date_to = kwargs['date_to']
    rule_no = kwargs['rule_no']

    ticker_head_list = cmi.futures_butterfly_strategy_tickerhead_list

    total_pnl_frame = construct_futures_butterfly_portfolio(date_list=date_list, rule_no=rule_no,
                                                            backtest_output=backtest_output,
                                                            pnl_field='hold_pnl5')
    frame_selected = total_pnl_frame[(total_pnl_frame['report_date']>=date_from) & (total_pnl_frame['report_date']<=date_to)]

    ticker_head_pnls = frame_selected[ticker_head_list].sum()

    ticker_head_total_pnls = pd.DataFrame({'ticker_head' : ticker_head_pnls.keys(),'pnl': ticker_head_pnls.values ,'abs_pnl' : np.absolute(ticker_head_pnls)})

    return {'total_pnl_frame': frame_selected, 'ticker_head_total_pnls': ticker_head_total_pnls.sort('abs_pnl', ascending=False)}


def save_trade_paths(**kwargs):

    risk_per_trade = 10000
    report_date = kwargs['report_date']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results_ws.pkl') and use_existing_filesQ:
        return pd.read_pickle(output_dir + '/backtest_results_ws.pkl')

    butt_out = opfb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    strategy_sheet = butt_out['butterflies']

    strategy_sheet['long_pnl_ws1'] = np.nan
    strategy_sheet['long_pnl_ws2'] = np.nan
    strategy_sheet['long_pnl_ws3'] = np.nan

    strategy_sheet['short_pnl_ws1'] = np.nan
    strategy_sheet['short_pnl_ws2'] = np.nan
    strategy_sheet['short_pnl_ws3'] = np.nan

    strategy_sheet['long_stop_days1'] = np.nan
    strategy_sheet['long_stop_days2'] = np.nan
    strategy_sheet['long_stop_days3'] = np.nan

    strategy_sheet['short_stop_days1'] = np.nan
    strategy_sheet['short_stop_days2'] = np.nan
    strategy_sheet['short_stop_days3'] = np.nan

    strategy_sheet['long_pnl20'] = np.nan
    strategy_sheet['long_pnl40'] = np.nan
    strategy_sheet['long_pnl60'] = np.nan

    strategy_sheet['short_pnl20'] = np.nan
    strategy_sheet['short_pnl40'] = np.nan
    strategy_sheet['short_pnl60'] = np.nan
    strategy_sheet['report_date'] = report_date

    for i in range(len(strategy_sheet.index)):
        selected_trade = strategy_sheet.iloc[i]
        quantity_long = round(risk_per_trade/abs(selected_trade['downside']))
        quantity_short = -round(risk_per_trade/abs(selected_trade['upside']))
        data_list = []
        for j in range(3):
            ticker_frame = gfp.get_futures_price_preloaded(ticker=selected_trade['ticker'+str(j+1)],settle_date_from=report_date)
            ticker_frame.set_index('settle_date', drop=False, inplace=True)
            data_list.append(ticker_frame)
        merged_data = pd.concat(data_list, axis=1, join='inner')
        merged_data = merged_data[merged_data['tr_dte'].iloc[:, 0] >= 15]
        merged_data = merged_data[:71]

        spread_price = (merged_data['close_price'].iloc[:,0]-merged_data['close_price'].iloc[:,1])-selected_trade['second_spread_weight_1']*(merged_data['close_price'].iloc[:,1]-merged_data['close_price'].iloc[:,2])

        long_entry_slippage = quantity_long*selected_trade['multiplier']*(spread_price[1]-spread_price[0])
        short_entry_slippage = quantity_long*selected_trade['multiplier']*(spread_price[1]-spread_price[0])

        if (abs(long_entry_slippage)>risk_per_trade/2) or (abs(short_entry_slippage)>risk_per_trade/2):
            continue

        long_path = quantity_long*selected_trade['multiplier']*(spread_price[2:]-spread_price[1])
        short_path = quantity_short*selected_trade['multiplier']*(spread_price[2:]-spread_price[1])

        if len(spread_price.index) < 10:
            continue

        long_path.reset_index(drop=True,inplace=True)
        short_path.reset_index(drop=True,inplace=True)

        long_stop1 = long_path[(long_path<-risk_per_trade)|(long_path>(risk_per_trade/3))]
        long_stop2 = long_path[(long_path<-2*risk_per_trade)|(long_path>(risk_per_trade/3))]
        long_stop3 = long_path[(long_path<-3*risk_per_trade)|(long_path>(risk_per_trade/3))]

        short_stop1 = short_path[(short_path<-risk_per_trade)|(short_path>(risk_per_trade/3))]
        short_stop2 = short_path[(short_path<-2*risk_per_trade)|(short_path>(risk_per_trade/3))]
        short_stop3 = short_path[(short_path<-3*risk_per_trade)|(short_path>(risk_per_trade/3))]

        long_output1 = get_spike_robust_stop_results(long_stop1)
        long_output2 = get_spike_robust_stop_results(long_stop2)
        long_output3 = get_spike_robust_stop_results(long_stop3)
        short_output1 = get_spike_robust_stop_results(short_stop1)
        short_output2 = get_spike_robust_stop_results(short_stop2)
        short_output3 = get_spike_robust_stop_results(short_stop3)

        strategy_sheet['long_stop_days1'].iloc[i] = long_output1['holding_period']
        strategy_sheet['long_pnl_ws1'].iloc[i] = long_output1['pnl']

        strategy_sheet['long_stop_days2'].iloc[i] = long_output2['holding_period']
        strategy_sheet['long_pnl_ws2'].iloc[i] = long_output2['pnl']

        strategy_sheet['long_stop_days3'].iloc[i] = long_output3['holding_period']
        strategy_sheet['long_pnl_ws3'].iloc[i] = long_output3['pnl']

        strategy_sheet['short_stop_days1'].iloc[i] = short_output1['holding_period']
        strategy_sheet['short_pnl_ws1'].iloc[i] = short_output1['pnl']

        strategy_sheet['short_stop_days2'].iloc[i] = short_output2['holding_period']
        strategy_sheet['short_pnl_ws2'].iloc[i] = short_output2['pnl']

        strategy_sheet['short_stop_days3'].iloc[i] = short_output3['holding_period']
        strategy_sheet['short_pnl_ws3'].iloc[i] = short_output3['pnl']

        if len(spread_price.index) > 21:
            strategy_sheet['long_pnl20'].iloc[i] = long_path[19]
            strategy_sheet['short_pnl20'].iloc[i] = short_path[19]

        if len(spread_price.index) > 41:
            strategy_sheet['long_pnl40'].iloc[i] = long_path[39]
            strategy_sheet['short_pnl40'].iloc[i] = short_path[39]

        if len(spread_price.index) > 61:
            strategy_sheet['long_pnl60'].iloc[i] = long_path[59]
            strategy_sheet['short_pnl60'].iloc[i] = short_path[59]

    strategy_sheet.to_pickle(output_dir + '/backtest_results_ws.pkl')

    return strategy_sheet


def get_spike_robust_stop_results(raw_path):

    holding_period = np.nan
    pnl = np.nan

    if len(raw_path.index) > 3:

        valid_index = [False]*len(raw_path.index)

        for i in range(3,len(raw_path)):
            if ((raw_path.index[i-3]+1 == raw_path.index[i-2])&
                    (raw_path.index[i-2]+1 == raw_path.index[i-1])&
                    (raw_path.index[i-1]+1 == raw_path.index[i])&
                    (np.sign(raw_path.iloc[i-3]) == np.sign(raw_path.iloc[i-2]))&
                    (np.sign(raw_path.iloc[i-2]) == np.sign(raw_path.iloc[i-1]))&
                    (np.sign(raw_path.iloc[i-1]) == np.sign(raw_path.iloc[i]))):

                valid_index[i] = True

        smooth_path = raw_path[valid_index]

        if (not smooth_path.empty) and len(smooth_path.index) > 1:
            holding_period = smooth_path.index[1]
            pnl = smooth_path.iloc[1]

    return {'holding_period': holding_period, 'pnl': pnl}

















