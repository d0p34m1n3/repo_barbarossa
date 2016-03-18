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

    if rule_no in [1, 3, 4, 5, 6]:
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








