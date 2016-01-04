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
import os.path
import ta.strategy as ts
import signals.futures_filters as sf

def backtest_futures_butterfly_4date(**kwargs):

    report_date = kwargs['report_date']
    futures_data_dictionary = kwargs['futures_data_dictionary']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results.pkl') and use_existing_filesQ:
        return pd.read_pickle(output_dir + '/backtest_results.pkl')

    report_date_datetime = cu.convert_doubledate_2datetime(report_date)

    butt_out = opfb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = butt_out['butterflies']

    num_butterflies = len(butterflies.index)
    butterfly_pnl_long5 = [0]*num_butterflies
    butterfly_pnl_short5 = [0]*num_butterflies

    butterfly_pnl_long2 = [0]*num_butterflies
    butterfly_pnl_short2 = [0]*num_butterflies

    butterfly_pnl_long1 = [0]*num_butterflies
    butterfly_pnl_short1 = [0]*num_butterflies

    for i in range(num_butterflies):
        ticker_head_current = butterflies['tickerHead'][i]
        futures_data = futures_data_dictionary[ticker_head_current]
        data4_date = futures_data[futures_data['settle_date'] == report_date_datetime]

        ticker1_pnl1 = data4_date['change1'][data4_date['ticker'] == butterflies['ticker1'][i]]
        ticker2_pnl1 = data4_date['change1'][data4_date['ticker'] == butterflies['ticker2'][i]]
        ticker3_pnl1 = data4_date['change1'][data4_date['ticker'] == butterflies['ticker3'][i]]

        ticker1_pnl2 = data4_date['change2'][data4_date['ticker'] == butterflies['ticker1'][i]]
        ticker2_pnl2 = data4_date['change2'][data4_date['ticker'] == butterflies['ticker2'][i]]
        ticker3_pnl2 = data4_date['change2'][data4_date['ticker'] == butterflies['ticker3'][i]]

        ticker1_pnl5 = data4_date['change5'][data4_date['ticker'] == butterflies['ticker1'][i]]
        ticker2_pnl5 = data4_date['change5'][data4_date['ticker'] == butterflies['ticker2'][i]]
        ticker3_pnl5 = data4_date['change5'][data4_date['ticker'] == butterflies['ticker3'][i]]

        quantity_long = round(10000/abs(butterflies['downside'][i]))
        quantity_short = -round(10000/abs(butterflies['upside'][i]))

        bf_unit_pnl5 = (ticker1_pnl5.values[0]-(1+butterflies['second_spread_weight_1'][i])*ticker2_pnl5.values[0]
                                       +butterflies['second_spread_weight_1'][i]*ticker3_pnl5.values[0])*butterflies['multiplier'][i]

        bf_unit_pnl2 = (ticker1_pnl2.values[0]-(1+butterflies['second_spread_weight_1'][i])*ticker2_pnl2.values[0]
                                       +butterflies['second_spread_weight_1'][i]*ticker3_pnl2.values[0])*butterflies['multiplier'][i]

        bf_unit_pnl1 = (ticker1_pnl1.values[0]-(1+butterflies['second_spread_weight_1'][i])*ticker2_pnl1.values[0]
                                       +butterflies['second_spread_weight_1'][i]*ticker3_pnl1.values[0])*butterflies['multiplier'][i]

        butterfly_pnl_long5[i] = quantity_long*bf_unit_pnl5
        butterfly_pnl_short5[i] = quantity_short*bf_unit_pnl5

        butterfly_pnl_long2[i] = quantity_long*bf_unit_pnl2
        butterfly_pnl_short2[i] = quantity_short*bf_unit_pnl2

        butterfly_pnl_long1[i] = quantity_long*bf_unit_pnl1
        butterfly_pnl_short1[i] = quantity_short*bf_unit_pnl1

    butterflies['pnl_long5'] = butterfly_pnl_long5
    butterflies['pnl_short5'] = butterfly_pnl_short5

    butterflies['pnl_long2'] = butterfly_pnl_long2
    butterflies['pnl_short2'] = butterfly_pnl_short2

    butterflies['pnl_long1'] = butterfly_pnl_long1
    butterflies['pnl_short1'] = butterfly_pnl_short1

    butterflies['report_date'] = report_date

    butterflies['pnl_final'] = butterflies['pnl_long5']

    butterflies.loc[butterflies['Q'] >= 55, 'pnl_final'] = butterflies.loc[butterflies['Q'] >= 55, 'pnl_short5']

    butterflies.to_pickle(output_dir + '/backtest_results.pkl')

    return butterflies

def backtest_futures_butterfly(**kwargs):

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.futures_butterfly_strategy_tickerhead_list}
    date_list = kwargs['date_list']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    backtest_output = []

    for report_date in date_list:
        backtest_output.append(backtest_futures_butterfly_4date(report_date=report_date,
                                                                futures_data_dictionary=futures_data_dictionary,
                                                                use_existing_filesQ=use_existing_filesQ))

    return {'big_data' : pd.concat(backtest_output), 'backtest_output': backtest_output}


def construct_futures_butterfly_portfolio(**kwargs):

    rule_no = kwargs['rule_no']

    if rule_no == 1:
        stop_loss = -100000000000
    elif rule_no == 2:
        stop_loss = -1000

    if os.path.isfile(dn.backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '.pkl'):
        return pd.read_pickle(dn.backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '.pkl')

    date_list = kwargs['date_list']
    bfb_output = backtest_futures_butterfly(date_list=date_list)
    backtest_output = bfb_output['backtest_output']

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
                                             (np.isfinite(ticker_head_results['pnl_final']))]

            selected_long_trades = ticker_head_results[filter_output_long['selection_indx'] &
                                           (np.isfinite(ticker_head_results['pnl_final']))]

            if len(selected_short_trades.index) > 0:
                selected_short_trades.loc[selected_short_trades['pnl_short1'] < stop_loss, 'pnl_final']= \
                    selected_short_trades.loc[selected_short_trades['pnl_short1'] < stop_loss, 'pnl_short2']

                pnl_tickerhead_frame['sell_mean_pnl'][j] = selected_short_trades['pnl_final'].mean()

            if len(selected_long_trades.index) > 0:

                selected_long_trades.loc[selected_long_trades['pnl_long1'] <stop_loss, 'pnl_final'] = \
                    selected_long_trades.loc[selected_long_trades['pnl_long1'] <stop_loss, 'pnl_long2']

                pnl_tickerhead_frame['buy_mean_pnl'][j] = selected_long_trades['pnl_final'].mean()

            pnl_tickerhead_frame['total_pnl'][j] = pnl_tickerhead_frame['buy_mean_pnl'][j] + pnl_tickerhead_frame['sell_mean_pnl'][j]
            total_pnl_frame[ticker_head_list[j]][i] = pnl_tickerhead_frame['total_pnl'][j]

        total_pnl_frame['portfolio'][i] = pnl_tickerhead_frame['total_pnl'].sum()
    total_pnl_frame.to_pickle(dn.backtest_results_folder + '/futures_butterfly/portfolio' + str(rule_no) + '.pkl')

    return total_pnl_frame


def analyze_portfolio_contributors(**kwargs):
    date_list = kwargs['date_list']
    date_from = kwargs['date_from']
    date_to = kwargs['date_to']
    rule_no = kwargs['rule_no']

    ticker_head_list = cmi.futures_butterfly_strategy_tickerhead_list

    total_pnl_frame = construct_futures_butterfly_portfolio(date_list=date_list, rule_no=rule_no)
    frame_selected = total_pnl_frame[(total_pnl_frame['report_date']>=date_from) & (total_pnl_frame['report_date']<=date_to)]

    ticker_head_pnls = frame_selected[ticker_head_list].sum()

    ticker_head_total_pnls = pd.DataFrame({'ticker_head' : ticker_head_pnls.keys(),'pnl': ticker_head_pnls.values ,'abs_pnl' : np.absolute(ticker_head_pnls)})

    return {'total_pnl_frame': frame_selected, 'ticker_head_total_pnls': ticker_head_total_pnls.sort('abs_pnl', ascending=False)}



