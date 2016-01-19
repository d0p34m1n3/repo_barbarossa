__author__ = 'kocat_000'

import opportunity_constructs.spread_carry as sc
import get_price.get_futures_price as gfp
import pandas as pd
import numpy as np

def backtest_spread_carry(**kwargs):

    date_list = kwargs['date_list']
    risk = 1000

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in sc.max_tr_dte_limits.keys()}

    ticker_head_list = list(sc.max_tr_dte_limits.keys())

    total_pnl_frame = pd.DataFrame({'report_date': date_list})
    total_pnl_frame['portfolio'] = 0
    backtest_output = []

    for i in range(len(ticker_head_list)):
        total_pnl_frame[ticker_head_list[i]] = 0

    for i in range(len(date_list)):
        spread_carry_output = sc.generate_spread_carry_sheet_4date(report_date=date_list[i],futures_data_dictionary=futures_data_dictionary)

        if spread_carry_output['success']:
            daily_sheet = spread_carry_output['spread_report']
        else:
            continue

        backtest_output.append(daily_sheet)

        daily_sheet['q_carry_abs'] = abs(daily_sheet['q_carry'])
        pnl_tickerhead_frame = pd.DataFrame({'ticker_head': ticker_head_list})
        pnl_tickerhead_frame['total_pnl'] = 0

        for j in range(len(ticker_head_list)):
            ticker_head_results = daily_sheet[daily_sheet['ticker_head'] == ticker_head_list[j]]

            if len(ticker_head_results.index)<=1:
                continue

            max_q_carry_abs = ticker_head_results['q_carry_abs'].max()

            if np.isnan(max_q_carry_abs):
                continue

            selected_spread = ticker_head_results.ix[ticker_head_results['q_carry_abs'].idxmax()]

            if selected_spread['q_carry']>0:
                total_pnl_frame[ticker_head_list[j]][i] = selected_spread['change5']*risk/abs(selected_spread['downside'])
                pnl_tickerhead_frame['total_pnl'][j] = total_pnl_frame[ticker_head_list[j]][i]
            elif selected_spread['q_carry']<0:
                total_pnl_frame[ticker_head_list[j]][i] = -selected_spread['change5']*risk/abs(selected_spread['upside'])
                pnl_tickerhead_frame['total_pnl'][j] = total_pnl_frame[ticker_head_list[j]][i]

        total_pnl_frame['portfolio'][i] = pnl_tickerhead_frame['total_pnl'].sum()

    big_data = pd.concat(backtest_output)
    big_data['pnl_long'] = big_data['change5']*risk/abs(big_data['downside'])
    big_data['pnl_short'] = -big_data['change5']*risk/abs(big_data['upside'])
    big_data['pnl_final'] = big_data['pnl_long']
    big_data.loc[big_data['q_carry'] < 0, 'pnl_final'] = big_data.loc[big_data['q_carry'] <0, 'pnl_short']

    return {'total_pnl_frame': backtest_output, 'big_data': big_data}
