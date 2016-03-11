__author__ = 'kocat_000'

import pandas as pd
import numpy as np
import opportunity_constructs.curve_pca as cpc
import contract_utilities.contract_meta_info as cmi
import backtesting.utilities as bu
import shared.directory_names as dn
pd.options.mode.chained_assignment = None  # default='warn'
import os.path
import pickle as pickle


def backtest_curve_pca(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']
    years_back = kwargs['years_back']
    indicator = kwargs['indicator']

    backtest_output_dir = dn.get_directory_name(ext='backtest_results')

    file_name = ticker_head + '_' + str(date_to) + '_' + str(years_back) + '_' + indicator

    if os.path.isfile(backtest_output_dir + '/curve_pca/' + file_name + '.pkl'):
        backtest_results = pd.read_pickle(backtest_output_dir + '/curve_pca/' + file_name + '.pkl')
        return backtest_results

    dates_output = bu.get_backtesting_dates(date_to=date_to, years_back=years_back)
    date_list = sorted(dates_output['double_dates'], reverse=False)

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    report_results_list = []
    success_indx = []

    contract_multiplier = cmi.contract_multiplier[ticker_head]

    for date_to in date_list:

        report_out = cpc.get_curve_pca_report(ticker_head=ticker_head, date_to=date_to, use_existing_filesQ=use_existing_filesQ)
        success_indx.append(report_out['success'])

        if report_out['success']:

            pca_results = report_out['pca_results']
            pca_results['report_date'] = date_to
            report_results_list.append(pca_results)


    good_dates = [date_list[i] for i in range(len(date_list)) if success_indx[i]]

    total_pnl_list = []
    z_score_list = []
    residual_list = []
    num_contract_list = []
    short_side_weight_list = []

    for i in range(len(good_dates)):
        daily_report = report_results_list[i]

        #daily_report = daily_report[daily_report['monthSpread']==1]
        #daily_report = daily_report[3:]

        daily_report_filtered = daily_report[(daily_report['tr_dte_front']>80) & (daily_report['monthSpread']==1)]

        #median_factor_load2 = daily_report['factor_load2'].median()

        #if median_factor_load2>0:
        #    daily_report_filtered = daily_report[daily_report['factor_load2']>=0]
        #else:
        #    daily_report_filtered = daily_report[daily_report['factor_load2']<=0]

        daily_report_filtered.sort(indicator,ascending=True,inplace=True)
        num_contract_4side = round(len(daily_report_filtered.index)/4)
        long_side = daily_report_filtered.iloc[:num_contract_4side]
        short_side = daily_report_filtered.iloc[-num_contract_4side:]
        short_side_weight = long_side['factor_load1'].sum()/short_side['factor_load1'].sum()

        z_score_list.append(np.nanmean(short_side[indicator])-np.nanmean(long_side[indicator]))

        if any(np.isnan(long_side['change5'])) or any(np.isnan(short_side['change5'])):
            total_pnl_list.append(np.NAN)
        else:
            total_pnl_list.append(np.nanmean(long_side['change5'])-short_side_weight*np.nanmean(short_side['change5']))

        residual_list.append(np.nanmean(short_side['residuals'])-np.nanmean(long_side['residuals']))
        num_contract_list.append(num_contract_4side)
        short_side_weight_list.append(short_side_weight)

    backtest_results =  {'pnl_frame': pd.DataFrame.from_items([('settle_date',good_dates),
                                                   ('num_contracts',num_contract_list),
                                                   ('z',z_score_list),
                                                   ('residual',residual_list),
                                                  ('short_side_weight',short_side_weight_list),
                                                   ('pnl',[x*contract_multiplier for x in total_pnl_list])]),
                         'report_results_list': report_results_list}

    with open(backtest_output_dir + '/curve_pca/' + file_name + '.pkl', 'wb') as handle:
        pickle.dump(backtest_results, handle)

    return backtest_results




