
import opportunity_constructs.intraday_future_spreads as ifs
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
import signals.utils as sutil
import signals.ifs as sifs
import signals.intraday_futures_signals as sigifs
import datetime as dt
import numpy as np
import pandas as pd
import ta.strategy as ts
import os.path


def backtest_ifs(**kwargs):

    intraday_spreads = kwargs['intraday_spreads']
    date_list = kwargs['date_list']
    trade_id = kwargs['trade_id']

    pnl1 = 0
    pnl1wc = 0
    pnl2 = 0
    pnl2wc = 0
    pnl5 = 0
    pnl5wc = 0
    pnl6 = 0
    pnl6wc = 0

    start_hour = dt.time(9, 0, 0, 0)
    end_hour = dt.time(12, 55, 0, 0)

    ticker_list = [intraday_spreads.iloc[trade_id]['contract1'],intraday_spreads.iloc[trade_id]['contract2'],intraday_spreads.iloc[trade_id]['contract3']]
    ticker_list = [x for x in ticker_list if x is not None]

    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    num_contracts = len(ticker_list)
    weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]
    spread_weights = weights_output['spread_weights']

    backtest_output = {'pnl1': pnl1, 'pnl1wc': pnl1wc,
                       'pnl2': pnl2, 'pnl2wc': pnl2wc,
                       'pnl5': pnl5, 'pnl5wc': pnl5wc,
                       'pnl6': pnl6, 'pnl6wc': pnl6wc}

    intraday_data = sifs.get_data4datelist(ticker_list=ticker_list,date_list=date_list)

    unique_settle_dates = intraday_data['settle_date'].unique()

    if len(unique_settle_dates)<2:
        return backtest_output

    selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    intraday_data = intraday_data.iloc[selection_indx]

    mean5 = intraday_spreads.iloc[trade_id]['mean']
    std5 = intraday_spreads.iloc[trade_id]['std']

    mean1 = intraday_spreads.iloc[trade_id]['mean1']
    std1 = intraday_spreads.iloc[trade_id]['std1']

    mean2 = intraday_spreads.iloc[trade_id]['mean2']
    std2 = intraday_spreads.iloc[trade_id]['std2']

    long_qty = -5000/intraday_spreads.iloc[trade_id]['downside']
    short_qty = -5000/intraday_spreads.iloc[trade_id]['upside']

    intraday_data['z5'] = (intraday_data['spread']-mean5)/std5
    intraday_data['z1'] = (intraday_data['spread']-mean1)/std1
    intraday_data['z2'] = (intraday_data['spread']-mean2)/std2
    intraday_data['z6'] = (intraday_data['spread']-mean1)/std5

    entry_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-2]).date()]
    exit_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-1]).date()]

    indicator = 'z1'

    opportunity_index = (entry_data[indicator]>1)|(entry_data[indicator]<-1)
    opportunity_data = entry_data[opportunity_index]

    if not opportunity_data.empty:
        entry_point = opportunity_data.iloc[0]

        exit_point = exit_data[exit_data['hour_minute'] == entry_point['hour_minute'][0]]

        if not exit_point.empty:
            if entry_point[indicator][0] > 1:
                pnl1 = short_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl1wc = pnl1-2*2*abs(short_qty)*num_contracts
            elif entry_point[indicator][0] < -1:
                pnl1 = long_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl1wc = pnl1-2*2*long_qty*num_contracts

    indicator = 'z2'
    opportunity_index = (entry_data[indicator]>1)|(entry_data[indicator]<-1)
    opportunity_data = entry_data[opportunity_index]

    if not opportunity_data.empty:
        entry_point = opportunity_data.iloc[0]
        exit_point = exit_data[exit_data['hour_minute'] == entry_point['hour_minute'][0]]
        if not exit_point.empty:
            if entry_point[indicator][0] > 1:
                pnl2 = short_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl2wc = pnl2-2*2*abs(short_qty)*num_contracts
            elif entry_point[indicator][0] < -1:
                pnl2 = long_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl2wc = pnl2-2*2*long_qty*num_contracts

    indicator = 'z5'
    opportunity_index = (entry_data[indicator]>1)|(entry_data[indicator]<-1)
    opportunity_data = entry_data[opportunity_index]

    if not opportunity_data.empty:
        entry_point = opportunity_data.iloc[0]
        exit_point = exit_data[exit_data['hour_minute'] == entry_point['hour_minute'][0]]
        if not exit_point.empty:
            if entry_point[indicator][0] > 1.5:
                pnl5 = short_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl5wc = pnl5-2*2*abs(short_qty)*num_contracts
            elif entry_point[indicator][0] < -1.5:
                pnl5 = long_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl5wc = pnl5-2*2*long_qty*num_contracts

    indicator = 'z6'
    opportunity_index = (entry_data[indicator]>0.25)|(entry_data[indicator]<-0.25)
    opportunity_data = entry_data[opportunity_index]

    if not opportunity_data.empty:
        entry_point = opportunity_data.iloc[0]
        exit_point = exit_data[exit_data['hour_minute'] == entry_point['hour_minute'][0]]
        if not exit_point.empty:
            if entry_point[indicator][0] > 0.25:
                pnl6 = short_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl6wc = pnl6-2*2*abs(short_qty)*num_contracts
            elif entry_point[indicator][0] < -0.25:
                pnl6 = long_qty*(exit_point['spread'].iloc[0]-entry_point['spread'][0])*contract_multiplier_list[0]/spread_weights[0]
                pnl6wc = pnl6-2*2*long_qty*num_contracts

    return {'pnl1': pnl1, 'pnl1wc': pnl1wc, 'pnl2': pnl2, 'pnl2wc': pnl2wc, 'pnl5': pnl5, 'pnl5wc': pnl5wc, 'pnl6': pnl6, 'pnl6wc': pnl6wc}


def backtest_continuous_ifs(**kwargs):

    intraday_spreads = kwargs['intraday_spreads']
    date_list = kwargs['date_list']
    trade_id = kwargs['trade_id']

    start_hour = dt.time(9, 0, 0, 0)
    end_hour = dt.time(12, 55, 0, 0)

    ticker_list = [intraday_spreads.iloc[trade_id]['contract1'],intraday_spreads.iloc[trade_id]['contract2'],intraday_spreads.iloc[trade_id]['contract3']]
    ticker_list = [x for x in ticker_list if x is not None]

    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    num_contracts = len(ticker_list)

    if ticker_head_list in sigifs.fixed_weight_future_spread_list:
        weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
        spread_weights = weights_output['spread_weights']
    else:
        spread_weights = [1, intraday_spreads.iloc[trade_id]['spread_weight']]

    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]

    intraday_data = sifs.get_data4datelist(ticker_list=ticker_list,date_list=date_list,spread_weights=spread_weights)

    unique_settle_dates = intraday_data['settle_date'].unique()

    if len(unique_settle_dates)<2:
        return pd.DataFrame()

    selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    intraday_data = intraday_data.iloc[selection_indx]

    mean1 = intraday_spreads.iloc[trade_id]['mean1']
    std1 = intraday_spreads.iloc[trade_id]['std1']

    mean2 = intraday_spreads.iloc[trade_id]['mean2']
    std2 = intraday_spreads.iloc[trade_id]['std2']

    mean5 = intraday_spreads.iloc[trade_id]['mean5']
    std5 = intraday_spreads.iloc[trade_id]['std5']

    mean10 = intraday_spreads.iloc[trade_id]['mean10']
    std10 = intraday_spreads.iloc[trade_id]['std10']

    long_qty = -5000/intraday_spreads.iloc[trade_id]['downside']
    short_qty = -5000/intraday_spreads.iloc[trade_id]['upside']

    intraday_data['z1'] = (intraday_data['spread']-mean1)/std1
    intraday_data['z2'] = (intraday_data['spread']-mean2)/std2
    intraday_data['z5'] = (intraday_data['spread']-mean5)/std5
    intraday_data['z10'] = (intraday_data['spread']-mean10)/std10
    intraday_data['z6'] = (intraday_data['spread']-mean1)/std5

    entry_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-2]).date()]
    exit_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-1]).date()]

    exit_morning_data = exit_data[(exit_data['hour_minute'] >= 930)&(exit_data['hour_minute'] <= 1000)]
    exit_afternoon_data = exit_data[(exit_data['hour_minute'] >= 1230)&(exit_data['hour_minute'] <= 1300)]

    entry_data['pnl_long_morning'] = long_qty*(exit_morning_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]/spread_weights[0]
    entry_data['pnl_long_morning_wc'] = entry_data['pnl_long_morning'] - 2*2*long_qty*num_contracts

    entry_data['pnl_short_morning'] = short_qty*(exit_morning_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]/spread_weights[0]
    entry_data['pnl_short_morning_wc'] = entry_data['pnl_short_morning'] - 2*2*abs(short_qty)*num_contracts

    entry_data['pnl_long_afternoon'] = long_qty*(exit_afternoon_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]/spread_weights[0]
    entry_data['pnl_long_afternoon_wc'] = entry_data['pnl_long_afternoon'] - 2*2*long_qty*num_contracts

    entry_data['pnl_short_afternoon'] = short_qty*(exit_afternoon_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]/spread_weights[0]
    entry_data['pnl_short_afternoon_wc'] = entry_data['pnl_short_afternoon'] - 2*2*abs(short_qty)*num_contracts

    entry_data['pnl_morning_wc'] = 0
    entry_data['pnl_afternoon_wc'] = 0

    long_indx = entry_data['z1'] < 0
    short_indx = entry_data['z1'] > 0

    entry_data.loc[long_indx, 'pnl_morning_wc'] = entry_data.loc[long_indx, 'pnl_long_morning_wc'].values
    entry_data.loc[short_indx > 0, 'pnl_morning_wc'] = entry_data.loc[short_indx > 0, 'pnl_short_morning_wc'].values

    entry_data.loc[long_indx < 0, 'pnl_afternoon_wc'] = entry_data.loc[long_indx < 0, 'pnl_long_afternoon_wc'].values
    entry_data.loc[short_indx > 0, 'pnl_afternoon_wc'] = entry_data.loc[short_indx > 0, 'pnl_short_afternoon_wc'].values

    return entry_data


def backtest_ifs_4date(**kwargs):

    report_date = kwargs['report_date']

    output_dir = ts.create_strategy_output_dir(strategy_class='ifs', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results.pkl'):
        intraday_spreads = pd.read_pickle(output_dir + '/backtest_results.pkl')
        return intraday_spreads

    sheet_output = ifs.generate_ifs_sheet_4date(date_to=report_date)
    intraday_spreads = sheet_output['intraday_spreads']

    intraday_spreads['pnl1'] = 0
    intraday_spreads['pnl2'] = 0
    intraday_spreads['pnl5'] = 0
    intraday_spreads['pnl6'] = 0

    intraday_spreads['pnl1_wc'] = 0
    intraday_spreads['pnl2_wc'] = 0
    intraday_spreads['pnl5_wc'] = 0
    intraday_spreads['pnl6_wc'] = 0

    intraday_spreads['report_date'] = report_date

    date_list = [exp.doubledate_shift_bus_days(double_date=report_date, shift_in_days=x) for x in [-1,-2]]

    for i in range(len(intraday_spreads.index)):

        backtest_ifs_output = backtest_ifs(intraday_spreads=intraday_spreads,date_list=date_list,trade_id=i)

        intraday_spreads['pnl1'].iloc[i] = backtest_ifs_output['pnl1']
        intraday_spreads['pnl2'].iloc[i] = backtest_ifs_output['pnl2']
        intraday_spreads['pnl5'].iloc[i] = backtest_ifs_output['pnl5']
        intraday_spreads['pnl6'].iloc[i] = backtest_ifs_output['pnl6']

        intraday_spreads['pnl1_wc'].iloc[i] = backtest_ifs_output['pnl1wc']
        intraday_spreads['pnl2_wc'].iloc[i] = backtest_ifs_output['pnl2wc']
        intraday_spreads['pnl5_wc'].iloc[i] = backtest_ifs_output['pnl5wc']
        intraday_spreads['pnl6_wc'].iloc[i] = backtest_ifs_output['pnl6wc']

    intraday_spreads.to_pickle(output_dir + '/backtest_results.pkl')
    return intraday_spreads


def backtest_continuous_ifs_4date(**kwargs):

    report_date = kwargs['report_date']

    output_dir = ts.create_strategy_output_dir(strategy_class='ifs', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results_cont.pkl'):
        backtest_results = pd.read_pickle(output_dir + '/backtest_results_cont.pkl')
        return backtest_results

    sheet_output = ifs.generate_ifs_sheet_4date(date_to=report_date)
    intraday_spreads = sheet_output['intraday_spreads']

    intraday_spreads['report_date'] = report_date
    backtest_results_list = []

    date_list = [exp.doubledate_shift_bus_days(double_date=report_date, shift_in_days=x) for x in [-1,-2]]

    for i in range(len(intraday_spreads.index)):

        backtest_resul4_4ticker = backtest_continuous_ifs(intraday_spreads=intraday_spreads,date_list=date_list,trade_id=i)

        if not backtest_resul4_4ticker.empty:
            backtest_resul4_4ticker['spread_description'] = intraday_spreads['spread_description'][i]
            backtest_resul4_4ticker['ticker'] = intraday_spreads['ticker'][i]
            backtest_results_list.append(backtest_resul4_4ticker)

    backtest_results = pd.concat(backtest_results_list)
    backtest_results['report_date'] = report_date

    backtest_results.to_pickle(output_dir + '/backtest_results_cont.pkl')
    return backtest_results



