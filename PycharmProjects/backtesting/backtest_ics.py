
import contract_utilities.expiration as exp
import opportunity_constructs.intraday_calendar_spreads as ics
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import signals.intraday_machine_learner as iml
import shared.calendar_utilities as cu
import ta.strategy as ts
import datetime as dt
import os.path
import numpy as np
import pandas as pd


def backtest_ics_4date(**kwargs):

    report_date = kwargs['report_date']
    backtest_date = exp.doubledate_shift_bus_days(double_date=report_date,shift_in_days=-1)

    sheet_output = ics.generate_ics_sheet_4date(date_to=backtest_date)
    intraday_spreads = sheet_output['intraday_spreads']

    intraday_spreads['pnl'] = np.nan
    intraday_spreads['num_trades'] = np.nan
    intraday_spreads['mean_holding_period'] = np.nan

    signal_name = 'ma40_spread'

    for i in range(len(intraday_spreads.index)):

        num_contracts = 2
        contract_multiplier = cmi.contract_multiplier[intraday_spreads.iloc[i]['ticker_head']]

        intraday_data = iml.get_intraday_data(ticker=intraday_spreads.iloc[i]['ticker'],date_to=backtest_date,num_days_back=0)
        if intraday_data.empty:
            continue

        intraday_data = intraday_data[intraday_data['hour_minute'] > 930]
        pnl_list = []
        holding_period_list = []
        current_position = 0

        for j in range(len(intraday_data.index)):

            if (current_position == 0) & (intraday_data[signal_name].iloc[j]<intraday_spreads['ma_spread_low'].iloc[i]):
                current_position = 1
                entry_point = j
                entry_price = intraday_data['mid_p'].iloc[j]
            elif (current_position == 0) & (intraday_data[signal_name].iloc[j]>intraday_spreads['ma_spread_high'].iloc[i]):
                current_position = -1
                entry_point = j
                entry_price = intraday_data['mid_p'].iloc[j]
            elif (current_position == 1) & ((intraday_data[signal_name].iloc[j]>0)|(j==len(intraday_data.index)-1)):
                current_position = 0
                exit_price = intraday_data['mid_p'].iloc[j]
                pnl_list.append(contract_multiplier*(exit_price-entry_price)-2*num_contracts)
                holding_period_list.append(j-entry_point)
            elif (current_position == -1) & ((intraday_data[signal_name].iloc[j]<0)|(j==len(intraday_data.index)-1)):
                current_position = 0
                exit_price = intraday_data['mid_p'].iloc[j]
                pnl_list.append(contract_multiplier*(entry_price-exit_price)-2*num_contracts)
                holding_period_list.append(j-entry_point)

        if len(pnl_list)>0:
            intraday_spreads['pnl'].iloc[i] = sum(pnl_list)
            intraday_spreads['num_trades'].iloc[i] = len(pnl_list)
            intraday_spreads['mean_holding_period'].iloc[i] = np.mean(holding_period_list)

    return intraday_spreads


def backtest_continuous_ics(**kwargs):

    intraday_spreads = kwargs['intraday_spreads']
    trade_id = kwargs['trade_id']
    date_list = kwargs['date_list']

    ticker = intraday_spreads.iloc[trade_id]['ticker']

    num_contracts = 2

    ticker_list = ticker.split('-')
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    ticker_class = cmi.ticker_class[ticker_head_list[0]]

    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]
    t_cost = cmi.t_cost[ticker_head_list[0]]

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=[ticker],date_list=date_list)

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())
    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]

    end_hour = cmi.last_trade_hour_minute[ticker_head_list[0]]
    start_hour = cmi.first_trade_hour_minute[ticker_head_list[0]]

    if ticker_class == 'Ag':
        start_hour1 = dt.time(0, 45, 0, 0)
        end_hour1 = dt.time(7, 45, 0, 0)
        selection_indx = [x for x in range(len(intraday_data.index)) if
                          ((intraday_data['time_stamp'].iloc[x].time() < end_hour1)
                           and(intraday_data['time_stamp'].iloc[x].time() >= start_hour1)) or
                          ((intraday_data['time_stamp'].iloc[x].time() < end_hour)
                           and(intraday_data['time_stamp'].iloc[x].time() >= start_hour))]

    else:
        selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    intraday_data = intraday_data.iloc[selection_indx]

    start_hour = dt.time(9, 0, 0, 0)
    end_hour = dt.time(12, 55, 0, 0)

    selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    intraday_data = intraday_data.iloc[selection_indx]

    mean5 = intraday_spreads.iloc[trade_id]['intraday_mean5']
    std5 = intraday_spreads.iloc[trade_id]['intraday_std5']

    mean2 = intraday_spreads.iloc[trade_id]['intraday_mean2']
    std2 = intraday_spreads.iloc[trade_id]['intraday_std2']

    mean1 = intraday_spreads.iloc[trade_id]['intraday_mean1']
    std1 = intraday_spreads.iloc[trade_id]['intraday_std1']

    long_qty = -5000/intraday_spreads.iloc[trade_id]['downside']
    short_qty = -5000/intraday_spreads.iloc[trade_id]['upside']

    intraday_data['spread'] = (intraday_data['c1']['best_bid_p']+intraday_data['c1']['best_ask_p'])/2

    intraday_data['z5'] = (intraday_data['spread']-mean5)/std5
    intraday_data['z1'] = (intraday_data['spread']-mean1)/std1
    intraday_data['z2'] = (intraday_data['spread']-mean2)/std2
    intraday_data['z6'] = (intraday_data['spread']-mean1)/std5

    entry_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-2]).date()]
    exit_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-1]).date()]

    exit_morning_data = exit_data[(exit_data['hour_minute'] >= 930)&(exit_data['hour_minute'] <= 1000)]
    exit_afternoon_data = exit_data[(exit_data['hour_minute'] >= 1230)&(exit_data['hour_minute'] <= 1300)]

    entry_data['pnl_long_morning'] = long_qty*(exit_morning_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]
    entry_data['pnl_long_morning_wc'] = entry_data['pnl_long_morning'] - 2*t_cost*long_qty*num_contracts

    entry_data['pnl_short_morning'] = short_qty*(exit_morning_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]
    entry_data['pnl_short_morning_wc'] = entry_data['pnl_short_morning'] - 2*t_cost*abs(short_qty)*num_contracts

    entry_data['pnl_long_afternoon'] = long_qty*(exit_afternoon_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]
    entry_data['pnl_long_afternoon_wc'] = entry_data['pnl_long_afternoon'] - 2*t_cost*long_qty*num_contracts

    entry_data['pnl_short_afternoon'] = short_qty*(exit_afternoon_data['spread'].mean()-entry_data['spread'])*contract_multiplier_list[0]
    entry_data['pnl_short_afternoon_wc'] = entry_data['pnl_short_afternoon'] - 2*t_cost*abs(short_qty)*num_contracts

    entry_data['pnl_morning_wc'] = 0
    entry_data['pnl_afternoon_wc'] = 0

    long_indx = entry_data['z1'] < 0
    short_indx = entry_data['z1'] > 0

    entry_data.loc[long_indx, 'pnl_morning_wc'] = entry_data.loc[long_indx, 'pnl_long_morning_wc'].values
    entry_data.loc[short_indx > 0, 'pnl_morning_wc'] = entry_data.loc[short_indx > 0, 'pnl_short_morning_wc'].values

    entry_data.loc[long_indx < 0, 'pnl_afternoon_wc'] = entry_data.loc[long_indx < 0, 'pnl_long_afternoon_wc'].values
    entry_data.loc[short_indx > 0, 'pnl_afternoon_wc'] = entry_data.loc[short_indx > 0, 'pnl_short_afternoon_wc'].values

    return entry_data

def backtest_continuous_ics_4date(**kwargs):

    report_date = kwargs['report_date']

    output_dir = ts.create_strategy_output_dir(strategy_class='ics', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results_cont.pkl'):
        backtest_results = pd.read_pickle(output_dir + '/backtest_results_cont.pkl')
        return backtest_results

    sheet_output = ics.generate_ics_sheet_4date(date_to=report_date)
    intraday_spreads = sheet_output['intraday_spreads']

    backtest_results_list = []

    date_list = [exp.doubledate_shift_bus_days(double_date=report_date, shift_in_days=x) for x in [-1,-2]]

    for i in range(len(intraday_spreads.index)):

        backtest_resul4_4ticker = backtest_continuous_ics(intraday_spreads=intraday_spreads,date_list=date_list,trade_id=i)
        backtest_resul4_4ticker['ticker'] = intraday_spreads['ticker'][i]
        backtest_resul4_4ticker['ticker_head'] = intraday_spreads['ticker_head'][i]

        backtest_results_list.append(backtest_resul4_4ticker)

    backtest_results = pd.concat(backtest_results_list)
    backtest_results['report_date'] = report_date

    backtest_results.to_pickle(output_dir + '/backtest_results_cont.pkl')

    return backtest_results


