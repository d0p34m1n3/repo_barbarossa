
import opportunity_constructs.overnight_calendar_spreads as ocs
import contract_utilities.expiration as exp
import ta.strategy as ts
import numpy as np
import pandas as pd
import os.path

def get_backtest_summary_4_date(**kwargs):

    report_date = kwargs['report_date']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    output_dir = ts.create_strategy_output_dir(strategy_class='ocs', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results.pkl') and use_existing_filesQ:
        return pd.read_pickle(output_dir + '/backtest_results.pkl')

    ocs_output = ocs.generate_overnight_spreads_sheet_4date(date_to=report_date)
    strategy_sheet = ocs_output['overnight_calendars']

    date_list = [exp.doubledate_shift_bus_days(double_date=report_date, shift_in_days=-x) for x in range(1, 11)]

    fwd_looking_dictionary = {}

    for i in range(len(date_list)):
        ocs_output = ocs.generate_overnight_spreads_sheet_4date(date_to=date_list[i])
        fwd_looking_dictionary[i+1] = ocs_output['overnight_calendars']

    long_frame = strategy_sheet[(strategy_sheet['butterflyZ'] > 1) & (strategy_sheet['butterflyQ'] > 72)]
    short_frame = strategy_sheet[(strategy_sheet['butterflyZ'] < -1) & (strategy_sheet['butterflyQ'] < 28)]

    long_frame.reset_index(inplace=True,drop=True)
    short_frame.reset_index(inplace=True, drop=True)

    long_frame['path_pnl1'] = np.nan
    long_frame['path_pnl2'] = np.nan
    long_frame['path_pnl3'] = np.nan

    long_frame['holding_period1'] = np.nan
    long_frame['holding_period2'] = np.nan
    long_frame['holding_period3'] = np.nan

    short_frame['path_pnl1'] = np.nan
    short_frame['path_pnl2'] = np.nan
    short_frame['path_pnl3'] = np.nan

    short_frame['holding_period1'] = np.nan
    short_frame['holding_period2'] = np.nan
    short_frame['holding_period3'] = np.nan

    for i in range(len(long_frame.index)):
        ticker1 = long_frame['ticker1'].loc[i]
        ticker2 = long_frame['ticker2'].loc[i]
        contract_multiplier = long_frame['multiplier'].loc[i]
        spread_price_initial = long_frame['spreadPrice'].loc[i]
        holding_period1 = 0
        holding_period2 = 0
        holding_period3 = 0
        for j in range(1, 11):
            fwd_looking_data = fwd_looking_dictionary[j]
            select_data = fwd_looking_data[
                (fwd_looking_data['ticker1'] == ticker1) & (fwd_looking_data['ticker2'] == ticker2)]

            if select_data.empty:
                continue
            if ((select_data['butterflyZ'].iloc[0] < 0.5) | (j == 10)) & (holding_period1 == 0):
                long_frame['path_pnl1'].loc[i] = contract_multiplier * (
                select_data['spreadPrice'].iloc[0] - spread_price_initial)
                long_frame['holding_period1'].loc[i] = j
                holding_period1 = j
            if ((select_data['butterflyZ'].iloc[0] < 0) | (j == 10)) & (holding_period2 == 0):
                long_frame['path_pnl2'].loc[i] = contract_multiplier * (
                select_data['spreadPrice'].iloc[0] - spread_price_initial)
                long_frame['holding_period2'].loc[i] = j
                holding_period2 = j
            if ((select_data['butterflyQ'].iloc[0] < 60) | (j == 10)) & (holding_period3 == 0):
                long_frame['path_pnl3'].loc[i] = contract_multiplier * (
                select_data['spreadPrice'].iloc[0] - spread_price_initial)
                long_frame['holding_period3'].loc[i] = j
                holding_period3 = j

    for i in range(len(short_frame.index)):
        ticker1 = short_frame['ticker1'].loc[i]
        ticker2 = short_frame['ticker2'].loc[i]
        contract_multiplier = short_frame['multiplier'].loc[i]
        spread_price_initial = short_frame['spreadPrice'].loc[i]
        holding_period1 = 0
        holding_period2 = 0
        holding_period3 = 0
        for j in range(1, 11):
            fwd_looking_data = fwd_looking_dictionary[j]
            select_data = fwd_looking_data[
                (fwd_looking_data['ticker1'] == ticker1) & (fwd_looking_data['ticker2'] == ticker2)]

            if select_data.empty:
                continue
            if ((select_data['butterflyZ'].iloc[0] > -0.5) | (j == 10)) & (holding_period1 == 0):
                short_frame['path_pnl1'].loc[i] = contract_multiplier * (
                spread_price_initial - select_data['spreadPrice'].iloc[0])
                short_frame['holding_period1'].loc[i] = j
                holding_period1 = j
            if ((select_data['butterflyZ'].iloc[0] > 0) | (j == 10)) & (holding_period2 == 0):
                short_frame['path_pnl2'].loc[i] = contract_multiplier * (
                spread_price_initial - select_data['spreadPrice'].iloc[0])
                short_frame['holding_period2'].loc[i] = j
                holding_period2 = j
            if ((select_data['butterflyQ'].iloc[0] > 40) | (j == 10)) & (holding_period3 == 0):
                short_frame['path_pnl3'].loc[i] = contract_multiplier * (
                spread_price_initial - select_data['spreadPrice'].iloc[0])
                short_frame['holding_period3'].loc[i] = j
                holding_period3 = j

    trades_frame = pd.concat([long_frame,short_frame])
    trades_frame['path_pnl1Net'] = trades_frame['path_pnl1']-12
    trades_frame['path_pnl2Net'] = trades_frame['path_pnl2']-12
    trades_frame['path_pnl3Net'] = trades_frame['path_pnl3']-12

    trades_frame.to_pickle(output_dir + '/backtest_results.pkl')

    return trades_frame


