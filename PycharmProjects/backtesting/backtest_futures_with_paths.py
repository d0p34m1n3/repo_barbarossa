__author__ = 'kocat_000'

import opportunity_constructs.futures_butterfly as opfb
import pandas as pd
import numpy as np
import backtesting.utilities as bu
import ta.strategy as ts
import os.path
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi

def get_backtest_summary_4_date(**kwargs):

    report_date = kwargs['report_date']
    futures_data_dictionary = kwargs['futures_data_dictionary']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    if os.path.isfile(output_dir + '/backtest_results.pkl') and use_existing_filesQ:
        return pd.read_pickle(output_dir + '/backtest_results.pkl')

    butt_out = opfb.generate_futures_butterfly_sheet_4date(date_to=report_date)

    strategy_sheet = butt_out['butterflies']
    num_trades = len(strategy_sheet.index)

    holding_period5 = [np.NAN]*num_trades
    holding_period10 = [np.NAN]*num_trades
    holding_period15 = [np.NAN]*num_trades
    holding_period20 = [np.NAN]*num_trades
    holding_period25 = [np.NAN]*num_trades

    path_pnl5 = [np.NAN]*num_trades
    path_pnl10 = [np.NAN]*num_trades
    path_pnl15 = [np.NAN]*num_trades
    path_pnl20 = [np.NAN]*num_trades
    path_pnl25 = [np.NAN]*num_trades

    hold_pnl1long = [np.NAN]*num_trades
    hold_pnl2long = [np.NAN]*num_trades
    hold_pnl5long = [np.NAN]*num_trades
    hold_pnl10long = [np.NAN]*num_trades
    hold_pnl20long = [np.NAN]*num_trades

    hold_pnl1short = [np.NAN]*num_trades
    hold_pnl2short = [np.NAN]*num_trades
    hold_pnl5short = [np.NAN]*num_trades
    hold_pnl10short = [np.NAN]*num_trades
    hold_pnl20short = [np.NAN]*num_trades

    path_pnl5_per_contract = [np.NAN]*num_trades
    path_pnl10_per_contract = [np.NAN]*num_trades
    path_pnl15_per_contract = [np.NAN]*num_trades
    path_pnl20_per_contract = [np.NAN]*num_trades
    path_pnl25_per_contract = [np.NAN]*num_trades

    hold_pnl1long_per_contract = [np.NAN]*num_trades
    hold_pnl2long_per_contract = [np.NAN]*num_trades
    hold_pnl5long_per_contract = [np.NAN]*num_trades
    hold_pnl10long_per_contract = [np.NAN]*num_trades
    hold_pnl20long_per_contract = [np.NAN]*num_trades

    hold_pnl1short_per_contract = [np.NAN]*num_trades
    hold_pnl2short_per_contract = [np.NAN]*num_trades
    hold_pnl5short_per_contract = [np.NAN]*num_trades
    hold_pnl10short_per_contract = [np.NAN]*num_trades
    hold_pnl20short_per_contract = [np.NAN]*num_trades

    for i in range(num_trades):
        sheet_entry = strategy_sheet.iloc[i]

        data_list = []

        for j in range(3):

            ticker_frame = gfp.get_futures_price_preloaded(ticker=sheet_entry['ticker'+str(j+1)],
                                    futures_data_dictionary=futures_data_dictionary,
                                    settle_date_from_exclusive=report_date)

            ticker_frame.set_index('settle_date', drop=False, inplace=True)
            data_list.append(ticker_frame)

        merged_data = pd.concat(data_list, axis=1, join='inner')

        mid_price = (merged_data['close_price'].iloc[:,0]*sheet_entry['weight1']+
                     merged_data['close_price'].iloc[:,2]*sheet_entry['weight3'])
        ratio_path = mid_price/(-sheet_entry['weight2']*merged_data['close_price'].iloc[:,1])

        if len(ratio_path.index) < 20:
            continue

        quantity_long = round(10000/abs(sheet_entry['downside']))
        quantity_short = -round(10000/abs(sheet_entry['upside']))

        contracts_traded_per_unit = 4*(1+sheet_entry['second_spread_weight_1'])

        if sheet_entry['QF'] > 50:
            trigger_direction = 'going_down'
            quantity = quantity_short
        elif sheet_entry['QF'] < 50:
            trigger_direction = 'going_up'
            quantity = quantity_long
        else:
            quantity = np.NAN

        total_contracts_traded_per_unit = abs(quantity)*contracts_traded_per_unit

        exit5 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target5'],
                                   trigger_direction=trigger_direction,max_exit_point=20)

        exit10 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target10'],
                                    trigger_direction=trigger_direction,max_exit_point=20)

        exit15 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target15'],
                                    trigger_direction=trigger_direction,max_exit_point=20)

        exit20 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target20'],
                                    trigger_direction=trigger_direction,max_exit_point=20)

        exit25 = bu.find_exit_point(time_series=ratio_path,trigger_value=sheet_entry['ratio_target25'],
                                    trigger_direction=trigger_direction,max_exit_point=20)

        holding_period5[i] = exit5
        holding_period10[i] = exit10
        holding_period15[i] = exit15
        holding_period20[i] = exit20
        holding_period25[i] = exit25

        path_path_list = []
        hold_pnl_list = []

        for exit_indx in [exit5,exit10,exit15,exit20,exit25]:

            raw_pnl = (merged_data['close_price'].iloc[exit_indx,0]-merged_data['close_price'].iloc[0,0])\
                       -(1+sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[exit_indx,1]-merged_data['close_price'].iloc[0,1])\
                       +(sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[exit_indx,2]-merged_data['close_price'].iloc[0,2])

            path_path_list.append(raw_pnl*sheet_entry['multiplier']*quantity)

        for hold_indx in [1, 2, 5, 10, 20]:

            hold_pnl = (merged_data['close_price'].iloc[hold_indx, 0]-merged_data['close_price'].iloc[0, 0])\
                       -(1+sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[hold_indx, 1]-merged_data['close_price'].iloc[0, 1])\
                       +(sheet_entry['second_spread_weight_1'])*(merged_data['close_price'].iloc[hold_indx, 2]-merged_data['close_price'].iloc[0, 2])

            hold_pnl_list.append(hold_pnl*sheet_entry['multiplier'])

        path_pnl5[i] = path_path_list[0]
        path_pnl10[i] = path_path_list[1]
        path_pnl15[i] = path_path_list[2]
        path_pnl20[i] = path_path_list[3]
        path_pnl25[i] = path_path_list[4]

        path_pnl5_per_contract[i] = path_path_list[0]/total_contracts_traded_per_unit
        path_pnl10_per_contract[i] = path_path_list[1]/total_contracts_traded_per_unit
        path_pnl15_per_contract[i] = path_path_list[2]/total_contracts_traded_per_unit
        path_pnl20_per_contract[i] = path_path_list[3]/total_contracts_traded_per_unit
        path_pnl25_per_contract[i] = path_path_list[4]/total_contracts_traded_per_unit

        hold_pnl1long[i] = hold_pnl_list[0]*quantity_long
        hold_pnl2long[i] = hold_pnl_list[1]*quantity_long
        hold_pnl5long[i] = hold_pnl_list[2]*quantity_long
        hold_pnl10long[i] = hold_pnl_list[3]*quantity_long
        hold_pnl20long[i] = hold_pnl_list[4]*quantity_long

        hold_pnl1long_per_contract[i] = hold_pnl_list[0]/contracts_traded_per_unit
        hold_pnl2long_per_contract[i] = hold_pnl_list[1]/contracts_traded_per_unit
        hold_pnl5long_per_contract[i] = hold_pnl_list[2]/contracts_traded_per_unit
        hold_pnl10long_per_contract[i] = hold_pnl_list[3]/contracts_traded_per_unit
        hold_pnl20long_per_contract[i] = hold_pnl_list[4]/contracts_traded_per_unit

        hold_pnl1short[i] = hold_pnl_list[0]*quantity_short
        hold_pnl2short[i] = hold_pnl_list[1]*quantity_short
        hold_pnl5short[i] = hold_pnl_list[2]*quantity_short
        hold_pnl10short[i] = hold_pnl_list[3]*quantity_short
        hold_pnl20short[i] = hold_pnl_list[4]*quantity_short

        hold_pnl1short_per_contract[i] = -hold_pnl_list[0]/contracts_traded_per_unit
        hold_pnl2short_per_contract[i] = -hold_pnl_list[1]/contracts_traded_per_unit
        hold_pnl5short_per_contract[i] = -hold_pnl_list[2]/contracts_traded_per_unit
        hold_pnl10short_per_contract[i] = -hold_pnl_list[3]/contracts_traded_per_unit
        hold_pnl20short_per_contract[i] = -hold_pnl_list[4]/contracts_traded_per_unit

    strategy_sheet['holding_period5'] = holding_period5
    strategy_sheet['holding_period10'] = holding_period10
    strategy_sheet['holding_period15'] = holding_period15
    strategy_sheet['holding_period20'] = holding_period20
    strategy_sheet['holding_period25'] = holding_period25

    strategy_sheet['path_pnl5'] = path_pnl5
    strategy_sheet['path_pnl10'] = path_pnl10
    strategy_sheet['path_pnl15'] = path_pnl15
    strategy_sheet['path_pnl20'] = path_pnl20
    strategy_sheet['path_pnl25'] = path_pnl25

    strategy_sheet['path_pnl5_per_contract'] = path_pnl5_per_contract
    strategy_sheet['path_pnl10_per_contract'] = path_pnl10_per_contract
    strategy_sheet['path_pnl15_per_contract'] = path_pnl15_per_contract
    strategy_sheet['path_pnl20_per_contract'] = path_pnl20_per_contract
    strategy_sheet['path_pnl25_per_contract'] = path_pnl25_per_contract

    strategy_sheet['hold_pnl1long'] = hold_pnl1long
    strategy_sheet['hold_pnl2long'] = hold_pnl2long
    strategy_sheet['hold_pnl5long'] = hold_pnl5long
    strategy_sheet['hold_pnl10long'] = hold_pnl10long
    strategy_sheet['hold_pnl20long'] = hold_pnl20long

    strategy_sheet['hold_pnl1long_per_contract'] = hold_pnl1long_per_contract
    strategy_sheet['hold_pnl2long_per_contract'] = hold_pnl2long_per_contract
    strategy_sheet['hold_pnl5long_per_contract'] = hold_pnl5long_per_contract
    strategy_sheet['hold_pnl10long_per_contract'] = hold_pnl10long_per_contract
    strategy_sheet['hold_pnl20long_per_contract'] = hold_pnl20long_per_contract

    strategy_sheet['hold_pnl1short'] = hold_pnl1short
    strategy_sheet['hold_pnl2short'] = hold_pnl2short
    strategy_sheet['hold_pnl5short'] = hold_pnl5short
    strategy_sheet['hold_pnl10short'] = hold_pnl10short
    strategy_sheet['hold_pnl20short'] = hold_pnl20short

    strategy_sheet['hold_pnl1short_per_contract'] = hold_pnl1short_per_contract
    strategy_sheet['hold_pnl2short_per_contract'] = hold_pnl2short_per_contract
    strategy_sheet['hold_pnl5short_per_contract'] = hold_pnl5short_per_contract
    strategy_sheet['hold_pnl10short_per_contract'] = hold_pnl10short_per_contract
    strategy_sheet['hold_pnl20short_per_contract'] = hold_pnl20short_per_contract

    strategy_sheet['report_date'] = report_date

    strategy_sheet['hold_pnl1'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl2'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl5'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl10'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl20'] = [np.NAN]*num_trades

    strategy_sheet['hold_pnl1_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl2_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl5_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl10_per_contract'] = [np.NAN]*num_trades
    strategy_sheet['hold_pnl20_per_contract'] = [np.NAN]*num_trades

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl2short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl5short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl10short']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20'] = strategy_sheet.loc[strategy_sheet['QF'] > 50,'hold_pnl20short']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl1long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl2long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl5long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl10long']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20'] = strategy_sheet.loc[strategy_sheet['QF'] < 50,'hold_pnl20long']

    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl1short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl2short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl5short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl10short_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] > 50, 'hold_pnl20short_per_contract']

    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl1long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl2long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl5long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl10long_per_contract']
    strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20_per_contract'] = strategy_sheet.loc[strategy_sheet['QF'] < 50, 'hold_pnl20long_per_contract']

    strategy_sheet.to_pickle(output_dir + '/backtest_results.pkl')

    return strategy_sheet


def get_backtest_summary(**kwargs):

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.futures_butterfly_strategy_tickerhead_list}
    date_list = kwargs['date_list']

    if 'use_existing_filesQ' in kwargs.keys():
        use_existing_filesQ = kwargs['use_existing_filesQ']
    else:
        use_existing_filesQ = True

    backtest_output = []

    for report_date in date_list:
        backtest_output.append(get_backtest_summary_4_date(report_date=report_date,
                                                                futures_data_dictionary=futures_data_dictionary,
                                                                use_existing_filesQ=use_existing_filesQ))

    return {'big_data' : pd.concat(backtest_output), 'backtest_output': backtest_output}




