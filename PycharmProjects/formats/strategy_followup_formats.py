
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import contract_utilities.expiration as exp
import ta.strategy as ts
import ta.strategy_followup as sf
import shared.converters as sc
import ta.portfolio_manager as pm
import risk.historical_risk as hr
import shared.directory_names as dn
from openpyxl import load_workbook
import pandas as pd


def generate_futures_butterfly_followup_report(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    if 'writer' in kwargs.keys():
        writer = kwargs['writer']
    else:
        ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date, ext='ta')
        writer = pd.ExcelWriter(ta_output_dir + '/followup.xlsx', engine='xlsxwriter')

    strategy_frame = ts.get_open_strategies(**kwargs)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    futures_butterfly_indx = [x == 'futures_butterfly' for x in strategy_class_list]

    futures_butterfly_frame = strategy_frame[futures_butterfly_indx]

    results = [sf.get_results_4strategy(alias=futures_butterfly_frame['alias'].iloc[x],
                                        strategy_info_output=futures_butterfly_frame.iloc[x])
               for x in range(len(futures_butterfly_frame.index))]

    butterfly_followup_frame = pd.DataFrame(results)
    butterfly_followup_frame['alias'] = futures_butterfly_frame['alias'].values

    pnl_frame = pm.get_daily_pnl_snapshot(as_of_date=as_of_date, con=con)
    risk_output = hr.get_historical_risk_4open_strategies(as_of_date=as_of_date, con=con)

    merged_frame1 = pd.merge(butterfly_followup_frame,pnl_frame, how='left', on='alias')
    merged_frame2 = pd.merge(merged_frame1, risk_output['strategy_risk_frame'], how='left', on='alias')

    butterfly_followup_frame = merged_frame2[['alias', 'holding_tr_dte', 'short_tr_dte',
                                                         'z1_initial', 'z1', 'QF_initial', 'QF',
                                                         'total_pnl', 'downside']]
    butterfly_followup_frame.sort('QF', ascending=False,inplace=True)

    butterfly_followup_frame['z1'] = butterfly_followup_frame['z1'].round(2)

    butterfly_followup_frame.to_excel(writer, sheet_name='butterflies')
    worksheet_butterflies = writer.sheets['butterflies']

    worksheet_butterflies.set_column('B:B', 26)
    worksheet_butterflies.freeze_panes(1, 0)

    worksheet_butterflies.autofilter(0, 0, len(butterfly_followup_frame.index),
                              len(butterfly_followup_frame.columns))

    if 'con' not in kwargs.keys():
        con.close()

    return writer


def generate_spread_carry_followup_report(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    con = msu.get_my_sql_connection(**kwargs)

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date, ext='ta')

    if 'writer' in kwargs.keys():
        writer = kwargs['writer']
    else:
        writer = pd.ExcelWriter(ta_output_dir + '/followup.xlsx', engine='xlsxwriter')

    strategy_frame = ts.get_open_strategies(**kwargs)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    spread_carry_indx = [x == 'spread_carry' for x in strategy_class_list]
    spread_carry_frame = strategy_frame[spread_carry_indx]

    results = [sf.get_results_4strategy(alias=spread_carry_frame['alias'].iloc[x],
                                        strategy_info_output=spread_carry_frame.iloc[x],
                                        con=con)
               for x in range(len(spread_carry_frame.index))]

    results_frame_list = [results[x]['results_frame'] for x in range(len(results)) if results[x]['success']]
    spread_carry_followup_frame = pd.concat(results_frame_list)

    spread_carry_followup_frame.to_excel(writer, sheet_name='sc')
    worksheet_sc = writer.sheets['sc']
    worksheet_sc.freeze_panes(1, 0)

    worksheet_sc.autofilter(0, 0, len(spread_carry_followup_frame.index),
                              len(spread_carry_followup_frame.columns))

    if 'con' not in kwargs.keys():
        con.close()

    return writer


def generate_vcs_followup_report(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date, ext='ta')

    con = msu.get_my_sql_connection(**kwargs)

    if 'writer' in kwargs.keys():
        writer = kwargs['writer']
    else:
        writer = pd.ExcelWriter(ta_output_dir + '/followup.xlsx', engine='xlsxwriter')

    strategy_frame = ts.get_open_strategies(**kwargs)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    vcs_indx = [x == 'vcs' for x in strategy_class_list]
    vcs_frame = strategy_frame[vcs_indx]

    results = [sf.get_results_4strategy(alias=vcs_frame['alias'].iloc[x],
                                        strategy_info_output=vcs_frame.iloc[x])
               for x in range(len(vcs_frame.index))]

    vcs_followup_frame = pd.DataFrame(results)
    vcs_followup_frame['alias'] = vcs_frame['alias'].values

    pnl_frame = pm.get_daily_pnl_snapshot(**kwargs)
    merged_frame1 = pd.merge(vcs_followup_frame,pnl_frame, how='left', on='alias')

    vcs_followup_frame = merged_frame1[['alias', 'last_adjustment_days_ago','min_tr_dte', 'long_short_ratio',
                   'net_oev', 'net_theta', 'long_oev', 'short_oev', 'favQMove', 'total_pnl','recommendation']]

    vcs_followup_frame['long_short_ratio'] = vcs_followup_frame['long_short_ratio'].round()
    vcs_followup_frame['net_oev'] = vcs_followup_frame['net_oev'].round(1)
    vcs_followup_frame['long_oev'] = vcs_followup_frame['long_oev'].round(1)
    vcs_followup_frame['short_oev'] = vcs_followup_frame['short_oev'].round(1)
    vcs_followup_frame['net_theta'] = vcs_followup_frame['net_theta'].round(1)

    vcs_followup_frame.sort('total_pnl', ascending=False, inplace=True)
    vcs_followup_frame.reset_index(drop=True,inplace=True)
    vcs_followup_frame.loc[len(vcs_followup_frame.index)] = ['TOTAL', None, None, None, None, vcs_followup_frame['net_theta'].sum(),
                                                             None, None, None, vcs_followup_frame['total_pnl'].sum(), None]

    vcs_followup_frame.to_excel(writer, sheet_name='vcs')
    worksheet_vcs = writer.sheets['vcs']
    worksheet_vcs.set_column('B:B', 18)
    worksheet_vcs.freeze_panes(1, 0)

    worksheet_vcs.autofilter(0, 0, len(vcs_followup_frame.index),
                              len(vcs_followup_frame.columns))

    if 'con' not in kwargs.keys():
        con.close()

    writer.save()








