

import my_sql_routines.my_sql_utilities as msu
import signals.option_signals as ops
import contract_utilities.contract_meta_info as cmi
import matplotlib.pyplot as plt
import numpy as np


def get_vcs_panel_plot(**kwargs):

    ticker_list = kwargs['ticker_list']

    if 'diagnostics_q' in kwargs.keys():
        diagnostics_q = kwargs['diagnostics_q']
    else:
        diagnostics_q = False

    #con = msu.get_my_sql_connection(**kwargs)

    option_indicator_output = ops.get_aligned_option_indicators(ticker_list=ticker_list,settle_date=kwargs['report_date'])
    hist = option_indicator_output['hist']

    new_index = list(range(len(hist.index)))
    contract_change_indx = (hist['c1']['ticker_year']-hist['c1']['ticker_year'].shift(1) != 0).values
    front_contract_year = hist['c1']['ticker_year'].astype('int') % 10

    contract_change_indx[0] = False

    x_tick_locations = [x for x in new_index if contract_change_indx[x]]

    x_tick_values = [cmi.letter_month_string[int(hist['c1']['ticker_month'].values[x])-1]+
                     str(front_contract_year.values[x]) for x in new_index if contract_change_indx[x]]

    plt.figure(figsize=(16, 7))
    plt.plot(hist['c1']['imp_vol']/hist['c2']['imp_vol'])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.ylabel('atmVolRatio')
    plt.grid()
    plt.show()

    fwd_var = hist['c2']['cal_dte']*(hist['c2']['imp_vol']**2)-hist['c1']['cal_dte']*(hist['c1']['imp_vol']**2)
    fwd_vol_sq = fwd_var/(hist['c2']['cal_dte']-hist['c1']['cal_dte'])
    fwd_vol_adj = np.sign(fwd_vol_sq)*((abs(fwd_vol_sq)).apply(np.sqrt))
    hist['fwd_vol_adj'] = fwd_vol_adj

    plt.figure(figsize=(16, 7))
    plt.plot(hist['fwd_vol_adj'])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.ylabel('Fwd Vol')
    plt.grid()
    plt.show()

    plt.figure(figsize=(16, 7))
    plt.plot(hist['c1']['close2close_vol20']/hist['c2']['close2close_vol20'])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.ylabel('realVolRatio')
    plt.grid()
    plt.show()

    plt.figure(figsize=(16, 7))
    plt.plot(hist['c1']['imp_vol']/hist['c1']['close2close_vol20'])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.ylabel('atmRealVolRatio')
    plt.grid()
    plt.show()

    if diagnostics_q:
        plt.figure(figsize=(16, 7))
        plt.plot(hist['c1']['ticker_year'])
        plt.xticks(x_tick_locations,x_tick_values)
        plt.ylabel('ticker_year')
        plt.grid()
        plt.show()

        plt.figure(figsize=(16, 7))
        plt.plot(hist['c1']['tr_dte']-hist['c2']['tr_dte'])
        plt.xticks(x_tick_locations,x_tick_values)
        plt.ylabel('tr dte diff')
        plt.grid()
        plt.show()

        plt.figure(figsize=(16, 7))
        plt.plot(hist['c1']['tr_dte'])
        plt.xticks(x_tick_locations,x_tick_values)
        plt.ylabel('tr dte')
        plt.grid()
        plt.show()

        plt.figure(figsize=(16, 7))
        plt.plot(hist['c1']['ticker_month'])
        plt.xticks(x_tick_locations,x_tick_values)
        plt.ylabel('ticker_month')
        plt.grid()
        plt.show()





    #if 'con' not in kwargs.keys():
    #    con.close()

    #return hist
