

import my_sql_routines.my_sql_utilities as msu
import signals.option_signals as ops
import contract_utilities.contract_meta_info as cmi


def get_vcs_panel_plot(**kwargs):

    ticker_list = kwargs['ticker_list']

    #con = msu.get_my_sql_connection(**kwargs)

    signal_output = ops.get_vcs_signals(ticker1=ticker_list[0], ticker2=ticker_list[1],settle_date=kwargs['report_date'])
    hist = signal_output['hist']

    new_index = list(range(len(hist.index)))
    contract_change_indx = (hist['TickerYear1']-hist['TickerYear1'].shift(1)!=0).values
    front_contract_year = hist['TickerYear1'] % 10



    contract_change_indx[0] = False

    x_tick_locations = [x for x in new_index if contract_change_indx[x]]

    x_tick_values = [cmi.letter_month_string[hist['TickerMonth1'].values[x]-1]+
                     str(front_contract_year.values[x]) for x in new_index if contract_change_indx[x]]

    plt.figure(figsize=(16, 7))
    plt.plot(hist['imp_vol_ratio'])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.grid()



    #if 'con' not in kwargs.keys():
    #    con.close()

    #return hist
