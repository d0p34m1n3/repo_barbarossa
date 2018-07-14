
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import pandas as pd
import numpy as np


def get_rolling_curve_data(**kwargs):

    ticker_head = kwargs['ticker_head']
    num_contracts = kwargs['num_contracts']
    front_tr_dte_limit = kwargs['front_tr_dte_limit']
    date_from = kwargs['date_from']
    date_to = kwargs['date_to']

    date_from_datetime = cu.convert_doubledate_2datetime(date_from)
    date_to_datetime = cu.convert_doubledate_2datetime(date_to)

    panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head)

    panel_data = panel_data.loc[(panel_data['settle_date'] >= date_from_datetime) &
                                (panel_data['settle_date'] <= date_to_datetime) &
                                (panel_data['tr_dte'] >= front_tr_dte_limit)]

    if 'month_separation' in kwargs.keys():
        month_separation = kwargs['month_separation']
    elif ticker_head == 'ED':
        month_separation = 3
    else:
        month_separation = 1

    if month_separation != 1:
        panel_data = panel_data[panel_data['ticker_month'] % month_separation == 0]

    panel_data = panel_data[np.isfinite(panel_data['close_price'])]
    sorted_data = panel_data.sort_values(['settle_date', 'tr_dte'], ascending=[True, True])

    filtered_data = sorted_data.groupby('settle_date').filter(lambda x:len(x)>=num_contracts)

    filtered_data2 = filtered_data.groupby('settle_date').filter(lambda x:
                                                            all([cmi.get_month_seperation_from_cont_indx(x['cont_indx'].values[i],
                                                                                                         x['cont_indx'].values[i+1]) ==- month_separation for i in range(num_contracts-1)]))

    grouped = filtered_data2.groupby('settle_date')

    rolling_data_list = []

    for i in range(num_contracts):

        rolling_data_list.append(grouped.nth(i))

    return rolling_data_list

