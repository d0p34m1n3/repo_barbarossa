
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import pandas as pd


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
                                (panel_data['tr_dte']>= front_tr_dte_limit)]

    sorted_data = panel_data.sort(['settle_date', 'tr_dte'], ascending=[True, True])
    grouped = sorted_data.groupby('settle_date')

    rolling_data_list = []

    for i in range(num_contracts):

        rolling_data_list.append(grouped.nth(i))

    return rolling_data_list

