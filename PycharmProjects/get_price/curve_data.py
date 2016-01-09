
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

    panel_data = panel_data.loc[(panel_data['settle_date'] >= date_from_datetime) & (panel_data['settle_date']<=date_to_datetime)]

    unique_dates = panel_data['settle_date'].unique()
    unique_dates.sort()

    num_obs = len(unique_dates)

    success_indx = [False]*num_obs

    data_list = []

    for report_date in unique_dates:

        data4day = panel_data[panel_data['settle_date'] == report_date]
        data4day = data4day[data4day['tr_dte'] >= front_tr_dte_limit]

        if len(data4day.index) < num_contracts:
            continue

        data4day = data4day.iloc[:num_contracts]

        month_diff = [cmi.get_month_seperation_from_cont_indx(data4day.iloc[x+1]['cont_indx'],
                                                              data4day.iloc[x]['cont_indx']) for x in range(len(data4day)-1)]

        if not all([x==1 for x in month_diff]):
            continue

        data4day['no'] = range(1, len(data4day.index)+1)
        data4day = data4day.pivot(index='settle_date', columns='no')
        data_list.append(data4day)

    return pd.concat(data_list)

