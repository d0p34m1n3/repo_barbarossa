__author__ = 'kocat_000'

import numpy as np
import pandas as pd
import shared.statistics as stats
import backtesting.utilities as bu

def get_summary_stats(pnl_series):

    output = dict()

    output['total_pnl'] = float('NaN')
    output['mean_pnl'] = float('NaN')
    output['downside20'] = float('NaN')
    output['downside5'] = float('NaN')
    output['reward_risk'] = float('NaN')

    if len(pnl_series)==0:
        return output

    output['total_pnl'] = np.nansum(pnl_series)
    output['mean_pnl'] = np.nanmean(pnl_series)

    if len(pnl_series) >= 10:
        output['downside20'] = stats.get_number_from_quantile(y=pnl_series, quantile_list=20)

    if len(pnl_series) >= 40:
        output['downside5'] = stats.get_number_from_quantile(y=pnl_series, quantile_list=5)

    output['reward_risk'] = output['mean_pnl']/abs(output['downside20'] )

    return output


def get_indicator_rr_table(**kwargs):

    trade_data = kwargs['trade_data']
    indicator_name = kwargs['indicator_name']

    if 'num_buckets' in kwargs.keys():
        num_buckets = kwargs['num_buckets']
    else:
        num_buckets = 10

    quantile_limits = bu.get_equal_length_bucket_limits(min_value=0,
                                                  max_value=100,
                                                  num_buckets=num_buckets)

    bucket_limits = stats.get_number_from_quantile(y=trade_data[indicator_name].values,
                                                   quantile_list=quantile_limits)


    mean_pnl_list = []
    reward_risk_list = []

    for i in range(num_buckets):

        if i == 0:
            bucket_data = trade_data.loc[trade_data[indicator_name] <= bucket_limits[i],
                                         ['pnl_long5', 'pnl_short5', indicator_name]]
        elif i < num_buckets-1:
            bucket_data = trade_data.loc[(trade_data[indicator_name] > bucket_limits[i-1])&
                                         (trade_data[indicator_name] <= bucket_limits[i]),
                                    ['pnl_long5', 'pnl_short5', indicator_name]]
        else:
            bucket_data = trade_data.loc[trade_data[indicator_name] > bucket_limits[i-1],
                                         ['pnl_long5', 'pnl_short5', indicator_name]]

        if i<=(num_buckets/2):
            signed_pnl = bucket_data['pnl_long5']
        else:
            signed_pnl = bucket_data['pnl_short5']

        stats_output = get_summary_stats(signed_pnl.values)
        mean_pnl_list.append(stats_output['mean_pnl'])
        reward_risk_list.append(stats_output['reward_risk'])

    return pd.DataFrame.from_items([('indicator_ulimit', np.append(bucket_limits,np.NAN)),
                         ('mean_pnl', mean_pnl_list),
                         ('reward_risk',reward_risk_list)])