__author__ = 'kocat_000'

import numpy as np
import pandas as pd
import shared.statistics as stats
import shared.utils as su


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
        num_buckets = 9

    trade_data = trade_data[np.isfinite(trade_data['pnl_long5'])]

    bucket_data_output = su.bucket_data(data_input=trade_data, bucket_var=indicator_name, num_buckets=num_buckets)
    bucket_data_list = bucket_data_output['bucket_data_list']
    bucket_limits = bucket_data_output['bucket_limits']

    mean_pnl_list = []
    reward_risk_list = []

    for i in range(num_buckets):

        bucket_data = bucket_data_list[i]

        if i <= (num_buckets/2):
            signed_pnl = bucket_data['pnl_long5']
        else:
            signed_pnl = bucket_data['pnl_short5']

        stats_output = get_summary_stats(signed_pnl.values)
        mean_pnl_list.append(stats_output['mean_pnl'])
        reward_risk_list.append(stats_output['reward_risk'])

    return pd.DataFrame.from_items([('indicator_ulimit', np.append(bucket_limits, np.NAN)),
                         ('mean_pnl', mean_pnl_list),
                         ('reward_risk',reward_risk_list)])


def get_indicator_rr_double_table(**kwargs):

    trade_data = kwargs['trade_data']
    indicator_list = kwargs['indicator_list']

    if 'num_buckets' in kwargs.keys():
        num_buckets = kwargs['num_buckets']
    else:
        num_buckets = 3

    trade_data = trade_data[np.isfinite(trade_data['pnl_long5'])]

    bucket_data_output = su.bucket_data(data_input=trade_data, bucket_var=indicator_list[0], num_buckets=num_buckets)
    bucket_data_list1 = bucket_data_output['bucket_data_list']
    bucket_limits1_full = np.repeat(np.append(bucket_data_output['bucket_limits'], np.NAN), num_buckets)

    bucket_limits2_full = np.empty([1, 0])

    mean_pnl_list = []
    reward_risk_list = []

    for i in range(len(bucket_data_list1)):
        bucket_data_output = su.bucket_data(data_input=bucket_data_list1[i],
                                            bucket_var=indicator_list[1],
                                            num_buckets=num_buckets)
        bucket_data_list2 = bucket_data_output['bucket_data_list']
        bucket_limits2 = bucket_data_output['bucket_limits']

        bucket_limits2_full = np.append(bucket_limits2_full,
                                            np.append(bucket_limits2, np.NAN))

        for j in range(len(bucket_data_list2)):

            if i <= (num_buckets/2):
                signed_pnl = bucket_data_list2[j]['pnl_long5']
            else:
                signed_pnl = bucket_data_list2[j]['pnl_short5']

            stats_output = get_summary_stats(signed_pnl.values)
            mean_pnl_list.append(stats_output['mean_pnl'])
            reward_risk_list.append(stats_output['reward_risk'])

    return pd.DataFrame.from_items([('indicator1_ulimit', bucket_limits1_full),
                                 ('indicator2_ulimit', bucket_limits2_full),
                                  ('mean_pnl', mean_pnl_list),
                                 ('reward_risk',reward_risk_list)])


def get_indicator_ranking(**kwargs):
    trade_data = kwargs['trade_data']
    indicator_list = kwargs['indicator_list']

    trade_data = trade_data[np.isfinite(trade_data['pnl_long5'])]

    long_rr_list = []
    short_rr_list = []

    for i in range(len(indicator_list)):

        if isinstance(indicator_list[i],list):
            q_rr_table = get_indicator_rr_double_table(trade_data=trade_data,indicator_list=[indicator_list[i][0],
                                                                                indicator_list[i][1]])

        else:
            q_rr_table = get_indicator_rr_table(trade_data=trade_data, indicator_name=indicator_list[i])
        long_rr_list.append(q_rr_table['reward_risk'].iloc[0])
        short_rr_list.append(q_rr_table['reward_risk'].iloc[-1])

    long_ranking = np.array(long_rr_list).argsort().argsort()
    short_ranking = np.array(short_rr_list).argsort().argsort()

    return pd.DataFrame.from_items([('indicator',indicator_list ),
                                    ('ranking', long_ranking+short_ranking)])


def rank_indicators(**kwargs):
    trade_data = kwargs['trade_data']
    indicator_list_raw = kwargs['indicator_list']

    indicator_list = indicator_list_raw[:]

    for i in range(len(indicator_list_raw)):
        for j in range(len(indicator_list_raw)):
            if i==j:
                continue
            indicator_list.append([indicator_list_raw[i],
                               indicator_list_raw[j]])

    indicator_ranking_total = get_indicator_ranking(trade_data=trade_data, indicator_list=indicator_list)
    indicator_ranking_total.sort('ranking',ascending=False,inplace=True)

    ticker_head_list = list(trade_data['tickerHead'].unique())

    ranking_list = []

    for i in range(len(ticker_head_list)):

        data_4tickerhead = trade_data[trade_data['tickerHead'] == ticker_head_list[i]]
        indicator_ranking_output = get_indicator_ranking(trade_data=data_4tickerhead,
                                                 indicator_list=indicator_list)

        ranking_list.append(indicator_ranking_output['ranking'].values)

    tickerhead_ranking_sums = pd.DataFrame(ranking_list).sum()

    tickerhead_ranking_frame = pd.DataFrame.from_items([('indicator', indicator_list),
                             ('ranking', tickerhead_ranking_sums)])

    return {'indicator_ranking_total': indicator_ranking_total,
            'indicator_ranking_tickerhead_total': tickerhead_ranking_frame.sort('ranking',ascending=False,inplace=False)}