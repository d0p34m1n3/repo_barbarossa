__author__ = 'kocat_000'

import numpy as np
import shared.statistics as stats

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


