

import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import signals.option_signals as ops
import signals.realized_vol_until_expiration as rvue
import opportunity_constructs.vcs as vcs
import get_price.get_futures_price as gfp
import shared.statistics as stats
import datetime as dt
import numpy as np


def get_scv_signals(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    con = msu.get_my_sql_connection(**kwargs)

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [cmi.get_contract_specs(ticker)['ticker_head']]}

    aligned_indicators_output = ops.get_aligned_option_indicators(ticker_list=[ticker],
                                                                  settle_date=date_to, con=con)

    if not aligned_indicators_output['success']:
        return {'downside': np.NaN, 'upside': np.NaN, 'theta': np.NaN,
                'realized_vol_forecast': np.NaN,
                'real_vol20_current': np.NaN,
                'imp_vol': np.NaN,
                'imp_vol_premium': np.NaN,
                'q': np.NaN}

    hist = aligned_indicators_output['hist']
    current = aligned_indicators_output['current']

    vcs_output = vcs.generate_vcs_sheet_4date(date_to=date_to,con=con)

    if 'con' not in kwargs.keys():
            con.close()

    clean_indx = hist['c1']['profit5'].notnull()
    clean_data = hist[clean_indx]

    if clean_data.empty:
        downside = np.NaN
        upside = np.NaN
    else:
        last_available_align_date = clean_data.index[-1]
        clean_data = clean_data[clean_data.index >= last_available_align_date-dt.timedelta(5*365)]
        profit5 = clean_data['c1']['profit5']

        percentile_vector = stats.get_number_from_quantile(y=profit5.values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(profit5.values)/4)))

        downside = (percentile_vector[0]+percentile_vector[1])/2
        upside = (percentile_vector[2]+percentile_vector[3])/2

    realized_vol_output = rvue.forecast_realized_vol_until_expiration(ticker=ticker,
                                                futures_data_dictionary=futures_data_dictionary,
                                                date_to=date_to)

    realized_vol_forecast = realized_vol_output['realized_vol_forecast']
    real_vol20_current = realized_vol_output['real_vol20_current']
    imp_vol = current['imp_vol'][0]

    imp_vol_premium = 100*(imp_vol-realized_vol_forecast)/imp_vol

    q = np.NaN

    if vcs_output['success']:
        vcs_pairs = vcs_output['vcs_pairs']
        selected_pairs = vcs_pairs[vcs_pairs['ticker2'] == ticker]
        if not selected_pairs.empty:
            q = 100-selected_pairs['Q'].mean()

    return {'downside': downside, 'upside': upside, 'theta': current['theta'][0],
            'realized_vol_forecast': realized_vol_forecast,
            'real_vol20_current': real_vol20_current,
            'imp_vol': imp_vol,
            'imp_vol_premium': imp_vol_premium,
            'q': q}
