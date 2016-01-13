__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import pandas as pd
import signals.futures_signals as fs
import os.path
import ta.strategy as ts


def get_futures_butterflies_4date(**kwargs):

    date_to = kwargs['date_to']
    futures_dataframe = cl.generate_futures_list_dataframe({'date_to': date_to})

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
        futures_dataframe = futures_dataframe[futures_dataframe['volume']>volume_filter]

    futures_dataframe.reset_index(drop=True, inplace=True)

    unique_ticker_heads = futures_dataframe['ticker_head'].unique()
    tuples = []

    for ticker_head_i in unique_ticker_heads:
        ticker_head_data = futures_dataframe[futures_dataframe['ticker_head']==ticker_head_i]

        if len(ticker_head_data.index)>=3:
            tuples = tuples + [(ticker_head_data.index[i-1], ticker_head_data.index[i],ticker_head_data.index[i+1]) for i in range(1, len(ticker_head_data.index)-1)]

        if len(ticker_head_data.index)>=5:
            tuples = tuples + [(ticker_head_data.index[i-2], ticker_head_data.index[i],ticker_head_data.index[i+2]) for i in range(2, len(ticker_head_data.index)-2)]

        if len(ticker_head_data.index)>=7:
            tuples = tuples + [(ticker_head_data.index[i-3], ticker_head_data.index[i],ticker_head_data.index[i+3]) for i in range(3, len(ticker_head_data.index)-3)]

    return pd.DataFrame([(futures_dataframe['ticker'][indx[0]],
    futures_dataframe['ticker'][indx[1]],
    futures_dataframe['ticker'][indx[2]],
    futures_dataframe['ticker_head'][indx[0]],
    futures_dataframe['ticker_class'][indx[0]],
    futures_dataframe['tr_dte'][indx[0]],
    futures_dataframe['tr_dte'][indx[1]],
    futures_dataframe['tr_dte'][indx[2]],
    futures_dataframe['multiplier'][indx[0]],
    futures_dataframe['aggregation_method'][indx[0]],
    futures_dataframe['contracts_back'][indx[0]]) for indx in tuples],
                        columns=['ticker1','ticker2','ticker3','tickerHead','tickerClass','trDte1','trDte2','trDte3','multiplier','agg','cBack'])


def generate_futures_butterfly_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        butterflies = pd.read_pickle(output_dir + '/summary.pkl')
        return {'butterflies': butterflies,'success': True}

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
    else:
        volume_filter = 100

    butterflies = get_futures_butterflies_4date(date_to=date_to, volume_filter= volume_filter)

    butterflies = butterflies[butterflies['trDte1'] >= 35]
    butterflies.reset_index(drop=True,inplace=True)
    num_butterflies = len(butterflies)

    q_list = [None]*num_butterflies
    qf_list = [None]*num_butterflies

    zscore1_list = [None]*num_butterflies
    zscore2_list = [None]*num_butterflies
    zscore3_list = [None]*num_butterflies
    zscore4_list = [None]*num_butterflies

    rsquared1_list = [None]*num_butterflies
    rsquared2_list = [None]*num_butterflies

    regime_change_list = [None]*num_butterflies
    contract_seasonality_list = [None]*num_butterflies
    yield1_list = [None]*num_butterflies
    yield2_list = [None]*num_butterflies
    bf_price_list = [None]*num_butterflies
    noise_ratio_list = [None]*num_butterflies
    alpha1_list = [None]*num_butterflies
    alpha2_list = [None]*num_butterflies
    residual_std1_list = [None]*num_butterflies
    residual_std2_list = [None]*num_butterflies

    second_spread_weight_1_list = [None]*num_butterflies
    second_spread_weight_2_list = [None]*num_butterflies

    weight1_list = [None]*num_butterflies
    weight2_list = [None]*num_butterflies
    weight3_list = [None]*num_butterflies

    downside_list = [None]*num_butterflies
    upside_list = [None]*num_butterflies

    recent_5day_pnl_list = [None]*num_butterflies
    recent_vol_ratio_list = [None]*num_butterflies
    theo_pnl_list = [None]*num_butterflies

    price_1_list = [None]*num_butterflies
    price_2_list = [None]*num_butterflies
    price_3_list = [None]*num_butterflies

    mean_reversion_rsquared_list = [None]*num_butterflies
    mean_reversion_signif_list = [None]*num_butterflies

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.futures_butterfly_strategy_tickerhead_list}

    date5_years_ago = cu.doubledate_shift(date_to,5*365)
    datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    for i in range(num_butterflies):
        bf_signals_output = fs.get_futures_butterfly_signals(ticker_list=[butterflies['ticker1'][i], butterflies['ticker2'][i], butterflies['ticker3'][i]],
                                          tr_dte_list=[butterflies['trDte1'][i], butterflies['trDte2'][i], butterflies['trDte3'][i]],
                                          aggregation_method=butterflies['agg'][i],
                                          contracts_back=butterflies['cBack'][i],
                                          date_to=date_to,
                                          futures_data_dictionary=futures_data_dictionary,
                                          contract_multiplier=butterflies['multiplier'][i],
                                          datetime5_years_ago=datetime5_years_ago)

        q_list[i] = bf_signals_output['q']
        qf_list[i] = bf_signals_output['qf']
        zscore1_list[i] = bf_signals_output['zscore1']
        zscore2_list[i] = bf_signals_output['zscore2']
        zscore3_list[i] = bf_signals_output['zscore3']
        zscore4_list[i] = bf_signals_output['zscore4']
        rsquared1_list[i] = bf_signals_output['rsquared1']
        rsquared2_list[i] = bf_signals_output['rsquared2']

        regime_change_list[i] = bf_signals_output['regime_change_ind']
        contract_seasonality_list[i] = bf_signals_output['contract_seasonality_ind']
        yield1_list[i] = bf_signals_output['yield1_current']
        yield2_list[i] = bf_signals_output['yield2_current']
        bf_price_list[i] = bf_signals_output['bf_price']
        noise_ratio_list[i] = bf_signals_output['noise_ratio']
        alpha1_list[i] = bf_signals_output['alpha1']
        alpha2_list[i] = bf_signals_output['alpha2']
        residual_std1_list = bf_signals_output['residual_std1']
        residual_std2_list = bf_signals_output['residual_std2']

        second_spread_weight_1_list[i] = bf_signals_output['second_spread_weight_1']
        second_spread_weight_2_list[i] = bf_signals_output['second_spread_weight_2']
        weight1_list[i] = bf_signals_output['weight1']
        weight2_list[i] = bf_signals_output['weight2']
        weight3_list[i] = bf_signals_output['weight3']

        downside_list[i] = bf_signals_output['downside']
        upside_list[i] = bf_signals_output['upside']

        recent_5day_pnl_list[i] = bf_signals_output['recent_5day_pnl']
        recent_vol_ratio_list[i] = bf_signals_output['recent_vol_ratio']
        theo_pnl_list[i] = bf_signals_output['theo_pnl']

        price_1_list[i] = bf_signals_output['price_1']
        price_2_list[i] = bf_signals_output['price_2']
        price_3_list[i] = bf_signals_output['price_3']

        mean_reversion_rsquared_list[i] = bf_signals_output['mean_reversion_rsquared']
        mean_reversion_signif_list[i] = bf_signals_output['mean_reversion_signif']

    butterflies['Q'] = q_list
    butterflies['QF'] = qf_list

    butterflies['z1'] = zscore1_list
    butterflies['z2'] = zscore2_list
    butterflies['z3'] = zscore3_list
    butterflies['z4'] = zscore4_list

    butterflies['r1'] = rsquared1_list
    butterflies['r2'] = rsquared2_list

    butterflies['RC'] = regime_change_list
    butterflies['seasonality'] = contract_seasonality_list
    butterflies['yield1'] = yield1_list
    butterflies['yield2'] = yield2_list

    butterflies['bf_price'] = bf_price_list
    butterflies['noise_ratio'] = noise_ratio_list
    butterflies['alpha1'] = alpha1_list
    butterflies['alpha2'] = alpha2_list

    butterflies['residual_std1'] = residual_std1_list
    butterflies['residual_std2'] = residual_std2_list

    butterflies['second_spread_weight_1'] = second_spread_weight_1_list
    butterflies['second_spread_weight_2'] = second_spread_weight_2_list

    butterflies['weight1'] = weight1_list
    butterflies['weight2'] = weight2_list
    butterflies['weight3'] = weight3_list
    butterflies['downside'] = downside_list
    butterflies['upside'] = upside_list

    butterflies['recent_5day_pnl'] = recent_5day_pnl_list
    butterflies['recent_vol_ratio'] = recent_vol_ratio_list
    butterflies['theo_pnl'] = theo_pnl_list

    butterflies['price1'] = price_1_list
    butterflies['price2'] = price_2_list
    butterflies['price3'] = price_3_list

    butterflies['mean_reversion_rsquared'] = mean_reversion_rsquared_list
    butterflies['mean_reversion_signif'] = mean_reversion_signif_list

    butterflies['z1'] = butterflies['z1'].round(2)
    butterflies['z2'] = butterflies['z2'].round(2)
    butterflies['z3'] = butterflies['z3'].round(2)
    butterflies['z4'] = butterflies['z4'].round(2)
    butterflies['r1'] = butterflies['r1'].round(2)
    butterflies['r2'] = butterflies['r2'].round(2)
    butterflies['RC'] = butterflies['RC'].round(2)
    butterflies['seasonality'] = butterflies['seasonality'].round(2)
    butterflies['second_spread_weight_1'] = butterflies['second_spread_weight_1'].round(2)
    butterflies['second_spread_weight_2'] = butterflies['second_spread_weight_1'].round(2)

    butterflies['yield1'] = butterflies['yield1'].round(3)
    butterflies['yield2'] = butterflies['yield2'].round(3)

    butterflies['noise_ratio'] = butterflies['noise_ratio'].round(3)
    butterflies['alpha1'] = butterflies['alpha1'].round(3)
    butterflies['alpha2'] = butterflies['alpha2'].round(3)

    butterflies['residual_std1'] = butterflies['residual_std1'].round(3)
    butterflies['residual_std2'] = butterflies['residual_std2'].round(3)

    butterflies['downside'] = butterflies['downside'].round(3)
    butterflies['upside'] = butterflies['upside'].round(3)

    butterflies['recent_5day_pnl'] = butterflies['recent_5day_pnl'].round(3)
    butterflies['recent_vol_ratio'] = butterflies['recent_vol_ratio'].round(2)
    butterflies['theo_pnl'] = butterflies['theo_pnl'].round(3)

    butterflies['price1'] = butterflies['price1'].round(4)
    butterflies['price2'] = butterflies['price2'].round(4)
    butterflies['price3'] = butterflies['price3'].round(4)

    butterflies['mean_reversion_rsquared'] = butterflies['mean_reversion_rsquared'].round(2)

    butterflies.to_pickle(output_dir + '/summary.pkl')

    return {'butterflies': butterflies,'success': True}





