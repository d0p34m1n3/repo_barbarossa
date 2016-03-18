__author__ = 'kocat_000'

import shared.statistics as stats
import numpy as np


def calc_theo_spread_move_from_ratio_normalization(**kwargs):

    ratio_time_series = kwargs['ratio_time_series']
    starting_quantile = kwargs['starting_quantile']
    num_price = kwargs['num_price']
    den_price = kwargs['den_price']
    favorable_quantile_move_list = kwargs['favorable_quantile_move_list']

    ratio_target_list = [np.NAN]*len(favorable_quantile_move_list)

    if starting_quantile > 50:

        ratio_target_list = stats.get_number_from_quantile(y=ratio_time_series,
                                                       quantile_list=[starting_quantile-x for x in favorable_quantile_move_list])
    elif starting_quantile < 50:

        ratio_target_list = stats.get_number_from_quantile(y=ratio_time_series,
                                                       quantile_list=[starting_quantile+x for x in favorable_quantile_move_list])

    theo_spread_move_list = \
        [calc_spread_move_from_new_ratio(num_price=num_price,
                                         den_price=den_price,
                                         new_ratio=x)
         if np.isfinite(x) else np.NAN for x in ratio_target_list]

    return {'ratio_target_list': ratio_target_list, 'theo_spread_move_list': theo_spread_move_list}


def calc_spread_move_from_new_ratio(**kwargs):

    equal_move = (kwargs['num_price']-kwargs['new_ratio']*kwargs['den_price'])/(1+kwargs['new_ratio'])
    return -2*equal_move


def get_signal_correlation(**kwargs):

    strategy_class = kwargs['strategy_class']
    signal_name = kwargs['signal_name']

    if strategy_class == 'futures_butterfly':
        if signal_name in ['Q', 'QF', 'z1', 'z2', 'z3', 'z4']:
            correlation = -1
        elif signal_name == 'mom5':
            correlation = 1
    elif strategy_class == 'spread_carry':
        if signal_name == 'q':
            correlation = -1
        elif signal_name in ['q_carry','reward_risk'] :
            correlation = 1
    elif strategy_class == 'curve_pca':
        if signal_name == 'z':
            correlation = 1

    return correlation







