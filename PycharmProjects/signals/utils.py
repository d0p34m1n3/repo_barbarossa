__author__ = 'kocat_000'

import shared.statistics as stats
import pandas as pd
import numpy as np
import math as m
from dateutil.relativedelta import relativedelta
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
from pandas.tseries.offsets import CustomBusinessDay
import shared.constants as const
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi


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


def get_bus_dates_from_agg_method_and_contracts_back(**kwargs):

    ref_date = kwargs['ref_date']
    aggregation_method = kwargs['aggregation_method']
    contracts_back = kwargs['contracts_back']

    ref_datetime = cu.convert_doubledate_2datetime(ref_date)

    if aggregation_method == 12:
        cal_date_list = [ref_datetime - relativedelta(years=x) for x in range(1, contracts_back+1)]
    elif aggregation_method == 1:
        cal_date_list = [ref_datetime - relativedelta(months=x) for x in range(1, contracts_back+1)]

    bday_us = CustomBusinessDay(expcalendar=exp.get_calendar_4ticker_head(const.reference_tickerhead_4business_calendar))

    if 'shift_bus_days' in kwargs.keys():
        shift_bus_days = kwargs['shift_bus_days']
        if shift_bus_days >= 0:
            bus_date_list = [pd.date_range(x, periods=shift_bus_days+1, freq=bday_us)[shift_bus_days].to_datetime() for x in cal_date_list]
        elif shift_bus_days < 0:
            bus_date_list = [pd.date_range(start=x-relativedelta(days=(max(m.ceil(-shift_bus_days*7/5)+5, -shift_bus_days+5))), end=x, freq=bday_us)[shift_bus_days-1].to_datetime() for x in cal_date_list]
    else:
        bus_date_list = [pd.date_range(x, periods=1, freq=bday_us)[0].to_datetime() for x in cal_date_list]

    return bus_date_list


def get_tickers_from_agg_method_and_contracts_back(**kwargs):

    ticker = kwargs['ticker']
    aggregation_method = kwargs['aggregation_method']
    contracts_back = kwargs['contracts_back']

    contact_specs_out = cmi.get_contract_specs(ticker)

    ref_date = 10000*contact_specs_out['ticker_year']+100*contact_specs_out['ticker_month_num']+1
    ref_datetime = cu.convert_doubledate_2datetime(ref_date)

    if aggregation_method == 12:
        cal_date_list = [ref_datetime - relativedelta(years=x) for x in range(1, contracts_back+1)]
    elif aggregation_method == 1:
        cal_date_list = [ref_datetime - relativedelta(months=x) for x in range(1, contracts_back+1)]

    ticker_list = [contact_specs_out['ticker_head'] + cmi.full_letter_month_list[x.month-1] +
                   str(x.year) for x in cal_date_list]

    return ticker_list


def get_rolling_futures_price(**kwargs):

    if 'roll_tr_dte_aim' in kwargs.keys():
        roll_tr_dte_aim = kwargs['roll_tr_dte_aim']
    else:
        roll_tr_dte_aim = 50

    futures_dataframe = gfp.get_futures_price_preloaded(**kwargs)

    futures_dataframe = futures_dataframe[futures_dataframe['cal_dte'] < 270]
    futures_dataframe.sort(['cont_indx', 'settle_date'], ascending=[True,True], inplace=True)

    grouped = futures_dataframe.groupby('cont_indx')
    shifted = grouped.shift(1)

    futures_dataframe['log_return'] = np.log(futures_dataframe['close_price']/shifted['close_price'])

    futures_dataframe['tr_dte_diff'] = abs(roll_tr_dte_aim-futures_dataframe['tr_dte'])
    futures_dataframe.sort(['settle_date','tr_dte_diff'], ascending=[True,True], inplace=True)
    futures_dataframe.drop_duplicates('settle_date', inplace=True, take_last=False)

    return futures_dataframe


def get_spread_weights_4contract_list(**kwargs):

    ticker_head_list = kwargs['ticker_head_list']

    if ticker_head_list in [['CL', 'HO'], ['CL', 'RB']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -42]
    elif ticker_head_list in [['HO', 'CL'], ['RB', 'CL']]:
        portfolio_weights = [1, -1]
        spread_weights = [42, -1]
    elif ticker_head_list in [['B', 'CL'], ['CL', 'B']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -1]
    elif ticker_head_list == ['S', 'BO', 'SM']:
        portfolio_weights = [1, -1, -1]
        spread_weights = [1, -11, -2.2]
    elif ticker_head_list in [['C', 'W'], ['W', 'C']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -1]
    elif ticker_head_list in [['W', 'KW'], ['KW', 'W']]:
        portfolio_weights = [1, -1]
        spread_weights = [1, -1]
    elif ticker_head_list == ['ES', 'NQ']:
        portfolio_weights = [1, -1]
        spread_weights = [1, -0.45]

    return {'portfolio_weights': portfolio_weights, 'spread_weights': spread_weights}
















