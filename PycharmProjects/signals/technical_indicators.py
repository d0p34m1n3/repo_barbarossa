__author__ = 'kocat_000'

import numpy as np
import pandas as pd
import shared.statistics as stats
pd.options.mode.chained_assignment = None  # default='warn'


def rsi(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    change_field = kwargs['change_field']
    period = kwargs['period']

    data_frame_input['rsi'] = np.NaN

    if len(data_frame_input.index)<period:
        return data_frame_input

    data_frame_input['gains'] = data_frame_input[change_field]
    data_frame_input['gains'][data_frame_input['gains']<0] = 0

    data_frame_input['losses'] = data_frame_input[change_field]
    data_frame_input['losses'][data_frame_input['losses']>0] = 0

    data_frame_input['losses'] *= -1

    data_frame_input['gains_ma'] = pd.rolling_mean(data_frame_input['gains'],window=period)
    data_frame_input['losses_ma'] = pd.rolling_mean(data_frame_input['losses'],window=period)

    data_frame_input['rsi'] = 100*data_frame_input['gains_ma']/(data_frame_input['gains_ma']+data_frame_input['losses_ma'])

    data_frame_input.drop(['gains','losses','gains_ma','losses_ma'], 1, inplace=True)

    return data_frame_input


def stochastic(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    p1 = kwargs['p1']
    p2 = kwargs['p2']
    p3 = kwargs['p3']

    data_frame_input['hh'] = pd.rolling_max(data_frame_input['high'], window=p1, min_periods=p1)
    data_frame_input['ll'] = pd.rolling_min(data_frame_input['low'], window=p1, min_periods=p1)
    data_frame_input['K'] = 100*(data_frame_input['close']-data_frame_input['ll'])/(data_frame_input['hh']- data_frame_input['ll'])
    data_frame_input['D1'] = pd.rolling_mean(data_frame_input['K'], window=p2)
    data_frame_input['D2'] = pd.rolling_mean(data_frame_input['D1'], window=p3)

    return data_frame_input.drop(['hh','ll'], 1, inplace=False)


def time_series_regression(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    num_obs = kwargs['num_obs']
    data_frame_input = data_frame_input.iloc[-num_obs:]

    return stats.get_regression_results({'x': range(num_obs),'y':data_frame_input[kwargs['y_var_name']],'clean_num_obs': num_obs})




















