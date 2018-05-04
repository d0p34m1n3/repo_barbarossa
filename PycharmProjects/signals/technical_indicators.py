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

    data_frame_input['smg'] = np.nan
    data_frame_input['sml'] = np.nan

    smoothing_lookback = 150

    for i in range(smoothing_lookback):
        if i == 0:
            data_frame_input['smg'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['gains'].iloc[(-smoothing_lookback - period + 1):-smoothing_lookback + 1 + i])
            data_frame_input['sml'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['losses'].iloc[(-smoothing_lookback - period + 1):-smoothing_lookback + 1 + i])
        else:
            data_frame_input['smg'].iloc[-smoothing_lookback + i] = ((period - 1) * data_frame_input['smg'].iloc[-smoothing_lookback - 1 + i] +data_frame_input['gains'].iloc[-smoothing_lookback + i]) / period
            data_frame_input['sml'].iloc[-smoothing_lookback + i] = ((period - 1) * data_frame_input['sml'].iloc[-smoothing_lookback - 1 + i] + data_frame_input['losses'].iloc[-smoothing_lookback + i]) / period


    data_frame_input['rs'] = data_frame_input['smg']/data_frame_input['sml']
    data_frame_input['rsi'] = 100-(100/(1+data_frame_input['rs']))

    data_frame_input.drop(['gains','losses','smg','sml','rs'], 1, inplace=True)

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

def get_atr(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']

    if 'percent_q' in kwargs.keys():
        percent_q = kwargs['percent_q']
    else:
        percent_q = False


    data_frame_input['close_1'] = data_frame_input['close'].shift(1)


    data_frame_input['TR1'] = data_frame_input['high'] - data_frame_input['low']
    data_frame_input['TR2'] = abs(data_frame_input['high'] - data_frame_input['close_1'])
    data_frame_input['TR3'] = abs(data_frame_input['low'] - data_frame_input['close_1'])

    data_frame_input['TR'] = data_frame_input[['TR1', 'TR2', 'TR3']].max(axis=1)

    if percent_q:
        data_frame_input['denominator'] = np.nan
        first_selection = data_frame_input['TR']==data_frame_input['TR2']
        data_frame_input['denominator'].loc[first_selection] = data_frame_input['close_1'].loc[first_selection]+(data_frame_input['TR'].loc[first_selection]/2)
        second_selection = (data_frame_input['TR']==data_frame_input['TR1'])|(data_frame_input['TR']==data_frame_input['TR3'])
        data_frame_input['denominator'].loc[second_selection] = data_frame_input['low'].loc[second_selection] +(data_frame_input['TR'].loc[second_selection]/2)
        data_frame_input['TR'] = 100*data_frame_input['TR']/data_frame_input['denominator']

    data_frame_input['ATR'] = np.nan

    for i in range(150):

        if i == 0:
            data_frame_input['ATR'].iloc[-150 + i] = np.mean(data_frame_input['TR'].iloc[(-150-period+1):-149 + i])
        else:
            data_frame_input['ATR'].iloc[-150 + i] = ((period-1) * data_frame_input['ATR'].iloc[-151 + i] +
                                                      data_frame_input['TR'].iloc[-150 + i]) / period


    return data_frame_input


def get_adx(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    period = kwargs['period']
    smoothing_lookback = 150

    data_frame_input['high_1'] = data_frame_input['high'].shift(1)
    data_frame_input['low_1'] = data_frame_input['low'].shift(1)

    positive_index = data_frame_input['high'] - data_frame_input['high_1'] > data_frame_input['low_1'] - data_frame_input['low']
    data_frame_input['DMP'] = 0
    data_frame_input['DMP'].loc[positive_index] = data_frame_input['high'].loc[positive_index] - data_frame_input['high_1'].loc[positive_index]
    data_frame_input['DMP'][data_frame_input['DMP'] < 0] = 0

    negative_index = data_frame_input['high'] - data_frame_input['high_1'] < data_frame_input['low_1'] - data_frame_input['low']
    data_frame_input['DMN'] = 0
    data_frame_input['DMN'].loc[negative_index] = data_frame_input['low_1'].loc[negative_index] - data_frame_input['low'].loc[negative_index]
    data_frame_input['DMN'][data_frame_input['DMN'] < 0] = 0

    data_frame_input['DMPS'] = np.nan
    data_frame_input['DMNS'] = np.nan


    for i in range(150):

        if i == 0:
            data_frame_input['DMPS'].iloc[-150 + i] = np.mean(data_frame_input['DMP'].iloc[(-150-period+1):-149 + i])
            data_frame_input['DMNS'].iloc[-150 + i] = np.mean(data_frame_input['DMN'].iloc[(-150-period+1):-149 + i])
        else:
            data_frame_input['DMPS'].iloc[-150 + i] = ((period-1) * data_frame_input['DMPS'].iloc[-151 + i] +
                                                      data_frame_input['DMP'].iloc[-150 + i]) / period

            data_frame_input['DMNS'].iloc[-150 + i] = ((period - 1) * data_frame_input['DMNS'].iloc[-151 + i] +
                                                       data_frame_input['DMN'].iloc[-150 + i]) / period

    data_frame_input = get_atr(data_frame_input=data_frame_input,period=period)

    data_frame_input['DIP'] = 100*data_frame_input['DMPS']/data_frame_input['ATR']
    data_frame_input['DIN'] = 100*data_frame_input['DMNS']/data_frame_input['ATR']

    data_frame_input['DX'] = 100*abs(data_frame_input['DIP']-data_frame_input['DIN'])/(data_frame_input['DIP']+data_frame_input['DIN'])

    smoothing_lookback = 120
    data_frame_input['ADX'] = np.nan

    for i in range(smoothing_lookback):

        if i == 0:
            data_frame_input['ADX'].iloc[-smoothing_lookback + i] = np.mean(data_frame_input['DX'].iloc[(-smoothing_lookback-period+1):-smoothing_lookback+1 + i])
        else:
            data_frame_input['ADX'].iloc[-smoothing_lookback + i] = ((period-1) * data_frame_input['ADX'].iloc[-smoothing_lookback-1 + i] +
                                                      data_frame_input['DX'].iloc[-smoothing_lookback + i]) / period

    return data_frame_input




























