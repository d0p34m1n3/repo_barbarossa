__author__ = 'kocat_000'

import numpy as np
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

def rsi(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    change_field = kwargs['change_field']
    period = kwargs['period']

    data_frame_input['rsi'] = np.NaN;

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
















