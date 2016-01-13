__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')

import pandas as pd
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
from pandas.tseries.offsets import CustomBusinessDay
import math as m
import numpy as np

def get_backtesting_dates(**kwargs):
    date_to = kwargs['date_to']
    years_back = kwargs['years_back']

    date_from = cu.doubledate_shift(date_to, years_back*365)

    trading_calendar = exp.get_calendar_4ticker_head('CL')
    bday_us = CustomBusinessDay(calendar=trading_calendar)

    dts = pd.date_range(start=cu.convert_doubledate_2datetime(date_from),
                    end=cu.convert_doubledate_2datetime(date_to), freq=bday_us)

    dts = dts[dts.dayofweek==2]

    return {'date_time_dates': dts,
            'double_dates': [int(x.strftime('%Y%m%d')) for x in dts]}

def get_equal_length_bucket_limits(**kwargs):

    min_value = kwargs['min_value']
    max_value = kwargs['max_value']
    num_buckets = kwargs['num_buckets']

    bucket_step = (max_value-min_value)/num_buckets

    return np.arange(min_value+bucket_step,max_value,bucket_step)






