

import opportunity_constructs.utilities as opUtil
import signals.utils as sutil
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import shared.directory_names as dn
import shared.statistics as stats
import shared.calendar_utilities as cu
import ta.strategy as ts
import os.path
import datetime as dt
import pandas as pd
import numpy as np


def get_data4datelist(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_list = kwargs['date_list']

    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]

    if 'spread_weights' in kwargs.keys():
        spread_weights = kwargs['spread_weights']
    else:
        weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
        spread_weights = weights_output['spread_weights']

    num_contracts = len(ticker_list)

    ticker_class_list = [cmi.ticker_class[x] for x in ticker_head_list]

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=ticker_list,
                                       date_list=date_list)

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())
    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]

    end_hour = min([cmi.last_trade_hour_minute[x] for x in ticker_head_list])
    start_hour = max([cmi.first_trade_hour_minute[x] for x in ticker_head_list])

    if 'Ag' in ticker_class_list:
        start_hour1 = dt.time(0, 45, 0, 0)
        end_hour1 = dt.time(7, 45, 0, 0)
        selection_indx = [x for x in range(len(intraday_data.index)) if
                          ((intraday_data['time_stamp'].iloc[x].time() < end_hour1)
                           and(intraday_data['time_stamp'].iloc[x].time() >= start_hour1)) or
                          ((intraday_data['time_stamp'].iloc[x].time() < end_hour)
                           and(intraday_data['time_stamp'].iloc[x].time() >= start_hour))]

    else:
        selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    intraday_data = intraday_data.iloc[selection_indx]
    intraday_data['spread'] = 0

    for i in range(num_contracts):
        intraday_data['c' + str(i+1), 'mid_p'] = (intraday_data['c' + str(i+1)]['best_bid_p'] +
                                         intraday_data['c' + str(i+1)]['best_ask_p'])/2

        intraday_data['spread'] = intraday_data['spread']+intraday_data['c' + str(i+1)]['mid_p']*spread_weights[i]

    return intraday_data

