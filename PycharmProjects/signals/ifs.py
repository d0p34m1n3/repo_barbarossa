

import opportunity_constructs.utilities as opUtil
import signals.utils as sutil
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import shared.directory_names as dn
import os.path
import datetime as dt
import pandas as pd
import numpy as np


def calc_pnl4date(**kwargs):

    ticker_list = kwargs['ticker_list']
    pnl_date = kwargs['pnl_date']
    #print(pnl_date)

    ticker_list = [x for x in ticker_list if x is not None]
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    ticker_class_list = [cmi.ticker_class[x] for x in ticker_head_list]

    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]

    date_list = [exp.doubledate_shift_bus_days(double_date=pnl_date,shift_in_days=x) for x in [2, 1]]
    date_list.append(pnl_date)

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=ticker_list,
                                       date_list=date_list)

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())
    intraday_data['time'] = intraday_data['time_stamp'].apply(lambda x: x.time())

    weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
    spread_weights = weights_output['spread_weights']

    end_hour = min([cmi.last_trade_hour_minute[x] for x in ticker_head_list])
    start_hour = max([cmi.first_trade_hour_minute[x] for x in ticker_head_list])

    trade_start_hour = dt.time(9, 30, 0, 0)
    num_contracts = len(ticker_list)

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

    unique_settle_dates = intraday_data['settle_date'].unique()

    if len(unique_settle_dates)<3:
        return {'pnl_date': pnl_date, 'total_pnl': np.nan,'long_pnl':np.nan, 'short_pnl': np.nan }

    final_spread_price = np.mean(intraday_data['spread'][(intraday_data['settle_date'] == unique_settle_dates[2])&(intraday_data['time']>=trade_start_hour)])

    calibration_data = intraday_data[(intraday_data['settle_date'] == unique_settle_dates[0])]

    backtest_data = intraday_data[(intraday_data['settle_date'] == unique_settle_dates[1])]
    backtest_data = backtest_data[backtest_data['time']>=trade_start_hour]
    backtest_data['spread_diff'] = contract_multiplier_list[0]*(final_spread_price-backtest_data['spread'])/spread_weights[0]

    calibration_mean = calibration_data['spread'].mean()
    calibration_std = calibration_data['spread'].std()

    cheap_data = backtest_data[backtest_data['spread'] < calibration_mean-1*calibration_std]
    expensive_data = backtest_data[backtest_data['spread'] > calibration_mean+1*calibration_std]

    if cheap_data.empty:
        long_pnl = 0
    else:
        long_pnl = cheap_data['spread_diff'].mean()-2*2*num_contracts

    if expensive_data.empty:
        short_pnl = 0
    else:
        short_pnl = -expensive_data['spread_diff'].mean()-2*2*num_contracts

    return {'pnl_date': pnl_date, 'total_pnl': long_pnl+short_pnl,'long_pnl':long_pnl, 'short_pnl': short_pnl }


def get_pnl_4_date_range(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']
    num_bus_days_back = kwargs['num_bus_days_back']

    directory_name = dn.get_directory_name(ext='backtest_results')

    file_name = '_'.join(ticker_list)

    if os.path.isfile(directory_name + '/ifs_pnls/' + file_name + '.pkl'):
        pnl_frame = pd.read_pickle(directory_name + '/ifs_pnls/' + file_name + '.pkl')
    else:
        pnl_frame = pd.DataFrame(columns=['pnl_date', 'long_pnl', 'short_pnl','total_pnl'])

    date_from = exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=num_bus_days_back)

    date_list = exp.get_bus_day_list(date_from=date_from,date_to=date_to)

    dates2calculate = list(set(date_list)-set(pnl_frame['pnl_date']))

    if not dates2calculate:
        return pnl_frame[(pnl_frame['pnl_date']>=date_list[0])&(pnl_frame['pnl_date']<=date_list[-1])]

    pnl_list = []

    for i in dates2calculate:
        #print(i)
        pnl_list.append(calc_pnl4date(ticker_list=ticker_list,pnl_date=i))

    pnl_frame = pd.concat([pnl_frame,pd.DataFrame(pnl_list)])

    pnl_frame = pnl_frame[['pnl_date', 'long_pnl', 'short_pnl','total_pnl']]
    pnl_frame.sort('pnl_date',ascending=True,inplace=True)
    pnl_frame.to_pickle(directory_name + '/ifs_pnls/' + file_name + '.pkl')

    return pnl_frame[(pnl_frame['pnl_date']>=date_list[0])&(pnl_frame['pnl_date']<=date_list[-1])]













