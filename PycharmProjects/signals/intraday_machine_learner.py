

import contract_utilities.expiration as exp
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import pandas as pd
import datetime as dt


def get_intraday_data(**kwargs):
    ticker = kwargs['ticker']
    date_to = kwargs['date_to']
    #print(ticker)

    if 'num_days_back' in kwargs.keys():
        num_days_back = kwargs['num_days_back']
    else:
        num_days_back = 10

    ticker_list = ticker.split('-')

    ticker_head = cmi.get_contract_specs(ticker_list[0])['ticker_head']
    ticker_class = cmi.ticker_class[ticker_head]

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=x) for x in reversed(range(1,num_days_back))]
    date_list.append(date_to)

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=[ticker],
                                       date_list=date_list)

    if intraday_data.empty:
        return pd.DataFrame()

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]

    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]
    intraday_data['settle_date'] = [x.date() for x in intraday_data['time_stamp']]

    end_hour = cmi.last_trade_hour_minute[ticker_head]
    start_hour = cmi.first_trade_hour_minute[ticker_head]

    if ticker_class in ['Ag']:
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

    selected_data = intraday_data.iloc[selection_indx]
    selected_data['mid_p'] = (selected_data['c1']['best_bid_p']+selected_data['c1']['best_ask_p'])/2

    selected_data_shifted = selected_data.groupby('settle_date').shift(-60)
    selected_data['mid_p_shifted'] = selected_data_shifted['mid_p']
    selected_data['mid_p_delta60'] = selected_data['mid_p_shifted']-selected_data['mid_p']
    selected_data['ewma10'] = pd.ewma(selected_data['mid_p'], span=10)
    selected_data['ewma50'] = pd.ewma(selected_data['mid_p'], span=50)
    selected_data['ma40'] = pd.rolling_mean(selected_data['mid_p'], 40)
    selected_data['ma20'] = pd.rolling_mean(selected_data['mid_p'], 20)
    selected_data['ma10'] = pd.rolling_mean(selected_data['mid_p'], 10)

    selected_data['ewma50_spread'] = selected_data['mid_p']-selected_data['ewma50']
    selected_data['ma40_spread'] = selected_data['mid_p']-selected_data['ma40']
    selected_data['ma20_spread'] = selected_data['mid_p']-selected_data['ma20']
    selected_data['ma10_spread'] = selected_data['mid_p']-selected_data['ma10']

    return selected_data


def get_intraday_data_4spread(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    if 'spread_weights' in kwargs.keys():
        spread_weights = kwargs['spread_weights']
    else:
        spread_weights = [1,-1]

    if 'num_days_back' in kwargs.keys():
        num_days_back = kwargs['num_days_back']
    else:
        num_days_back = 10

    num_contracts = len(ticker_list)

    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    ticker_class_list = [cmi.ticker_class[x] for x in ticker_head_list]

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=x) for x in reversed(range(1,num_days_back))]
    date_list.append(date_to)

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=ticker_list,
                                       date_list=date_list)

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]

    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]
    intraday_data['settle_date'] = [x.date() for x in intraday_data['time_stamp']]

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

    selected_data = intraday_data.iloc[selection_indx]

    selected_data['spread'] = 0

    for i in range(num_contracts):
        selected_data['c' + str(i+1), 'mid_p'] = (selected_data['c' + str(i+1)]['best_bid_p'] +
                                         selected_data['c' + str(i+1)]['best_ask_p'])/2

        selected_data['spread'] = selected_data['spread']+selected_data['c' + str(i+1)]['mid_p']*spread_weights[i]

    selected_data_shifted = selected_data.groupby('settle_date').shift(-60)
    selected_data['spread_shifted'] = selected_data_shifted['spread']
    selected_data['delta60'] = selected_data['spread_shifted']-selected_data['spread']
    selected_data['ewma10'] = pd.ewma(selected_data['spread'], span=10)
    selected_data['ewma50'] = pd.ewma(selected_data['spread'], span=50)
    selected_data['ewma200'] = pd.ewma(selected_data['spread'], span=200)

    selected_data['ma40'] = pd.rolling_mean(selected_data['spread'], 40)

    selected_data['ewma50_spread'] = selected_data['spread']-selected_data['ewma50']
    selected_data['ma40_spread'] = selected_data['spread']-selected_data['ma40']

    return selected_data





