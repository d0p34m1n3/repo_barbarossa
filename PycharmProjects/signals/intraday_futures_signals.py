
import signals.utils as sutil
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_lists as cl
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import shared.statistics as stats
import pandas as pd
import numpy as np
import pytz as pytz
import datetime as dt
import time as tm
central_zone = pytz.timezone('US/Central')

fixed_weight_future_spread_list = [['CL', 'HO'], ['CL', 'RB'],
                                   ['HO', 'CL'], ['RB', 'CL'],
                                   ['HO', 'RB', 'CL'],
                                   ['B', 'CL'], ['CL', 'B'],
                                   ['S', 'BO', 'SM'],
                                   ['C', 'W'], ['W', 'C'],
                                   ['W', 'KW'], ['KW', 'W']]

def get_intraday_spread_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    #print(ticker_list)


    ticker_list = [x for x in ticker_list if x is not None]
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    ticker_class_list = [cmi.ticker_class[x] for x in ticker_head_list]

    #print('-'.join(ticker_list))

    if 'tr_dte_list' in kwargs.keys():
        tr_dte_list = kwargs['tr_dte_list']
    else:
        tr_dte_list = [exp.get_days2_expiration(ticker=x,date_to=date_to, instrument='futures')['tr_dte'] for x in ticker_list]

    if 'aggregation_method' in kwargs.keys() and 'contracts_back' in kwargs.keys():
        aggregation_method = kwargs['aggregation_method']
        contracts_back = kwargs['contracts_back']
    else:

        amcb_output = [opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(x)) for x in ticker_list]
        aggregation_method = max([x['aggregation_method'] for x in amcb_output])
        contracts_back = min([x['contracts_back'] for x in amcb_output])

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in list(set(ticker_head_list))}

    if 'use_last_as_current' in kwargs.keys():
        use_last_as_current = kwargs['use_last_as_current']
    else:
        use_last_as_current = True

    if 'datetime5_years_ago' in kwargs.keys():
        datetime5_years_ago = kwargs['datetime5_years_ago']
    else:
        date5_years_ago = cu.doubledate_shift(date_to,5*365)
        datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    if 'num_days_back_4intraday' in kwargs.keys():
        num_days_back_4intraday = kwargs['num_days_back_4intraday']
    else:
        num_days_back_4intraday = 10

    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]

    aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_list,
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=use_last_as_current)

    aligned_data = aligned_output['aligned_data']
    current_data = aligned_output['current_data']

    if ticker_head_list in fixed_weight_future_spread_list:
        weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
        spread_weights = weights_output['spread_weights']
        portfolio_weights = weights_output['portfolio_weights']
    else:
        regress_output = stats.get_regression_results({'x':aligned_data['c2']['change_1'][-60:], 'y':aligned_data['c1']['change_1'][-60:]})
        spread_weights = [1, -regress_output['beta']]
        portfolio_weights = [1, -regress_output['beta']*contract_multiplier_list[0]/contract_multiplier_list[1]]


    aligned_data['spread'] = 0
    aligned_data['spread_pnl_1'] = 0
    aligned_data['spread_pnl1'] = 0
    spread_settle = 0

    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago

    num_contracts = len(ticker_list)

    for i in range(num_contracts):
        aligned_data['spread'] = aligned_data['spread']+aligned_data['c' + str(i+1)]['close_price']*spread_weights[i]
        spread_settle = spread_settle + current_data['c' + str(i+1)]['close_price']*spread_weights[i]
        aligned_data['spread_pnl_1'] = aligned_data['spread_pnl_1']+aligned_data['c' + str(i+1)]['change_1']*portfolio_weights[i]*contract_multiplier_list[i]
        aligned_data['spread_pnl1'] = aligned_data['spread_pnl1']+aligned_data['c' + str(i+1)]['change1_instant']*portfolio_weights[i]*contract_multiplier_list[i]

    aligned_data['spread_normalized'] = aligned_data['spread']/aligned_data['c1']['close_price']

    data_last5_years = aligned_data[last5_years_indx]

    percentile_vector = stats.get_number_from_quantile(y=data_last5_years['spread_pnl_1'].values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(data_last5_years.index)/4)))

    downside = (percentile_vector[0]+percentile_vector[1])/2
    upside = (percentile_vector[2]+percentile_vector[3])/2

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=x) for x in reversed(range(1,num_days_back_4intraday))]
    date_list.append(date_to)

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=ticker_list,
                                       date_list=date_list)

    if len(intraday_data.index)==0:
        return {'downside': downside, 'upside': upside,'intraday_data': intraday_data,'trading_data': intraday_data,
            'spread_weight': spread_weights[1], 'portfolio_weight':portfolio_weights[1],
            'z': np.nan,'recent_trend': np.nan,
            'intraday_mean10': np.nan, 'intraday_std10': np.nan,
            'intraday_mean5': np.nan, 'intraday_std5': np.nan,
            'intraday_mean2': np.nan, 'intraday_std2': np.nan,
            'intraday_mean1': np.nan, 'intraday_std1': np.nan,
            'aligned_output': aligned_output, 'spread_settle': spread_settle,
            'data_last5_years': data_last5_years,
            'ma_spread_lowL': np.nan,'ma_spread_highL': np.nan,
            'ma_spread_low': np.nan,'ma_spread_high': np.nan,
            'intraday_sharp': np.nan}

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())

    end_hour = min([cmi.last_trade_hour_minute[x] for x in ticker_head_list])
    start_hour = max([cmi.first_trade_hour_minute[x] for x in ticker_head_list])

    trade_start_hour = dt.time(9, 30, 0, 0)

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
    intraday_data['spread1'] = np.nan

    for i in range(len(unique_settle_dates)-1):
        if (intraday_data['settle_date'] == unique_settle_dates[i]).sum() == \
                (intraday_data['settle_date'] == unique_settle_dates[i+1]).sum():
            intraday_data.loc[intraday_data['settle_date'] == unique_settle_dates[i],'spread1'] = \
                intraday_data['spread'][intraday_data['settle_date'] == unique_settle_dates[i+1]].values

    intraday_data = intraday_data[intraday_data['settle_date'].notnull()]

    intraday_mean10 = intraday_data['spread'].mean()
    intraday_std10 = intraday_data['spread'].std()

    intraday_data_last5days = intraday_data[intraday_data['settle_date'] >= cu.convert_doubledate_2datetime(date_list[-5]).date()]
    intraday_data_last2days = intraday_data[intraday_data['settle_date'] >= cu.convert_doubledate_2datetime(date_list[-2]).date()]
    intraday_data_yesterday = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-1]).date()]

    intraday_mean5 = intraday_data_last5days['spread'].mean()
    intraday_std5 = intraday_data_last5days['spread'].std()

    intraday_mean2 = intraday_data_last2days['spread'].mean()
    intraday_std2 = intraday_data_last2days['spread'].std()

    intraday_mean1 = intraday_data_yesterday['spread'].mean()
    intraday_std1 = intraday_data_yesterday['spread'].std()

    intraday_z = (spread_settle-intraday_mean5)/intraday_std5

    num_obs_intraday = len(intraday_data.index)
    num_obs_intraday_half = round(num_obs_intraday/2)
    intraday_tail = intraday_data.tail(num_obs_intraday_half)

    num_positives = sum(intraday_tail['spread'] > intraday_data['spread'].mean())
    num_negatives = sum(intraday_tail['spread'] < intraday_data['spread'].mean())

    if num_positives+num_negatives!=0:
        recent_trend = 100*(num_positives-num_negatives)/(num_positives+num_negatives)
    else:
        recent_trend = np.nan

    intraday_data_shifted = intraday_data.groupby('settle_date').shift(-60)
    intraday_data['spread_shifted'] = intraday_data_shifted['spread']
    intraday_data['delta60'] = intraday_data['spread_shifted']-intraday_data['spread']

    intraday_data['ewma10'] = pd.ewma(intraday_data['spread'], span=10)
    intraday_data['ewma50'] = pd.ewma(intraday_data['spread'], span=50)
    intraday_data['ewma200'] = pd.ewma(intraday_data['spread'], span=200)

    intraday_data['ma40'] = pd.rolling_mean(intraday_data['spread'], 40)

    intraday_data['ewma50_spread'] = intraday_data['spread']-intraday_data['ewma50']
    intraday_data['ma40_spread'] = intraday_data['spread']-intraday_data['ma40']

    selection_indx = [x for x in range(len(intraday_data.index)) if (intraday_data['time_stamp'].iloc[x].time() > trade_start_hour)]
    selected_data = intraday_data.iloc[selection_indx]
    selected_data['delta60Net'] = (contract_multiplier_list[0]*selected_data['delta60']/spread_weights[0])

    selected_data.reset_index(drop=True,inplace=True)
    selected_data['proxy_pnl'] = 0

    t_cost = cmi.t_cost[ticker_head_list[0]]

    ma_spread_low = np.nan
    ma_spread_high = np.nan
    ma_spread_lowL = np.nan
    ma_spread_highL = np.nan
    intraday_sharp = np.nan

    if sum(selected_data['ma40_spread'].notnull())>30:
        quantile_list = selected_data['ma40_spread'].quantile([0.1,0.9])

        down_indx = selected_data['ma40_spread']<quantile_list[0.1]
        up_indx = selected_data['ma40_spread']>quantile_list[0.9]

        up_data = selected_data[up_indx]
        down_data = selected_data[down_indx]

        ma_spread_lowL = quantile_list[0.1]
        ma_spread_highL = quantile_list[0.9]

        #return {'selected_data':selected_data,'up_data':up_data,'up_indx':up_indx}

        selected_data.loc[up_indx,'proxy_pnl'] = (-up_data['delta60Net']-2*num_contracts*t_cost).values
        selected_data.loc[down_indx,'proxy_pnl'] = (down_data['delta60Net']-2*num_contracts*t_cost).values

        short_term_data = selected_data[selected_data['settle_date'] >= cu.convert_doubledate_2datetime(date_list[-5]).date()]
        if sum(short_term_data['ma40_spread'].notnull())>30:
            quantile_list = short_term_data['ma40_spread'].quantile([0.1,0.9])
            ma_spread_low = quantile_list[0.1]
            ma_spread_high = quantile_list[0.9]

        if selected_data['proxy_pnl'].std()!=0:
            intraday_sharp = selected_data['proxy_pnl'].mean()/selected_data['proxy_pnl'].std()

    return {'downside': downside, 'upside': upside,'intraday_data': intraday_data,'trading_data': selected_data,
            'spread_weight': spread_weights[1], 'portfolio_weight':portfolio_weights[1],
            'z': intraday_z,'recent_trend': recent_trend,
            'intraday_mean10': intraday_mean10, 'intraday_std10': intraday_std10,
            'intraday_mean5': intraday_mean5, 'intraday_std5': intraday_std5,
            'intraday_mean2': intraday_mean2, 'intraday_std2': intraday_std2,
            'intraday_mean1': intraday_mean1, 'intraday_std1': intraday_std1,
            'aligned_output': aligned_output, 'spread_settle': spread_settle,
            'data_last5_years': data_last5_years,
            'ma_spread_lowL': ma_spread_lowL,'ma_spread_highL': ma_spread_highL,
            'ma_spread_low': ma_spread_low,'ma_spread_high': ma_spread_high,
            'intraday_sharp': intraday_sharp}


def get_intraday_outright_covariance(**kwargs):

    date_to = kwargs['date_to']
    num_days_back_4intraday = 20

    liquid_futures_frame = cl.get_liquid_outright_futures_frame(settle_date=date_to)

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=x) for x in reversed(range(1,num_days_back_4intraday))]
    date_list.append(date_to)
    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=liquid_futures_frame['ticker'].values,date_list=date_list)

    if len(intraday_data.index)==0:
        return {'cov_matrix': pd.DataFrame(), 'cov_data_integrity': 0}


    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]
    intraday_data = intraday_data.resample('30min',how='last')
    intraday_data = intraday_data[(intraday_data['hour_minute'] >= 830) &
                                  (intraday_data['hour_minute'] <= 1200)]
    intraday_data_shifted = intraday_data.shift(1)
    selection_indx = intraday_data['hour_minute']-intraday_data_shifted['hour_minute'] > 0

    num_contracts = len(liquid_futures_frame.index)

    diff_frame = pd.DataFrame()

    for i in range(num_contracts):

        mid_p = (intraday_data['c' + str(i+1)]['best_bid_p']+
                         intraday_data['c' + str(i+1)]['best_ask_p'])/2
        mid_p_shifted = (intraday_data_shifted['c' + str(i+1)]['best_bid_p']+
                                 intraday_data_shifted['c' + str(i+1)]['best_ask_p'])/2

        diff_frame[liquid_futures_frame['ticker_head'].iloc[i]] = (mid_p-mid_p_shifted)*cmi.contract_multiplier[liquid_futures_frame['ticker_head'].iloc[i]]

    diff_frame = diff_frame[selection_indx]

    return {'cov_matrix': diff_frame.cov(),
     'cov_data_integrity': 100*diff_frame.notnull().sum().sum()/(len(diff_frame.columns)*20*6)}










