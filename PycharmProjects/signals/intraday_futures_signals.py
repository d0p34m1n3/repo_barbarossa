
import signals.utils as sutil
import opportunity_constructs.utilities as opUtil
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import shared.statistics as stats
import pandas as pd
import numpy as np
import pytz as pytz
import datetime as dt
central_zone = pytz.timezone('US/Central')


def get_intraday_spread_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    ticker_list = [x for x in ticker_list if x is not None]
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]

    if 'tr_dte_list' in kwargs.keys():
        tr_dte_list = kwargs['tr_dte_list']
    else:
        tr_dte_list = [exp.get_days2_expiration(ticker=x,date_to=date_to, instrument='futures')['tr_dte'] for x in ticker_list]

    weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)

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
        num_days_back_4intraday = 5

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
    spread_weights = weights_output['spread_weights']
    portfolio_weights = weights_output['portfolio_weights']
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

    intraday_data['spread'] = 0

    for i in range(num_contracts):
        intraday_data['c' + str(i+1), 'mid_p'] = (intraday_data['c' + str(i+1)]['best_bid_p'] +
                                         intraday_data['c' + str(i+1)]['best_ask_p'])/2

        intraday_data['spread'] = intraday_data['spread']+intraday_data['c' + str(i+1)]['mid_p']*spread_weights[i]

    intraday_mean = intraday_data['spread'].mean()
    intraday_std = intraday_data['spread'].std()
    intraday_z = (spread_settle-intraday_mean)/intraday_std

    num_obs_intraday = len(intraday_data.index)
    num_obs_intraday_half = round(num_obs_intraday/2)
    intraday_tail = intraday_data.tail(num_obs_intraday_half)

    num_positives = sum(intraday_tail['spread'] > intraday_data['spread'].mean())
    num_negatives = sum(intraday_tail['spread'] < intraday_data['spread'].mean())

    recent_trend = 100*(num_positives-num_negatives)/(num_positives+num_negatives)

    return {'downside': downside, 'upside': upside,'intraday_data': intraday_data,
            'z': intraday_z,'recent_trend': recent_trend,
            'intraday_mean': intraday_mean, 'intraday_std': intraday_std,
            'aligned_output': aligned_output, 'spread_settle': spread_settle}


def get_intraday_trend_signals(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    print(ticker)

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=1)]
    date_list.append(date_to)

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=[ticker],
                                       date_list=date_list)

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]

    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]

    start_hour = dt.time(8, 30, 0, 0, central_zone)
    end_hour = cmi.last_trade_hour_minute[ticker_head]

    selection_indx = [x for x in range(len(intraday_data.index)) if (intraday_data.index[x].to_datetime().time() < end_hour)
                      and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    selected_data = intraday_data.iloc[selection_indx]
    selected_data['mid_p'] = (selected_data['c1']['best_bid_p']+selected_data['c1']['best_ask_p'])/2

    selected_data['ewma100'] = pd.ewma(selected_data['mid_p'], span=100)
    selected_data['ewma25'] = pd.ewma(selected_data['mid_p'], span=25)

    selected_data = selected_data[[x.date() == datetime_to.date() for x in selected_data['time_stamp']]]

    selected_data.reset_index(inplace=True,drop=True)

    first_30_minutes = selected_data[selected_data['hour_minute'] <= 900]
    trading_data = selected_data[selected_data['hour_minute'] > 900]

    range_min = first_30_minutes['mid_p'].min()
    range_max = first_30_minutes['mid_p'].max()

    bullish_breakout = trading_data[trading_data['mid_p'] > range_max]
    bearish_breakout = trading_data[trading_data['mid_p'] < range_min]

    stop_loss = (range_max-range_min)*contract_multiplier

    #return bullish_breakout

    if bullish_breakout.empty and bearish_breakout.empty:
        daily_pnl = 0
    elif bullish_breakout.empty and not bearish_breakout.empty:
        daily_pnl = trading_data['mid_p'].loc[bearish_breakout.index[0]+1]-trading_data['mid_p'].iloc[-1]
    elif bearish_breakout.empty and not bullish_breakout.empty:
        daily_pnl = trading_data['mid_p'].iloc[-1]-trading_data['mid_p'].loc[bullish_breakout.index[0]+1]
    elif bullish_breakout['hour_minute'].iloc[0]<bearish_breakout['hour_minute'].iloc[0]:
        daily_pnl = range_min-trading_data['mid_p'].loc[bullish_breakout.index[0]+1]
    elif bullish_breakout['hour_minute'].iloc[0]>bearish_breakout['hour_minute'].iloc[0]:
        daily_pnl = trading_data['mid_p'].loc[bearish_breakout.index[0]+1]-range_max

    daily_pnl = daily_pnl*contract_multiplier


    return {'intraday_data': selected_data, 'range_min': range_min, 'range_max':range_max,'daily_pnl':daily_pnl,'stop_loss':stop_loss}




