
import opportunity_constructs.utilities as oputil
import signals.technical_indicators as ti
import shared.calendar_utilities as cu
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import pandas as pd
import numpy as np


def get_technical_scalper_4ticker2(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    datetime_to = cu.convert_doubledate_2datetime(date_to)
    daily_settles = gfp.get_futures_price_preloaded(ticker=ticker)
    daily_settles = daily_settles[daily_settles['settle_date'] <= datetime_to]
    daily_settles['close_price_daily_diff'] = daily_settles['close_price']-daily_settles['close_price'].shift(1)
    daily_noise = np.std(daily_settles['close_price_daily_diff'].iloc[-60:])

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    ticker_class = cmi.ticker_class[ticker_head]
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    intraday_data = oputil.get_clean_intraday_data(ticker=ticker,date_to=date_to,freq_str='S',num_days_back=0)

    hloc_data = intraday_data['mid_p'].resample('T', how='ohlc')
    hloc_data = hloc_data[hloc_data['close'].notnull()]

    longer_stochastic = ti.stochastic(data_frame_input=hloc_data, p1=25, p2=3, p3=3)

    hloc_data = ti.stochastic(data_frame_input=hloc_data, p1=5, p2=3, p3=3)

    hloc_data['D1_Long'] = longer_stochastic['D1']
    hloc_data['D2_Long'] = longer_stochastic['D2']

    hloc_data['D1_Lag'] = hloc_data['D1'].shift(1)
    hloc_data['D2_Lag'] = hloc_data['D2'].shift(1)

    hloc_data['close_lead'] = hloc_data['close'].shift(-1)
    hloc_data['close_lead2'] = hloc_data['close'].shift(-2)
    hloc_data['close_lead5'] = hloc_data['close'].shift(-5)

    hloc_data['CloseChange15'] = hloc_data['close'].shift(-16)-hloc_data['close_lead']

    hloc_data['ewma50'] = pd.ewma(hloc_data['close'], span=50)
    hloc_data['ewma100'] = pd.ewma(hloc_data['close'], span=100)

    hloc_data['ma25'] = pd.rolling_mean(hloc_data['close'], 25)
    hloc_data['ma50'] = pd.rolling_mean(hloc_data['close'], 50)

    hloc_data['high_Lag'] = hloc_data['high'].shift(1)
    hloc_data['low_Lag'] = hloc_data['low'].shift(1)

    hloc_data['long_stop'] = hloc_data[['low', 'low_Lag']].min(axis=1)
    hloc_data['short_stop'] = hloc_data[['high', 'high_Lag']].max(axis=1)

    hloc_data['hour_minute'] = [100*x.hour+x.minute for x in hloc_data.index]

    hloc_data['bullish_stochastic_crossover'] = False
    hloc_data['bearish_stochastic_crossover'] = False

    hloc_data['e_bullish_stochastic_crossover'] = False
    hloc_data['e_bearish_stochastic_crossover'] = False

    bullish_co_index = (hloc_data['D1_Lag']<hloc_data['D2_Lag'])&(hloc_data['D1']>hloc_data['D2'])
    bearish_co_index = (hloc_data['D1_Lag']>hloc_data['D2_Lag'])&(hloc_data['D1']<hloc_data['D2'])

    hloc_data.loc[bullish_co_index,'bullish_stochastic_crossover'] = True
    hloc_data.loc[bearish_co_index,'bearish_stochastic_crossover'] = True

    e_bullish_co_index = hloc_data['bullish_stochastic_crossover']&(hloc_data['D1_Lag'] <= 20)
    e_bearish_co_index = hloc_data['bearish_stochastic_crossover']&(hloc_data['D1_Lag'] >= 80)

    hloc_data.loc[e_bullish_co_index,'e_bullish_stochastic_crossover'] = True
    hloc_data.loc[e_bearish_co_index,'e_bearish_stochastic_crossover'] = True

    hloc_data['daily_trend_direction'] = 0

    hloc_data.loc[hloc_data['D1_Long']>hloc_data['D2_Long'], 'daily_trend_direction'] = 1
    hloc_data.loc[hloc_data['D1_Long']<hloc_data['D2_Long'], 'daily_trend_direction'] = -1

    if ticker_class in ['Ag', 'Livestock']:
        tradeable_data = hloc_data[hloc_data['hour_minute'] >= 1000]
    else:
        tradeable_data = hloc_data[hloc_data['hour_minute'] >= 930]

    trend_long_indx = (tradeable_data['daily_trend_direction']==1)&(tradeable_data['e_bullish_stochastic_crossover'])
    trend_short_indx = (tradeable_data['daily_trend_direction']==-1)&(tradeable_data['e_bearish_stochastic_crossover'])

    range_long_indx = (tradeable_data['e_bullish_stochastic_crossover'])
    range_short_indx = (tradeable_data['e_bearish_stochastic_crossover'])

    #range_long_indx = tradeable_data['D1']>tradeable_data['D2']
    #range_short_indx = tradeable_data['D1']<tradeable_data['D2']

    trend_long_indx = (tradeable_data['D1']>tradeable_data['D2']+10)&(tradeable_data['daily_trend_direction']==1)
    trend_short_indx = (tradeable_data['D1']<tradeable_data['D2']+10)&(tradeable_data['daily_trend_direction']==-1)

    indx_list = [range_long_indx,range_short_indx,trend_long_indx, trend_short_indx]
    trade_type_list = ['range_long','range_short','trend_long', 'trend_short']

    trade_frame_list = []

    for i in range(len(indx_list)):
        if sum(indx_list[i]) > 0:
            trade_frame = tradeable_data[indx_list[i]]
            trade_frame['trade_type'] = trade_type_list[i]
            if trade_type_list[i] in ['trend_long','range_long']:
                trade_frame['trade_direction'] = 1
            else:
                trade_frame['trade_direction'] = -1
            trade_frame_list.append(trade_frame)

    if len(trade_frame_list) > 0:
        merged_frame = pd.concat(trade_frame_list)
        merged_frame['ticker'] = ticker
        merged_frame['ticker_head'] = ticker_head
        merged_frame['ticker_class'] = ticker_class
        merged_frame['contract_multiplier'] = contract_multiplier
        merged_frame['daily_noise'] = daily_noise

        merged_frame['NormPnl15'] = (5000/daily_noise)*merged_frame['CloseChange15']*merged_frame['trade_direction']
        merged_frame['Qty'] = 5000/(daily_noise*contract_multiplier)

        merged_frame['NormPnl15WSRobust'] = merged_frame['NormPnl15']
        merged_frame['Holding_Period'] = 15

        for i in range(len(merged_frame.index)):

            trade_i = merged_frame.iloc[i]

            post_entry = hloc_data[hloc_data.index>merged_frame.index[i]]
            post_entry = post_entry.iloc[:15]

            if post_entry.empty:
                continue

            if trade_i['trade_direction'] == 1:
                stop_indx1 = post_entry['low'] < trade_i['long_stop']
                stop_indx2 = post_entry['D1'] < post_entry['D2']

                if sum(stop_indx1) > 0:
                    stop_point1 = (post_entry.index[stop_indx1])[0]
                else:
                    stop_point1 = post_entry.index[-1]

                if sum(stop_indx2) > 0:
                    stop_point2 = (post_entry.index[stop_indx2])[0]
                else:
                    stop_point2 = post_entry.index[-1]

                stop_point = min(stop_point1, stop_point2)
                #stop_point = stop_point2

                merged_frame['NormPnl15WSRobust'].iloc[i] = (5000/daily_noise)*(post_entry['close'].loc[stop_point]-trade_i['close'])*trade_i['trade_direction']
                merged_frame['Holding_Period'].iloc[i] = ((stop_point-merged_frame.index[i]).total_seconds()/60)

            if trade_i['trade_direction'] == -1:

                stop_indx1 = post_entry['high'] > trade_i['short_stop']
                stop_indx2 = post_entry['D1'] > post_entry['D2']

                if sum(stop_indx1) > 0:
                    stop_point1 = (post_entry.index[stop_indx1])[0]
                else:
                    stop_point1 = post_entry.index[-1]

                if sum(stop_indx2) > 0:
                    stop_point2 = (post_entry.index[stop_indx2])[0]
                else:
                    stop_point2 = post_entry.index[-1]

                stop_point = min(stop_point1, stop_point2)
                #stop_point = stop_point2
                merged_frame['Holding_Period'].iloc[i] = ((stop_point-merged_frame.index[i]).total_seconds()/60)
                merged_frame['NormPnl15WSRobust'].iloc[i] = (5000/daily_noise)*(post_entry['close'].loc[stop_point]-trade_i['close'])*trade_i['trade_direction']

        merged_frame['NormPnl15WSRobustPerContract'] = merged_frame['NormPnl15WSRobust']/merged_frame['Qty']
    else:
        merged_frame = pd.DataFrame()

    return {'success': True, 'hloc_data': hloc_data,'trade_data':merged_frame}


def get_technical_scalper_4ticker(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    #print(ticker)
    datetime_to = cu.convert_doubledate_2datetime(date_to)
    daily_settles = gfp.get_futures_price_preloaded(ticker=ticker)
    daily_settles = daily_settles[daily_settles['settle_date'] <= datetime_to]
    daily_settles['close_price_daily_diff'] = daily_settles['close_price']-daily_settles['close_price'].shift(1)

    yesterdays_high = daily_settles['high_price'].iloc[-2]
    yesterdays_low = daily_settles['low_price'].iloc[-2]

    daily_noise = np.std(daily_settles['close_price_daily_diff'].iloc[-60:])
    average_volume = np.mean(daily_settles['volume'].iloc[-20:])

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    ticker_class = cmi.ticker_class[ticker_head]
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    intraday_data = oputil.get_clean_intraday_data(ticker=ticker,date_to=date_to,freq_str='S',num_days_back=3)
    morning_data = intraday_data[(intraday_data['hour_minute']>=830)&(intraday_data['hour_minute']<=900)&(intraday_data['settle_date']==datetime_to.date())]

    if morning_data.empty:
        return {'success': False, 'trade_data': pd.DataFrame()}

    morning_low = min(morning_data['mid_p'])
    morning_high = max(morning_data['mid_p'])

    #return {'morning_low':morning_low,'morning_high':morning_high}

    hloc_data = intraday_data['mid_p'].resample('5T', how='ohlc')
    volume_data = intraday_data['total_traded_q'].resample('5T', how='ohlc')

    hloc_data['volume'] = volume_data['close']

    hloc_data['close_lead'] = hloc_data['close'].shift(-1)

    hloc_data = hloc_data[hloc_data['close'].notnull()]

    hloc_data['ewma10'] = pd.ewma(hloc_data['close'], span=10)
    hloc_data['ewma20'] = pd.ewma(hloc_data['close'], span=20)
    hloc_data['ewma50'] = pd.ewma(hloc_data['close'], span=50)
    hloc_data['ewma100'] = pd.ewma(hloc_data['close'], span=100)
    hloc_data['lma240'] = pd.rolling_window(hloc_data['close'],window=list(range(240)),mean=True)

    hloc_data['ma10'] = pd.rolling_mean(hloc_data['close'], 10)
    hloc_data['ma20'] = pd.rolling_mean(hloc_data['close'], 20)
    hloc_data['ma50'] = pd.rolling_mean(hloc_data['close'], 50)
    hloc_data['ma100'] = pd.rolling_mean(hloc_data['close'], 100)

    hloc_data = ti.stochastic(data_frame_input=hloc_data, p1=14, p2=3, p3=3)

    hloc_data['D1_Lag'] = hloc_data['D1'].shift(1)
    hloc_data['D2_Lag'] = hloc_data['D2'].shift(1)

    hloc_data['ma10_Lag'] = hloc_data['ma10'].shift(1)
    hloc_data['ewma10_Lag'] = hloc_data['ewma10'].shift(1)
    hloc_data['ewma20_Lag'] = hloc_data['ewma20'].shift(1)
    hloc_data['ewma50_Lag'] = hloc_data['ewma50'].shift(1)
    hloc_data['ewma100_Lag'] = hloc_data['ewma100'].shift(1)
    hloc_data['lma240_Lag'] = hloc_data['lma240'].shift(1)

    hloc_data['close_Lag'] = hloc_data['close'].shift(1)

    hloc_data['CloseChange15'] = hloc_data['close'].shift(-3)-hloc_data['close']
    hloc_data['CloseChange60'] = hloc_data['close'].shift(-12)-hloc_data['close']

    hloc_data['CloseChange_15'] = hloc_data['close']-hloc_data['close'].shift(3)
    hloc_data['CloseChange_60'] = hloc_data['close']-hloc_data['close'].shift(12)

    hloc_data['hourly_normalized_volume'] = (hloc_data['volume']-hloc_data['volume'].shift(12))/average_volume

    hloc_data['high_Lag'] = hloc_data['high'].shift(1)
    hloc_data['low_Lag'] = hloc_data['low'].shift(1)

    hloc_data['long_stop'] = hloc_data[['low', 'low_Lag']].min(axis=1)
    hloc_data['short_stop'] = hloc_data[['high', 'high_Lag']].max(axis=1)

    hloc_data['long_stop_dist'] = hloc_data['long_stop']-hloc_data['close']
    hloc_data['short_stop_dist'] = hloc_data['short_stop']-hloc_data['close']

    hloc_data['hour_minute'] = [100*x.hour+x.minute for x in hloc_data.index]
    hloc_data['settle_date'] = [x.to_datetime().date() for x in hloc_data.index]

    hloc_data['bullish_stochastic_crossover'] = False
    hloc_data['bearish_stochastic_crossover'] = False

    hloc_data['e_bullish_stochastic_crossover'] = False
    hloc_data['e_bearish_stochastic_crossover'] = False

    hloc_data['m_bullish_stochastic_crossover'] = False
    hloc_data['m_bearish_stochastic_crossover'] = False

    hloc_data['bullish_stochastic_zone'] = False
    hloc_data['bearish_stochastic_zone'] = False

    bullish_co_index = (hloc_data['D1_Lag']<hloc_data['D2_Lag'])&(hloc_data['D1']>hloc_data['D2'])
    bearish_co_index = (hloc_data['D1_Lag']>hloc_data['D2_Lag'])&(hloc_data['D1']<hloc_data['D2'])

    hloc_data.loc[bullish_co_index,'bullish_stochastic_crossover'] = True
    hloc_data.loc[bearish_co_index,'bearish_stochastic_crossover'] = True

    e_bullish_co_index = hloc_data['bullish_stochastic_crossover']&(hloc_data['D1_Lag'] <= 20)
    e_bearish_co_index = hloc_data['bearish_stochastic_crossover']&(hloc_data['D1_Lag'] >= 80)

    m_bullish_co_index = hloc_data['bullish_stochastic_crossover']&(hloc_data['D1_Lag'] <= 50)
    m_bearish_co_index = hloc_data['bearish_stochastic_crossover']&(hloc_data['D1_Lag'] >= 50)

    hloc_data.loc[e_bullish_co_index,'e_bullish_stochastic_crossover'] = True
    hloc_data.loc[e_bearish_co_index,'e_bearish_stochastic_crossover'] = True

    hloc_data.loc[m_bullish_co_index,'m_bullish_stochastic_crossover'] = True
    hloc_data.loc[m_bearish_co_index,'m_bearish_stochastic_crossover'] = True

    hloc_data['daily_trend_direction'] = 0

    hloc_data.loc[(hloc_data['ewma50']>hloc_data['ewma100'])&(hloc_data['ewma100']>hloc_data['lma240']), 'daily_trend_direction'] = 1
    hloc_data.loc[(hloc_data['ewma50']<hloc_data['ewma100'])&(hloc_data['ewma100']<hloc_data['lma240']), 'daily_trend_direction'] = -1

    hloc_data = hloc_data[(hloc_data['settle_date'] == datetime_to.date())]

    morning_data = hloc_data[hloc_data['hour_minute']<800]
    morning_average = morning_data['close'].mean()

    hloc_data['ma10Hybrid'] = (hloc_data['ma10']+morning_average)/2
    hloc_data['ma20Hybrid'] = (hloc_data['ma20']+morning_average)/2

    hloc_data['ticker'] = ticker
    hloc_data['tickerHead'] = ticker_head
    hloc_data['tickerClass'] = ticker_class

    hloc_data['ts_slope5'] = np.nan
    hloc_data['ts_slope10'] = np.nan
    hloc_data['ts_slope20'] = np.nan

    hloc_data['linear_deviation5'] = np.nan
    hloc_data['linear_deviation10'] = np.nan
    hloc_data['linear_deviation20'] = np.nan

    for i in range(5, len(hloc_data.index)+1):

        if i >= 5:
            ts_regression_output5 = ti.time_series_regression(data_frame_input=hloc_data.iloc[:i], num_obs=5, y_var_name='close')
            hloc_data['ts_slope5'].iloc[i-1] = ts_regression_output5['beta']/daily_noise
            hloc_data['linear_deviation5'].iloc[i-1] = ts_regression_output5['zscore']

        if i >= 10:
            ts_regression_output10 = ti.time_series_regression(data_frame_input=hloc_data.iloc[:i], num_obs=10, y_var_name='close')
            hloc_data['ts_slope10'].iloc[i-1] = ts_regression_output10['beta']/daily_noise
            hloc_data['linear_deviation10'].iloc[i-1] = ts_regression_output10['zscore']

        if i >= 20:
            ts_regression_output20 = ti.time_series_regression(data_frame_input=hloc_data.iloc[:i], num_obs=20, y_var_name='close')
            hloc_data['ts_slope20'].iloc[i-1] = ts_regression_output20['beta']/daily_noise
            hloc_data['linear_deviation20'].iloc[i-1] = ts_regression_output20['zscore']

    if ticker_class == 'Ag':
        tradeable_data = hloc_data[hloc_data['hour_minute'] >= 945]
    else:
        tradeable_data = hloc_data[hloc_data['hour_minute'] >= 930]

    tradeable_data['NormPnl60'] = (5000/daily_noise)*tradeable_data['CloseChange60']
    tradeable_data['NormPnl15'] = (5000/daily_noise)*tradeable_data['CloseChange15']
    tradeable_data['NormPnl60_'] = -tradeable_data['NormPnl60']

    tradeable_data['PerContractPnl60'] = contract_multiplier*tradeable_data['CloseChange60']
    tradeable_data['PerContractPnl15'] = contract_multiplier*tradeable_data['CloseChange15']

    tradeable_data['ma10_spread'] = (tradeable_data['close']-hloc_data['ma10'])/daily_noise
    tradeable_data['ma20_spread'] = (tradeable_data['close']-hloc_data['ma20'])/daily_noise
    tradeable_data['ma50_spread'] = (tradeable_data['close']-hloc_data['ma50'])/daily_noise
    tradeable_data['ma100_spread'] = (tradeable_data['close']-hloc_data['ma100'])/daily_noise

    tradeable_data['ma10Hybrid_spread'] = (tradeable_data['close']-hloc_data['ma10Hybrid'])/daily_noise
    tradeable_data['ma20Hybrid_spread'] = (tradeable_data['close']-hloc_data['ma20Hybrid'])/daily_noise
    tradeable_data['morning_spread'] = (tradeable_data['close']-morning_average)/daily_noise
    tradeable_data['NormCloseChange_15'] = tradeable_data['CloseChange_15']/daily_noise
    tradeable_data['NormCloseChange_60'] = tradeable_data['CloseChange_60']/daily_noise

    trend_long_indx = (tradeable_data['close'] > tradeable_data['ma10'])&(tradeable_data['ma50_spread']<-0.4)&(tradeable_data['hourly_normalized_volume'] < 0.25)
    trend_short_indx = (tradeable_data['close'] < tradeable_data['ma10'])&(tradeable_data['ma50_spread']>0.4)&(tradeable_data['hourly_normalized_volume'] < 0.25)
    range_long_indx = (tradeable_data['close'] > tradeable_data['ma10'])&(tradeable_data['close_Lag'] < tradeable_data['ma10_Lag'])&(tradeable_data['ma50_spread']<-0.4)
    range_short_indx = (tradeable_data['close'] < tradeable_data['ma10'])&(tradeable_data['close_Lag'] > tradeable_data['ma10_Lag'])&(tradeable_data['ma50_spread']>0.4)
    ma_long_index = (tradeable_data['close'] > tradeable_data['ma10'])&(tradeable_data['close_Lag'] < tradeable_data['ma10_Lag'])&(tradeable_data['ma50_spread']<-0.4)&(tradeable_data['hourly_normalized_volume'] < 0.25)
    ma_short_index = (tradeable_data['close'] < tradeable_data['ma10'])&(tradeable_data['close_Lag'] > tradeable_data['ma10_Lag'])&(tradeable_data['ma50_spread']>0.4)&(tradeable_data['hourly_normalized_volume'] < 0.25)

    indx_list = [trend_long_indx, trend_short_indx, range_long_indx, range_short_indx, ma_long_index, ma_short_index]
    trade_type_list = ['trend_long', 'trend_short', 'range_long', 'range_short', 'ma_long', 'ma_short']

    trade_frame_list = []

    for i in range(len(indx_list)):
        if sum(indx_list[i]) > 0:
            trade_frame = tradeable_data[indx_list[i]]
            trade_frame['trade_type'] = trade_type_list[i]
            if trade_type_list[i] in ['trend_long','range_long','ma_long']:
                trade_frame['trade_direction'] = 1
            else:
                trade_frame['trade_direction'] = -1
            trade_frame_list.append(trade_frame)

    if len(trade_frame_list) > 0:
        merged_frame = pd.concat(trade_frame_list)
        merged_frame['ticker'] = ticker
        merged_frame['ticker_head'] = ticker_head
        merged_frame['ticker_class'] = ticker_class
        merged_frame['contract_multiplier'] = contract_multiplier
        merged_frame['daily_noise'] = daily_noise

        merged_frame['Holding_Period'] = 60

        merged_frame['NormPnl15'] = merged_frame['NormPnl15']*merged_frame['trade_direction']
        merged_frame['NormPnl60'] = merged_frame['NormPnl60']*merged_frame['trade_direction']

        merged_frame['NormPnl60WS'] = merged_frame['NormPnl60']

        for i in range(len(merged_frame.index)):

            trade_i = merged_frame.iloc[i]

            post_entry = hloc_data[hloc_data.index>merged_frame.index[i]]
            post_entry = post_entry.iloc[:12]

            if post_entry.empty:
                continue

            if trade_i['trade_direction'] == 1:
                stop_indx1 = post_entry['low'] < trade_i['long_stop']
                stop_indx2 = post_entry['D1'] < post_entry['D2']

                if sum(stop_indx1) > 0:
                    stop_point1 = (post_entry.index[stop_indx1])[0]
                else:
                    stop_point1 = post_entry.index[-1]

                if sum(stop_indx2) > 0:
                    stop_point2 = (post_entry.index[stop_indx2])[0]
                else:
                    stop_point2 = post_entry.index[-1]

                stop_point = min(stop_point1, stop_point2)
                stop_point = stop_point2

            if trade_i['trade_direction'] == -1:

                stop_indx1 = post_entry['high'] > trade_i['short_stop']
                stop_indx2 = post_entry['D1'] > post_entry['D2']

                if sum(stop_indx1) > 0:
                    stop_point1 = (post_entry.index[stop_indx1])[0]
                else:
                    stop_point1 = post_entry.index[-1]

                if sum(stop_indx2) > 0:
                    stop_point2 = (post_entry.index[stop_indx2])[0]
                else:
                    stop_point2 = post_entry.index[-1]

                stop_point = min(stop_point1, stop_point2)
                stop_point = stop_point2

            merged_frame['NormPnl60WS'].iloc[i] = (5000/daily_noise)*(post_entry['close'].loc[stop_point]-trade_i['close'])*trade_i['trade_direction']
            merged_frame['Holding_Period'].iloc[i] = ((stop_point-merged_frame.index[i]).total_seconds()/60)

            merged_frame['NormPnl60WSPerContract'] = merged_frame['NormPnl60WS']/(5000/(contract_multiplier*daily_noise))


    else:
        merged_frame = pd.DataFrame()

    return {'success': True, 'tradeable_data': tradeable_data,'trade_data':merged_frame}


