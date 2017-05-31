
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import fundamental_data.cot_data as cot
import shared.calendar_utilities as cu
import shared.statistics as stats
import signals.technical_indicators as ti
from sklearn.svm import SVR
from sklearn.svm import SVC
import statsmodels.api as sm
import os.path
import ta.strategy as ts
import datetime as dt
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None


def get_results_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']

    ticker_class = cmi.ticker_class[ticker_head]

    indicator_list = ['change_1_high_volume','change_5_high_volume','change_10_high_volume','change_20_high_volume',
                      'change_1_low_volume','change_5_low_volume','change_10_low_volume','change_20_low_volume',
                      'change_1Normalized','change_5Normalized','change_10Normalized','change_20Normalized',
                      'comm_net_change_1_normalized','comm_net_change_2_normalized','comm_net_change_4_normalized',
                      'spec_net_change_1_normalized','spec_net_change_2_normalized','spec_net_change_4_normalized',
                      'comm_z','spec_z']

    date_from = cu.doubledate_shift(date_to, 5*365)

    datetime_to = cu.convert_doubledate_2datetime(date_to)

    panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head,settle_date_from=date_from,settle_date_to=date_to)
    panel_data = panel_data[panel_data['tr_dte']>=40]

    panel_data.sort(['settle_date','tr_dte'],ascending=[True,True],inplace=True)
    rolling_data = panel_data.drop_duplicates(subset=['settle_date'], take_last=False)

    daily_noise = np.std(rolling_data['change_1'])
    average_volume = rolling_data['volume'].mean()

    rolling_data['change_40'] = pd.rolling_sum(rolling_data['change_1'], 40, min_periods=30)
    rolling_data['change_20'] = pd.rolling_sum(rolling_data['change_1'], 20, min_periods=15)
    rolling_data['change_10'] = pd.rolling_sum(rolling_data['change_1'], 10, min_periods=7)

    rolling_data['change_1Normalized'] = rolling_data['change_1']/daily_noise
    rolling_data['change_5Normalized'] = rolling_data['change_5']/daily_noise
    rolling_data['change_10Normalized'] = rolling_data['change_10']/daily_noise
    rolling_data['change_20Normalized'] = rolling_data['change_20']/daily_noise
    rolling_data['change_40Normalized'] = rolling_data['change_40']/daily_noise

    rolling_data['change1Normalized'] = rolling_data['change1']/daily_noise
    rolling_data['change1_InstantNormalized'] = rolling_data['change1_instant']/daily_noise

    rolling_data['change1_InstantNormalized'] = rolling_data['change1_instant']/daily_noise
    rolling_data['high1_InstantNormalized'] = (rolling_data['high1_instant']-rolling_data['close_price'])/daily_noise
    rolling_data['low1_InstantNormalized'] = (rolling_data['low1_instant']-rolling_data['close_price'])/daily_noise

    rolling_data['change5Normalized'] = rolling_data['change5']/daily_noise
    rolling_data['change10Normalized'] = rolling_data['change10']/daily_noise
    rolling_data['change20Normalized'] = rolling_data['change20']/daily_noise

    rolling_data['volume_mean20'] = pd.rolling_mean(rolling_data['volume'], 20, min_periods=15)
    rolling_data['volume_mean10'] = pd.rolling_mean(rolling_data['volume'], 10, min_periods=7)
    rolling_data['volume_mean5'] = pd.rolling_mean(rolling_data['volume'], 5, min_periods=4)

    rolling_data['volume_mean20Normalized'] = rolling_data['volume_mean20']/average_volume
    rolling_data['volume_mean10Normalized'] = rolling_data['volume_mean10']/average_volume
    rolling_data['volume_mean5Normalized'] = rolling_data['volume_mean5']/average_volume
    rolling_data['volume_Normalized'] = rolling_data['volume']/average_volume

    rolling_data['change_1_high_volume'] = (rolling_data['volume']>average_volume)*rolling_data['change_1Normalized']
    rolling_data['change_5_high_volume'] = (rolling_data['volume_mean5']>average_volume)*rolling_data['change_5Normalized']
    rolling_data['change_10_high_volume'] = (rolling_data['volume_mean10']>average_volume)*rolling_data['change_10Normalized']
    rolling_data['change_20_high_volume'] = (rolling_data['volume_mean20']>average_volume)*rolling_data['change_20Normalized']

    rolling_data['change_1_low_volume'] = (rolling_data['volume']<=average_volume)*rolling_data['change_1Normalized']
    rolling_data['change_5_low_volume'] = (rolling_data['volume_mean5']<=average_volume)*rolling_data['change_5Normalized']
    rolling_data['change_10_low_volume'] = (rolling_data['volume_mean10']<=average_volume)*rolling_data['change_10Normalized']
    rolling_data['change_20_low_volume'] = (rolling_data['volume_mean20']<=average_volume)*rolling_data['change_20Normalized']

    cot_output = cot.get_cot_data(ticker_head=ticker_head,date_from=date_from,date_to=date_to)

    dictionary_out = {'vote1': np.nan, 'vote1_instant': np.nan,'vote12_instant': np.nan,'vote13_instant': np.nan,
                'vote5': np.nan,'vote10': np.nan,'vote20': np.nan,
                'regress_forecast1': np.nan,'regress_forecast2': np.nan,'regress_forecast3': np.nan,
                'svr_forecast1': np.nan,'svr_forecast2': np.nan,
                'norm_pnl1': np.nan,
                'norm_pnl1Instant': np.nan,
                'long_tight_stop_pnl1Instant': np.nan,
                'long_loose_stop_pnl1Instant': np.nan,
                'short_tight_stop_pnl1Instant': np.nan,
                'short_loose_stop_pnl1Instant': np.nan,
                'norm_pnl5': np.nan,
                'norm_pnl10': np.nan,
                'norm_pnl20': np.nan,
                'rolling_data': pd.DataFrame()}

    for ind in indicator_list:
        dictionary_out[ind] = np.nan

    if len(cot_output.index)<20:
        return dictionary_out

    cot_net = pd.DataFrame()

    if ticker_class in ['FX','STIR','Index','Treasury']:
        cot_net['comm_net'] = cot_output['Dealer Longs']-cot_output['Dealer Shorts']
        cot_net['spec_net'] = cot_output['Asset Manager Longs']-cot_output['Asset Manager Shorts']+cot_output['Leveraged Funds Longs']-cot_output['Leveraged Funds Shorts']
    else:
        cot_net['comm_net'] = cot_output['Producer/Merchant/Processor/User Longs']-cot_output['Producer/Merchant/Processor/User Shorts']
        cot_net['spec_net'] = cot_output['Money Manager Longs']-cot_output['Money Manager Shorts']

    cot_net['comm_net_change_1'] = cot_net['comm_net']-cot_net['comm_net'].shift(1)
    cot_net['comm_net_change_2'] = cot_net['comm_net']-cot_net['comm_net'].shift(2)
    cot_net['comm_net_change_4'] = cot_net['comm_net']-cot_net['comm_net'].shift(4)

    cot_net['spec_net_change_1'] = cot_net['spec_net']-cot_net['spec_net'].shift(1)
    cot_net['spec_net_change_2'] = cot_net['spec_net']-cot_net['spec_net'].shift(2)
    cot_net['spec_net_change_4'] = cot_net['spec_net']-cot_net['spec_net'].shift(4)

    comm_net_change_1_avg = np.std(cot_net['comm_net_change_1'])
    comm_net_change_2_avg = np.std(cot_net['comm_net_change_2'])
    comm_net_change_4_avg = np.std(cot_net['comm_net_change_4'])
    spec_net_change_1_avg = np.std(cot_net['spec_net_change_1'])
    spec_net_change_2_avg = np.std(cot_net['spec_net_change_2'])
    spec_net_change_4_avg = np.std(cot_net['spec_net_change_4'])

    cot_net['comm_net_change_1_normalized'] = cot_net['comm_net_change_1']/comm_net_change_1_avg
    cot_net['comm_net_change_2_normalized'] = cot_net['comm_net_change_2']/comm_net_change_2_avg
    cot_net['comm_net_change_4_normalized'] = cot_net['comm_net_change_4']/comm_net_change_4_avg

    cot_net['spec_net_change_1_normalized'] = cot_net['spec_net_change_1']/spec_net_change_1_avg
    cot_net['spec_net_change_2_normalized'] = cot_net['spec_net_change_2']/spec_net_change_2_avg
    cot_net['spec_net_change_4_normalized'] = cot_net['spec_net_change_4']/spec_net_change_4_avg

    cot_net['comm_z']= (cot_net['comm_net']-np.mean(cot_net['comm_net']))/np.std(cot_net['comm_net'])
    cot_net['spec_z']= (cot_net['spec_net']-np.mean(cot_net['spec_net']))/np.std(cot_net['spec_net'])

    cot_net['settle_date'] = cot_net.index
    cot_net['settle_date'] = [x+dt.timedelta(days=3) for x in cot_net['settle_date']]

    combined_data = pd.merge(rolling_data,cot_net,how='left',on='settle_date')

    combined_data['comm_net_change_1_normalized'] = combined_data['comm_net_change_1_normalized'].fillna(method='pad')
    combined_data['comm_net_change_2_normalized'] = combined_data['comm_net_change_2_normalized'].fillna(method='pad')
    combined_data['comm_net_change_4_normalized'] = combined_data['comm_net_change_4_normalized'].fillna(method='pad')

    combined_data['spec_net_change_1_normalized']  = combined_data['spec_net_change_1_normalized'].fillna(method='pad')
    combined_data['spec_net_change_2_normalized']  = combined_data['spec_net_change_2_normalized'].fillna(method='pad')
    combined_data['spec_net_change_4_normalized']  = combined_data['spec_net_change_4_normalized'].fillna(method='pad')

    combined_data['comm_z'] = combined_data['comm_z'].fillna(method='pad')
    combined_data['spec_z'] = combined_data['spec_z'].fillna(method='pad')

    test_data = combined_data[combined_data['settle_date'] == datetime_to]
    training_data = combined_data[combined_data['settle_date'] < datetime_to+dt.timedelta(days=-30)]

    if test_data.empty or training_data.empty:
        return dictionary_out

    sharp1_list = []
    sharp1_instant_list = []
    sharp5_list = []
    sharp10_list = []
    sharp20_list = []
    higher_level_list = []
    lower_level_list = []

    for i in range(len(indicator_list)):
        selected_data = training_data[training_data[indicator_list[i]].notnull()]
        indicator_levels = stats.get_number_from_quantile(y=selected_data[indicator_list[i]].values,quantile_list=[10,90])
        lower_level_list.append(indicator_levels[0])
        higher_level_list.append(indicator_levels[1])
        low_data = selected_data[selected_data[indicator_list[i]]<indicator_levels[0]]
        high_data = selected_data[selected_data[indicator_list[i]]>indicator_levels[1]]
        high_data['pnl1'] = high_data['change1Normalized']
        low_data['pnl1'] = -low_data['change1Normalized']

        high_data['pnl1_instant'] = high_data['change1_InstantNormalized']
        low_data['pnl1_instant'] = -low_data['change1_InstantNormalized']

        high_data['pnl5'] = high_data['change5Normalized']
        low_data['pnl5'] = -low_data['change5Normalized']

        high_data['pnl10'] = high_data['change10Normalized']
        low_data['pnl10'] = -low_data['change10Normalized']

        high_data['pnl20'] = high_data['change20Normalized']
        low_data['pnl20'] = -low_data['change20Normalized']
        merged_data = pd.concat([high_data,low_data])
        sharp1_list.append(16*merged_data['pnl1'].mean()/merged_data['pnl1'].std())
        sharp1_instant_list.append(16*merged_data['pnl1_instant'].mean()/merged_data['pnl1_instant'].std())
        sharp5_list.append(7.2*merged_data['pnl5'].mean()/merged_data['pnl5'].std())
        sharp10_list.append(5.1*merged_data['pnl10'].mean()/merged_data['pnl10'].std())
        sharp20_list.append(3.5*merged_data['pnl20'].mean()/merged_data['pnl20'].std())

    sharp_frame = pd.DataFrame.from_items([('indicator', indicator_list),
                                           ('lower_level',lower_level_list),('higher_level',higher_level_list),('sharp1', sharp1_list),
                         ('sharp1_instant', sharp1_instant_list),
                         ('sharp5', sharp5_list),
                         ('sharp10', sharp10_list),
                         ('sharp20', sharp20_list)])

    vote1 = 0

    for i in range(len(indicator_list)):
        indicator_value = test_data[indicator_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_list[i]]

        if (selected_sharp_row['sharp1'].iloc[0]>0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote1 += 1
        elif (selected_sharp_row['sharp1'].iloc[0]>0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote1 -= 1
        elif (selected_sharp_row['sharp1'].iloc[0]<-0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote1 -= 1
        elif (selected_sharp_row['sharp1'].iloc[0]<-0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote1 += 1

    vote1_instant = 0

    for i in range(len(indicator_list)):
        indicator_value = test_data[indicator_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_list[i]]

        if (selected_sharp_row['sharp1_instant'].iloc[0]>0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote1_instant += 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]>0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote1_instant -= 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]<-0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote1_instant -= 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]<-0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote1_instant += 1

    vote12_instant = 0

    indicator_t_list = ['change_1_high_volume','change_5_high_volume','change_10_high_volume','change_20_high_volume',
                      'change_1_low_volume','change_5_low_volume','change_10_low_volume','change_20_low_volume']

    for i in range(len(indicator_t_list)):
        indicator_value = test_data[indicator_t_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_t_list[i]]

        if (selected_sharp_row['sharp1_instant'].iloc[0]>0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote12_instant += 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]>0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote12_instant -= 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]<-0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote12_instant -= 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]<-0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote12_instant += 1

    vote13_instant = 0

    for i in range(len(indicator_t_list)):
        indicator_value = test_data[indicator_t_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_t_list[i]]

        if (selected_sharp_row['sharp1_instant'].iloc[0]>1)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote13_instant += 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]>1)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote13_instant -= 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]<-1)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote13_instant -= 1
        elif (selected_sharp_row['sharp1_instant'].iloc[0]<-1)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote13_instant += 1

    vote5 = 0

    for i in range(len(indicator_list)):
        indicator_value = test_data[indicator_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_list[i]]

        if (selected_sharp_row['sharp5'].iloc[0]>0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote5 += 1
        elif (selected_sharp_row['sharp5'].iloc[0]>0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote5 -= 1
        elif (selected_sharp_row['sharp5'].iloc[0]<-0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote5 -= 1
        elif (selected_sharp_row['sharp5'].iloc[0]<-0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote5 += 1

    vote10 = 0

    for i in range(len(indicator_list)):
        indicator_value = test_data[indicator_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_list[i]]

        if (selected_sharp_row['sharp10'].iloc[0]>0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote10 += 1
        elif (selected_sharp_row['sharp10'].iloc[0]>0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote10 -= 1
        elif (selected_sharp_row['sharp10'].iloc[0]<-0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote10 -= 1
        elif (selected_sharp_row['sharp10'].iloc[0]<-0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote10 += 1

    vote20 = 0

    for i in range(len(indicator_list)):
        indicator_value = test_data[indicator_list[i]].iloc[0]
        selected_sharp_row = sharp_frame[sharp_frame['indicator']==indicator_list[i]]

        if (selected_sharp_row['sharp20'].iloc[0]>0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote20 += 1
        elif (selected_sharp_row['sharp20'].iloc[0]>0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote20 -= 1
        elif (selected_sharp_row['sharp20'].iloc[0]<-0.75)&(indicator_value>selected_sharp_row['higher_level'].iloc[0]):
            vote20 -= 1
        elif (selected_sharp_row['sharp20'].iloc[0]<-0.75)&(indicator_value<selected_sharp_row['lower_level'].iloc[0]):
            vote20 += 1

    #svr_rbf = SVR(kernel='rbf', C=1e3, gamma=0.1)
    #svr_forecast = svr_rbf.fit(x,y).predict(test_data[indicator_list].values)[0]
    #svc_rbf1 = SVC(kernel='rbf', C=1, gamma=0.1)
    #svc_forecast1 = svc_rbf1.fit(x,y).predict(test_data[indicator_list].values)[0]

    regress_forecast1 = np.nan
    regress_forecast2 = np.nan
    regress_forecast3 = np.nan
    svr_forecast1 = np.nan
    svr_forecast2 = np.nan

    try:
        regress_input = training_data[['change_1Normalized','change_10Normalized','change1_InstantNormalized']].dropna()
        y = regress_input['change1_InstantNormalized']
        X = regress_input[['change_1Normalized','change_10Normalized']]
        X = sm.add_constant(X)
        params1 = sm.OLS(y, X).fit().params
        regress_forecast1 = params1['const']+\
                        params1['change_1Normalized']*test_data['change_1Normalized'].iloc[0]+\
                        params1['change_10Normalized']*test_data['change_10Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_1Normalized','change_10Normalized','change1_InstantNormalized','comm_net_change_1_normalized']].dropna()
        y = regress_input['change1_InstantNormalized']
        X = regress_input[['change_1Normalized','change_10Normalized','comm_net_change_1_normalized']]
        X = sm.add_constant(X)
        params2 = sm.OLS(y, X).fit().params
        regress_forecast2 = params2['const']+\
                        params2['change_1Normalized']*test_data['change_1Normalized'].iloc[0]+\
                        params2['change_10Normalized']*test_data['change_10Normalized'].iloc[0]+\
                        params2['comm_net_change_1_normalized']*test_data['comm_net_change_1_normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_1Normalized','change_10Normalized','change1_InstantNormalized','spec_net_change_1_normalized']].dropna()
        y = regress_input['change1_InstantNormalized']
        X = regress_input[['change_1Normalized','change_10Normalized','spec_net_change_1_normalized']]
        X = sm.add_constant(X)
        params3 = sm.OLS(y, X).fit().params
        regress_forecast3 = params3['const']+\
                        params3['change_1Normalized']*test_data['change_1Normalized'].iloc[0]+\
                        params3['change_10Normalized']*test_data['change_10Normalized'].iloc[0]+\
                        params3['spec_net_change_1_normalized']*test_data['spec_net_change_1_normalized'].iloc[0]
    except:
        pass

    regress_input = training_data[['change_1Normalized','change_10Normalized','change1_InstantNormalized','comm_net_change_1_normalized']].dropna()

    try:
        svr_rbf1 = SVR(kernel='rbf', C=1, gamma=0.04)
        y = regress_input['change1_InstantNormalized']
        X = regress_input[['change_1Normalized','change_10Normalized','comm_net_change_1_normalized']]
        svr_forecast1 = svr_rbf1.fit(X,y).predict(test_data[['change_1Normalized','change_10Normalized','comm_net_change_1_normalized']].values)[0]
    except:
        pass

    try:
        svr_rbf2 = SVR(kernel='rbf', C=50, gamma=0.04)
        y = regress_input['change1_InstantNormalized']
        X = regress_input[['change_1Normalized','change_10Normalized','comm_net_change_1_normalized']]
        svr_forecast2 = svr_rbf2.fit(X,y).predict(test_data[['change_1Normalized','change_10Normalized','comm_net_change_1_normalized']].values)[0]
    except:
        pass

    if test_data['low1_InstantNormalized'].iloc[0]<-0.25:
        long_tight_stop_pnl1Instant = -0.25
    else:
        long_tight_stop_pnl1Instant = test_data['change1_InstantNormalized'].iloc[0]

    if test_data['low1_InstantNormalized'].iloc[0]<-0.5:
        long_loose_stop_pnl1Instant = -0.5
    else:
        long_loose_stop_pnl1Instant = test_data['change1_InstantNormalized'].iloc[0]

    if test_data['high1_InstantNormalized'].iloc[0]>0.25:
        short_tight_stop_pnl1Instant = -0.25
    else:
        short_tight_stop_pnl1Instant = -test_data['change1_InstantNormalized'].iloc[0]

    if test_data['high1_InstantNormalized'].iloc[0]>0.5:
        short_loose_stop_pnl1Instant = -0.5
    else:
        short_loose_stop_pnl1Instant = -test_data['change1_InstantNormalized'].iloc[0]

    dictionary_out['vote1'] = vote1
    dictionary_out['vote1_instant'] = vote1_instant
    dictionary_out['vote12_instant'] = vote12_instant
    dictionary_out['vote13_instant'] = vote13_instant
    dictionary_out['vote5'] = vote5
    dictionary_out['vote10'] = vote10
    dictionary_out['vote20'] = vote20
    dictionary_out['regress_forecast1'] = regress_forecast1
    dictionary_out['regress_forecast2'] = regress_forecast2
    dictionary_out['regress_forecast3'] = regress_forecast3
    dictionary_out['svr_forecast1'] = svr_forecast1
    dictionary_out['svr_forecast2'] = svr_forecast2
    dictionary_out['norm_pnl1'] = test_data['change1Normalized'].iloc[0]
    dictionary_out['norm_pnl1Instant'] = test_data['change1_InstantNormalized'].iloc[0]
    dictionary_out['long_tight_stop_pnl1Instant'] = long_tight_stop_pnl1Instant
    dictionary_out['long_loose_stop_pnl1Instant'] = long_loose_stop_pnl1Instant
    dictionary_out['short_tight_stop_pnl1Instant'] = short_tight_stop_pnl1Instant
    dictionary_out['short_loose_stop_pnl1Instant'] = short_loose_stop_pnl1Instant
    dictionary_out['norm_pnl5'] = test_data['change5Normalized'].iloc[0]
    dictionary_out['norm_pnl10'] = test_data['change10Normalized'].iloc[0]
    dictionary_out['norm_pnl20'] = test_data['change20Normalized'].iloc[0]
    dictionary_out['rolling_data'] = rolling_data

    for ind in indicator_list:
        dictionary_out[ind] = test_data[ind].iloc[0]

    return dictionary_out


def get_results_4date(**kwargs):

    date_to = kwargs['date_to']
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_directional', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        directionals = pd.read_pickle(output_dir + '/summary.pkl')
        corr_matrix = pd.read_pickle(output_dir + '/corr.pkl')
        return {'directionals': directionals, 'corr_matrix': corr_matrix, 'success': True}

    ticker_head_list = list(set(cmi.cme_futures_tickerhead_list+cmi.futures_butterfly_strategy_tickerhead_list))

    results_output = [get_results_4tickerhead(ticker_head=x, date_to=date_to) for x in ticker_head_list]
    rolling_data_list = [x.pop('rolling_data') for x in results_output]

    directionals = pd.DataFrame(results_output)
    directionals['ticker_head'] = ticker_head_list
    directionals['weekday'] = datetime_to.weekday()

    directionals.to_pickle(output_dir + '/summary.pkl')

    aux_frame_list = []
    for i in range(len(ticker_head_list)):
        rolling_data = rolling_data_list[i].iloc[-20:]
        if not rolling_data.empty:
            aux_frame = pd.DataFrame(index=rolling_data['settle_date'])
            aux_frame[ticker_head_list[i]]=rolling_data['change_1'].values
            aux_frame_list.append(aux_frame)

    if len(aux_frame_list)>1:
        combined_frame = pd.concat(aux_frame_list, axis=1, join='inner')
        corr_matrix = combined_frame.corr()
    else:
        corr_matrix = pd.DataFrame()

    corr_matrix.to_pickle(output_dir + '/corr.pkl')

    return {'directionals': directionals, 'corr_matrix': corr_matrix, 'success': True}


