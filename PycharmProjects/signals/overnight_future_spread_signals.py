
import contract_utilities.expiration as exp
import signals.utils as sutil
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import statsmodels.api as sm
import shared.statistics as stats
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import datetime as dt
import numpy as np
import pandas as pd


def get_ofs_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    ticker_list = [x for x in ticker_list if x is not None]
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in list(set(ticker_head_list))}

    date5_years_ago = cu.doubledate_shift(date_to,5*365)
    datetime_to = cu.convert_doubledate_2datetime(date_to)

    rolling_data_list = []
    num_contracts = len(ticker_list)
    contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]

    for i in range(num_contracts):
        panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head_list[i],settle_date_from=date5_years_ago,settle_date_to=date_to,
                                                     futures_data_dictionary=futures_data_dictionary)
        panel_data = panel_data[panel_data['tr_dte']>=40]
        panel_data.sort(['settle_date','tr_dte'],ascending=[True,True],inplace=True)
        rolling_data = panel_data.drop_duplicates(subset=['settle_date'], take_last=False)
        rolling_data.set_index('settle_date',inplace=True)
        rolling_data_list.append(rolling_data)

    aligned_data = pd.concat(rolling_data_list, axis=1, join='inner',keys=['c'+ str(x+1) for x in range(num_contracts)])

    if ticker_head_list in sutil.fixed_weight_future_spread_list:
        weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
        spread_weights = weights_output['spread_weights']
        portfolio_weights = weights_output['portfolio_weights']
    else:
        regress_output = stats.get_regression_results({'x':aligned_data['c2']['change_1'], 'y':aligned_data['c1']['change_1']})
        spread_weights = [1, -regress_output['beta']]
        portfolio_weights = [1, -regress_output['beta']*contract_multiplier_list[0]/contract_multiplier_list[1]]

    num_contracts = len(ticker_list)

    aligned_data['spread'] = 0
    aligned_data['spread_change_1'] = 0
    aligned_data['spread_pnl_1'] = 0
    aligned_data['spread_change1_instant'] = 0
    aligned_data['spread_change1'] = 0
    aligned_data['spread_change5'] = 0
    aligned_data['spread_change10'] = 0

    for i in range(num_contracts):
        aligned_data['spread'] = aligned_data['spread']+aligned_data['c' + str(i+1)]['close_price']*spread_weights[i]
        aligned_data['spread_change_1'] = aligned_data['spread_change_1']+aligned_data['c' + str(i+1)]['change_1']*spread_weights[i]
        aligned_data['spread_pnl_1'] = aligned_data['spread_pnl_1']+aligned_data['c' + str(i+1)]['change_1']*portfolio_weights[i]*contract_multiplier_list[i]
        aligned_data['spread_change1_instant'] = aligned_data['spread_change1_instant']+aligned_data['c' + str(i+1)]['change1_instant']*spread_weights[i]
        aligned_data['spread_change1'] = aligned_data['spread_change1']+aligned_data['c' + str(i+1)]['change1']*spread_weights[i]
        aligned_data['spread_change5'] = aligned_data['spread_change5']+aligned_data['c' + str(i+1)]['change5']*spread_weights[i]
        aligned_data['spread_change10'] = aligned_data['spread_change10']+aligned_data['c' + str(i+1)]['change10']*spread_weights[i]

    aligned_data['spread_normalized'] = aligned_data['spread']/aligned_data['c1']['close_price']

    aligned_data['spread_change_40'] = pd.rolling_sum(aligned_data['spread_change_1'], 40, min_periods=30)
    aligned_data['spread_change_20'] = pd.rolling_sum(aligned_data['spread_change_1'], 20, min_periods=15)
    aligned_data['spread_change_10'] = pd.rolling_sum(aligned_data['spread_change_1'], 10, min_periods=7)
    aligned_data['spread_change_5'] = pd.rolling_sum(aligned_data['spread_change_1'], 5, min_periods=4)
    aligned_data['spread_change_2'] = pd.rolling_sum(aligned_data['spread_change_1'], 2, min_periods=2)

    daily_dollar_noise = np.std(aligned_data['spread_pnl_1'])
    daily_noise = np.std(aligned_data['spread_change_1'])

    aligned_data['change1_instantNormalized'] = aligned_data['spread_change1_instant']/daily_noise
    aligned_data['change1Normalized'] = aligned_data['spread_change1']/daily_noise
    aligned_data['change5Normalized'] = aligned_data['spread_change5']/daily_noise
    aligned_data['change10Normalized'] = aligned_data['spread_change10']/daily_noise
    aligned_data['change_1Normalized'] = aligned_data['spread_change_1']/daily_noise
    aligned_data['change_2Normalized'] = aligned_data['spread_change_2']/daily_noise
    aligned_data['change_5Normalized'] = aligned_data['spread_change_5']/daily_noise
    aligned_data['change_10Normalized'] = aligned_data['spread_change_10']/daily_noise
    aligned_data['change_20Normalized'] = aligned_data['spread_change_20']/daily_noise
    aligned_data['change_40Normalized'] = aligned_data['spread_change_40']/daily_noise

    aligned_data['change_2Delta'] = aligned_data['change_2Normalized']-aligned_data['change_2Normalized'].shift(2)

    test_data = aligned_data[aligned_data.index == datetime_to]
    training_data = aligned_data[aligned_data.index < datetime_to+dt.timedelta(days=-30)]

    regress_forecast1Instant1 = np.nan
    regress_forecast1Instant2 = np.nan
    regress_forecast1Instant3 = np.nan
    regress_forecast1Instant4 = np.nan
    regress_forecast11 = np.nan
    regress_forecast12 = np.nan
    regress_forecast13 = np.nan
    regress_forecast14 = np.nan
    regress_forecast51 = np.nan
    regress_forecast52 = np.nan
    regress_forecast53 = np.nan
    regress_forecast54 = np.nan
    regress_forecast101 = np.nan
    regress_forecast102 = np.nan
    regress_forecast103 = np.nan

    try:
        regress_input = training_data[['change_5Normalized','change_1Normalized','change1_instantNormalized']].dropna()
        y = regress_input['change1_instantNormalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast1Instant1 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_10Normalized','change_5Normalized','change_1Normalized','change1_instantNormalized']].dropna()
        y = regress_input['change1_instantNormalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast1Instant2 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_20Normalized','change_10Normalized','change_5Normalized','change_1Normalized','change1_instantNormalized']].dropna()
        y = regress_input['change1_instantNormalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized','change_20Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast1Instant3 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]+\
                        params[4]*test_data['change_20Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_40Normalized','change_20Normalized','change_10Normalized','change_5Normalized','change_1Normalized','change1_instantNormalized']].dropna()
        y = regress_input['change1_instantNormalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized','change_20Normalized','change_40Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast1Instant4 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]+\
                        params[4]*test_data['change_20Normalized'].iloc[0]+\
                        params[5]*test_data['change_40Normalized'].iloc[0]
    except:
        pass


    try:
        regress_input = training_data[['change_5Normalized','change_1Normalized','change1Normalized']].dropna()
        y = regress_input['change1Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast11 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_10Normalized','change_5Normalized','change_1Normalized','change1Normalized']].dropna()
        y = regress_input['change1Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast12 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_20Normalized','change_10Normalized','change_5Normalized','change_1Normalized','change1Normalized']].dropna()
        y = regress_input['change1Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized','change_20Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast13 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]+\
                        params[4]*test_data['change_20Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_40Normalized','change_20Normalized','change_10Normalized','change_5Normalized','change_1Normalized','change1Normalized']].dropna()
        y = regress_input['change1Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized','change_20Normalized','change_40Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast14 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]+\
                        params[4]*test_data['change_20Normalized'].iloc[0]+\
                        params[5]*test_data['change_40Normalized'].iloc[0]
    except:
        pass


    try:
        regress_input = training_data[['change_5Normalized','change_1Normalized','change5Normalized']].dropna()
        y = regress_input['change5Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast51 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_10Normalized','change_5Normalized','change_1Normalized','change5Normalized']].dropna()
        y = regress_input['change5Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast52 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_20Normalized','change_10Normalized','change_5Normalized','change_1Normalized','change5Normalized']].dropna()
        y = regress_input['change5Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized','change_20Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast53 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]+\
                        params[4]*test_data['change_20Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_40Normalized','change_20Normalized','change_10Normalized','change_5Normalized','change_1Normalized','change5Normalized']].dropna()
        y = regress_input['change5Normalized']
        X = regress_input[['change_1Normalized', 'change_5Normalized','change_10Normalized','change_20Normalized','change_40Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast54 = params[0]+\
                        params[1]*test_data['change_1Normalized'].iloc[0]+\
                        params[2]*test_data['change_5Normalized'].iloc[0]+\
                        params[3]*test_data['change_10Normalized'].iloc[0]+\
                        params[4]*test_data['change_20Normalized'].iloc[0]+\
                        params[5]*test_data['change_40Normalized'].iloc[0]
    except:
        pass



    try:
        regress_input = training_data[['change_10Normalized','change_5Normalized','change10Normalized']].dropna()
        y = regress_input['change10Normalized']
        X = regress_input[['change_5Normalized','change_10Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast101 = params[0]+\
                        params[1]*test_data['change_5Normalized'].iloc[0]+\
                        params[2]*test_data['change_10Normalized'].iloc[0]
    except:
        pass

    try:
        regress_input = training_data[['change_20Normalized','change_10Normalized','change_5Normalized','change10Normalized']].dropna()
        y = regress_input['change10Normalized']
        X = regress_input[['change_5Normalized','change_10Normalized','change_20Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast102 = params[0]+\
                        params[1]*test_data['change_5Normalized'].iloc[0]+\
                        params[2]*test_data['change_10Normalized'].iloc[0]+\
                        params[3]*test_data['change_20Normalized'].iloc[0]
    except:
        pass


    try:
        regress_input = training_data[['change_40Normalized','change_20Normalized','change_10Normalized','change_5Normalized','change10Normalized']].dropna()
        y = regress_input['change10Normalized']
        X = regress_input[['change_5Normalized','change_10Normalized','change_20Normalized','change_40Normalized']]
        X = sm.add_constant(X)
        params = sm.OLS(y, X).fit().params
        regress_forecast103 = params[0]+\
                        params[1]*test_data['change_5Normalized'].iloc[0]+\
                        params[2]*test_data['change_10Normalized'].iloc[0]+\
                        params[3]*test_data['change_20Normalized'].iloc[0]+\
                        params[4]*test_data['change_40Normalized'].iloc[0]

    except:
        pass

    return {'regress_forecast1Instant1':regress_forecast1Instant1,
            'regress_forecast1Instant2':regress_forecast1Instant2,
            'regress_forecast1Instant3':regress_forecast1Instant3,
            'regress_forecast1Instant4':regress_forecast1Instant4,
            'regress_forecast11':regress_forecast11,
            'regress_forecast12':regress_forecast12,
            'regress_forecast13':regress_forecast13,
            'regress_forecast14':regress_forecast14,
            'regress_forecast51':regress_forecast51,
            'regress_forecast52':regress_forecast52,
            'regress_forecast53':regress_forecast53,
            'regress_forecast54':regress_forecast54,
            'regress_forecast101':regress_forecast101,
            'regress_forecast102':regress_forecast102,
            'regress_forecast103':regress_forecast103,
            'daily_dollar_noise': daily_dollar_noise,
            'change1_instantNormalized': test_data['spread_change1_instant'].iloc[0],
            'change1Normalized': test_data['change1Normalized'].iloc[0],
            'change5Normalized': test_data['change5Normalized'].iloc[0],
            'change10Normalized': test_data['change10Normalized'].iloc[0],
            'change_1Normalized': test_data['change_1Normalized'].iloc[0],
            'change_2Delta': test_data['change_2Delta'].iloc[0],
            'change_5Normalized': test_data['change_5Normalized'].iloc[0],
            'change_10Normalized': test_data['change_10Normalized'].iloc[0],
            'change_20Normalized': test_data['change_20Normalized'].iloc[0],
            'change_40Normalized': test_data['change_40Normalized'].iloc[0]}

