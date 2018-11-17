
import get_price.get_stock_price as gsp
import math as m
from pykalman import KalmanFilter
import shared.statistics as ss
import statsmodels.api as sm
import pandas as pd
import numpy as np
import shared.calendar_utilities as cu
pd.options.mode.chained_assignment = None  # default='warn'


def get_summary(**kwargs):

    symbol1 = kwargs['symbol1']
    symbol2 = kwargs['symbol2']
    report_date = kwargs['report_date']

    if 'get_diagnosticQ' in kwargs.keys():
        get_diagnosticQ = kwargs['get_diagnosticQ']
    else:
        get_diagnosticQ = False

    report_datetime = cu.convert_doubledate_2datetime(report_date)

    data1 = gsp.get_stock_price_preloaded(ticker=symbol1, data_source='iex', settle_date_to = report_date)
    data2 = gsp.get_stock_price_preloaded(ticker=symbol2, data_source='iex', settle_date_to = report_date)

    merged_data = pd.merge(data1[['close','settle_datetime']], data2[['close','settle_datetime']], how='inner', on='settle_datetime')
    split = int(len(merged_data) * .4)

    if split<200 or report_datetime!=merged_data['settle_datetime'].iloc[-1]:
        return {'price1': np.nan,'price2': np.nan, 'p_value_2': np.nan,'p_value_1': np.nan,
            'beta_1': np.nan, 'beta_2': np.nan,
            'corr': np.nan,
            'cagr1': np.nan, 'cagr2': np.nan,
            'kalpha': np.nan, 'kbeta': np.nan,
            'meanSpread': np.nan, 'stdSpread': np.nan,
            'zScore': np.nan}

    training_data = merged_data[:split]
    test_data = merged_data[split:]
    cointegration_output_2 = sm.tsa.stattools.coint(training_data['close_x'], training_data['close_y'])
    cointegration_output_1 = sm.tsa.stattools.coint(test_data['close_x'], test_data['close_y'])

    regress_output_1 = ss.get_regression_results({'y': test_data['close_y'].values, 'x': test_data['close_x'].values})
    regress_output_2 = ss.get_regression_results({'y': training_data['close_y'].values, 'x': training_data['close_x'].values})
    regress_output_3 = ss.get_regression_results({'y': test_data['close_y'].diff().values, 'x': test_data['close_x'].diff().values})

    merged_data.set_index('settle_datetime', drop=True, inplace=True)
    backtest_output_1 = backtest(merged_data[split:], 'close_x', 'close_y')
    backtest_output_2 = backtest(merged_data[:split], 'close_x', 'close_y')

    if get_diagnosticQ:
        return {'backtest_output': backtest_output_1, 'cagr2': backtest_output_2['cagr']}
    else:
        return {'price1': merged_data['close_x'].iloc[-1],'price2': merged_data['close_y'].iloc[-1],
            'p_value_2': cointegration_output_2[1],'p_value_1': cointegration_output_1[1],
            'beta_1': regress_output_1['beta'], 'beta_2': regress_output_2['beta'],
            'corr': np.sqrt(regress_output_3['rsquared']/100),
            'cagr1': backtest_output_1['cagr'], 'cagr2': backtest_output_2['cagr'],
            'kalpha': backtest_output_1['kalpha'], 'kbeta': backtest_output_1['kbeta'],
            'meanSpread': backtest_output_1['meanSpread'], 'stdSpread': backtest_output_1['stdSpread'],
            'zScore': backtest_output_1['zScore']}






def KalmanFilterAverage(x):
    # Construct a Kalman filter
    kf = KalmanFilter(transition_matrices=[1],
                      observation_matrices=[1],
                      initial_state_mean=0,
                      initial_state_covariance=1,
                      observation_covariance=1,
                      transition_covariance=.01)

    # Use the observed values of the price to get a rolling mean
    state_means, _ = kf.filter(x.values)
    state_means = pd.Series(state_means.flatten(), index=x.index)
    return state_means


# Kalman filter regression
def KalmanFilterRegression(x, y):
    delta = 1e-3
    trans_cov = delta / (1 - delta) * np.eye(2)  # How much random walk wiggles
    obs_mat = np.expand_dims(np.vstack([[x], [np.ones(len(x))]]).T, axis=1)

    kf = KalmanFilter(n_dim_obs=1, n_dim_state=2,  # y is 1-dimensional, (alpha, beta) is 2-dimensional
                      initial_state_mean=[0, 0],
                      initial_state_covariance=np.ones((2, 2)),
                      transition_matrices=np.eye(2),
                      observation_matrices=obs_mat,
                      observation_covariance=2,
                      transition_covariance=trans_cov)

    # Use the observations y to get running estimates and errors for the state parameters
    state_means, state_covs = kf.filter(y.values)
    return state_means


def half_life(spread):
    spread_lag = spread.shift(1)
    spread_lag.iloc[0] = spread_lag.iloc[1]
    spread_ret = spread - spread_lag
    spread_ret.iloc[0] = spread_ret.iloc[1]
    spread_lag2 = sm.add_constant(spread_lag)
    model = sm.OLS(spread_ret, spread_lag2)
    res = model.fit()
    halflife = int(round(-np.log(2) / res.params[1], 0))

    if halflife <= 0:
        halflife = 1
    return halflife


def backtest(df, s1, s2):
    #############################################################
    # INPUT:
    # DataFrame of prices
    # s1: the symbol of contract one
    # s2: the symbol of contract two
    # x: the price series of contract one
    # y: the price series of contract two
    # OUTPUT:
    # df1['cum rets']: cumulative returns in pandas data frame
    # sharpe: Sharpe ratio
    # CAGR: Compound Annual Growth Rate

    x = df[s1]
    y = df[s2]

    # run regression (including Kalman Filter) to find hedge ratio and then create spread series
    df1 = pd.DataFrame({'y': y, 'x': x})

    state_means = KalmanFilterRegression(KalmanFilterAverage(x), KalmanFilterAverage(y))

    df1['hr'] = - state_means[:, 0]
    df1['alpha'] = - state_means[:, 1]
    df1['spread'] = df1.y + (df1.x * df1.hr+ df1.alpha)

    # calculate half life
    halflife = half_life(df1['spread'])

    # calculate z-score with window = half life period
    df1['meanSpread'] = df1.spread.rolling(window=60).mean()
    df1['stdSpread'] = df1.spread.rolling(window=60).std()
    df1['zScore'] = (df1.spread - df1.meanSpread) / df1.stdSpread

    ##############################################################
    # trading logic
    entryZscore = 2
    exitZscore = 0

    # set up num units long
    df1['long entry'] = ((df1.zScore < - entryZscore) & ( df1.zScore.shift(1) >- entryZscore))
    df1['long exit'] = ((df1.zScore > - exitZscore) & (df1.zScore.shift(1) <- exitZscore))
    df1['num units long'] = np.nan
    df1.loc[df1['long entry'], 'num units long'] = 1
    df1.loc[df1['long exit'], 'num units long'] = 0
    df1['num units long'][0] = 0
    df1['num units long'] = df1['num units long'].fillna(method='pad')
    # set up num units short
    df1['short entry'] = ((df1.zScore > entryZscore) & ( df1.zScore.shift(1) < entryZscore))
    df1['short exit'] = ((df1.zScore < exitZscore) & (df1.zScore.shift(1) > exitZscore))
    df1.loc[df1['short entry'], 'num units short'] = -1
    df1.loc[df1['short exit'], 'num units short'] = 0
    df1['num units short'][0] = 0
    df1['num units short'] = df1['num units short'].fillna(method='pad')

    start_date = df1.iloc[0].name
    end_date = df1.iloc[-1].name

    days = (end_date - start_date).days

    entry_price_x_list = []
    entry_price_y_list = []
    exit_price_x_list = []
    exit_price_y_list = []

    entry_index_list = []
    exit_index_list = []

    qty_x_list = []
    qty_y_list = []

    pnl_list = [0]

    for i in range(1, len(df1.index)):

        if df1['num units long'].iloc[i-1] > 0:
                pnl_list.append(df1['y'].iloc[i] - df1['y'].iloc[i - 1] + qty_x_list[-1] * (
                            df1['x'].iloc[i] - df1['x'].iloc[i - 1]))
        elif df1['num units short'].iloc[i-1] < 0:
                pnl_list.append(df1['y'].iloc[i - 1] - df1['y'].iloc[i] + qty_x_list[-1] * (df1['x'].iloc[i] - df1['x'].iloc[i - 1]))
        else:
                pnl_list.append(0)



        if df1['y_qty'].iloc[i] > 0 and df1['y_qty'].iloc[i - 1] <= 0:
            entry_price_x_list.append(df1['x'].iloc[i])
            entry_price_y_list.append(df1['y'].iloc[i])
            entry_index_list.append(i)
            qty_y_list.append(1)
            qty_x_list.append(df1['hr'].iloc[i])
        if df1['y_qty'].iloc[i] < 0 and df1['y_qty'].iloc[i - 1] >= 0:
            entry_price_x_list.append(df1['x'].iloc[i])
            entry_price_y_list.append(df1['y'].iloc[i])
            entry_index_list.append(i)
            qty_y_list.append(-1)
            qty_x_list.append(-df1['hr'].iloc[i])
        if (df1['y_qty'].iloc[i] <= 0 and df1['y_qty'].iloc[i - 1] > 0) or \
                (df1['y_qty'].iloc[i] >= 0 and df1['y_qty'].iloc[i - 1] < 0):
            exit_price_x_list.append(df1['x'].iloc[i])
            exit_price_y_list.append(df1['y'].iloc[i])
            exit_index_list.append(i)

    df1['pnl'] = pnl_list

    if len(entry_price_x_list) == len(exit_price_x_list) + 1:
        exit_price_x_list.append(np.nan)
        exit_price_y_list.append(np.nan)
        exit_index_list.append(np.nan)

    pnl_frame = pd.DataFrame.from_dict({'x_entry': entry_price_x_list,
                                        'y_entry': entry_price_y_list,
                                        'x_exit': exit_price_x_list,
                                        'y_exit': exit_price_y_list,
                                        'x_qty': qty_x_list,
                                        'y_qty': qty_y_list,
                                        'entry_index': entry_index_list,
                                        'exit_index': exit_index_list})
    pnl_frame['pnl'] = pnl_frame['x_qty'] * (pnl_frame['x_exit'] - pnl_frame['x_entry']) + \
                       pnl_frame['y_qty'] * (pnl_frame['y_exit'] - pnl_frame['y_entry'])

    pnl_frame['pnl_per'] = 100 * pnl_frame['pnl'] / pnl_frame['y_entry']

    CAGR = round(((pnl_frame['pnl_per'].sum())*(252.0 / days)),1)

    return {'data_frame' : df1,
            'pnl_frame': pnl_frame,
            'cagr': CAGR,
            'kalpha': -df1['alpha'].iloc[-1],
            'kbeta': -df1['hr'].iloc[-1],
            'meanSpread': df1['meanSpread'].iloc[-1],
            'stdSpread' : df1['stdSpread'].iloc[-1],
            'zScore': df1['zScore'].iloc[-1]}
