__author__ = 'kocat_000'

import pandas as pd

def backtest_curve_pca_4date(**kwargs):

    residuals = kwargs['residuals']
    change_data = kwargs['change_data']
    tr_dte_data = kwargs['tr_dte_data']

    spread_residual_vec = []
    spread_pnl_vec = []
    front_tr_dte_vec = []

    for i in range(len(residuals)-1):
        spread_residual_vec.append(residuals[i]-residuals[i+1])
        spread_pnl_vec.append(change_data[i]-change_data[i+1])
        front_tr_dte_vec.append(tr_dte_data[i])

    data_frame = pd.DataFrame.from_items([('residual', spread_residual_vec),
                         ('pnl5', spread_pnl_vec),
                         ('front_tr_dte', front_tr_dte_vec)])

    data_frame = data_frame[data_frame['front_tr_dte'] > 200]

    data_frame.sort('residual',ascending=True, inplace=True)

    long_frame = data_frame.iloc[0:3]
    short_frame = data_frame.iloc[-3:]

    return long_frame['pnl5'].sum()-short_frame['pnl5'].sum()




