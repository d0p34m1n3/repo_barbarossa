
import pandas as pd
import numpy as np
import pickle as pckl
import contract_utilities.contract_lists as cl
import ta.strategy as ts
import signals.intraday_futures_signals as ifs
import os.path


def get_tickers_4date(**kwargs):

    liquid_futures_frame = cl.get_liquid_outright_futures_frame(settle_date=kwargs['date_to'])
    liquid_futures_frame.reset_index(drop=True,inplace=True)
    return liquid_futures_frame[['ticker', 'ticker_head']]


def generate_ibo_sheet_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=date_to)

    #if os.path.isfile(output_dir + '/summary.pkl') and os.path.isfile(output_dir + '/cov.pkl'):
    #    sheet_4date = pd.read_pickle(output_dir + '/summary.pkl')
    #    cov_output = pckl.load(open(output_dir + '/cov.pkl', 'rb'))
    #    return {'sheet_4date': sheet_4date, 'cov_output': cov_output, 'success': True}

    sheet_4date = get_tickers_4date(**kwargs)

    num_tickers = len(sheet_4date.index)

    sheet_4date = pd.concat([ifs.get_intraday_trend_signals(ticker=sheet_4date.iloc[x]['ticker'],date_to=date_to)['pnl_frame']
                      for x in range(num_tickers)])

    sheet_4date.to_pickle(output_dir + '/summary.pkl')

    cov_output = ifs.get_intraday_outright_covariance(**kwargs)

    with open(output_dir + '/cov.pkl', 'wb') as handle:
        pckl.dump(cov_output, handle)

    return {'sheet_4date': sheet_4date, 'cov_output': cov_output, 'success': True}



