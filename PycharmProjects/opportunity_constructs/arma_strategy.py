

import contract_utilities.contract_lists as cl
import signals.futures_directional_signals as fds
import os.path
import ta.strategy as ts
import pandas as pd


def get_tickers_4date(**kwargs):
    data_out = cl.generate_futures_list_dataframe(**kwargs)
    data_out = data_out[(data_out['tr_dte'] >= 40)]
    data_out.sort(['ticker_head', 'tr_dte'],ascending=[True,True], inplace=True)
    data_out.drop_duplicates(subset=['ticker_head'], take_last=False, inplace=True)
    data_out.reset_index(drop=True,inplace=True)
    return data_out[['ticker', 'ticker_head', 'ticker_class']]


def get_arma_sheet_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='arma', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        arma_sheet = pd.read_pickle(output_dir + '/summary.pkl')
        return {'arma_sheet': arma_sheet,'success': True}

    arma_sheet = get_tickers_4date(**kwargs)
    signals_output = [fds.get_arma_signals(ticker_head=x, date_to=date_to) for x in arma_sheet['ticker_head']]
    arma_sheet = pd.concat([arma_sheet, pd.DataFrame(signals_output)], axis=1)
    arma_sheet = arma_sheet[arma_sheet['success']]

    arma_sheet = arma_sheet[['ticker','ticker_head','ticker_class', 'forecast','normalized_forecast','param1','param2', 'normalized_target']]

    arma_sheet.to_pickle(output_dir + '/summary.pkl')

    return {'arma_sheet': arma_sheet,'success': True}