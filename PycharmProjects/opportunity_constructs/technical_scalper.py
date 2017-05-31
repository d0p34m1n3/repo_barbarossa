
import contract_utilities.contract_lists as cl
import signals.intraday_technical_scalper as its
import ta.strategy as ts
import pandas as pd
import os.path


def get_tickers_4date(**kwargs):

    liquid_futures_frame = cl.get_liquid_outright_futures_frame(settle_date=kwargs['date_to'])

    liquid_futures_frame = liquid_futures_frame[~liquid_futures_frame['ticker_head'].isin(['ED', 'TU'])]

    liquid_futures_frame.reset_index(drop=True,inplace=True)
    return liquid_futures_frame[['ticker', 'ticker_head']]


def get_ts_results_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='ts', report_date=date_to)

    if os.path.isfile(output_dir + '/trades.pkl'):
        trades = pd.read_pickle(output_dir + '/trades.pkl')
        return trades

    tickers_4date = get_tickers_4date(**kwargs)
    trade_list = []

    for i in range(len(tickers_4date.index)):

        trade_data = its.get_technical_scalper_4ticker(ticker=tickers_4date['ticker'].iloc[i], date_to=date_to)['trade_data']

        if not trade_data.empty:
            trade_list.append(trade_data)

    if len(trade_list)>0:
        trades = pd.concat(trade_list)
        trades.to_pickle(output_dir + '/trades.pkl')
    else:
        trades = pd.DataFrame()
    return trades










