
import contract_utilities.contract_meta_info as cmi
import signals.intraday_futures_experimental as ife
import get_price.get_futures_price as gfp
import get_price.quantgo_data as qgd
import ta.strategy as ts
import boto3 as bt3
import pandas as pd
import os


def get_tickers_4date(**kwargs):

    ticker_head_list = cmi.cme_futures_tickerhead_list

    data_list = [gfp.get_futures_price_preloaded(ticker_head=x,settle_date=kwargs['date_to']) for x in ticker_head_list]
    ticker_frame = pd.concat(data_list)

    ticker_frame = ticker_frame[~((ticker_frame['ticker_head'] == 'ED') & (ticker_frame['tr_dte'] < 250))]

    ticker_frame.sort_values(['ticker_head', 'volume'], ascending=[True, False], inplace=True)
    ticker_frame.drop_duplicates(subset=['ticker_head'], keep='first', inplace=True)
    ticker_frame.reset_index(drop=True, inplace=True)

    return ticker_frame[['ticker','ticker_head']]

def get_result_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='intraday_futures_experimental', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        ticker_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'ticker_frame': ticker_frame, 'success': True}

    if 'boto_client' in kwargs.keys():
        boto_client = kwargs['boto_client']
    else:
        boto_client = qgd.get_boto_client()

    ticker_frame = get_tickers_4date(**kwargs)

    summary_stats_list = [ife.get_crossover_duration(ticker=x, date_to=kwargs['date_to'], boto_client=boto_client) for x in ticker_frame['ticker']]

    ticker_frame['mean_duration'] = [x['mean_duration'] for x in summary_stats_list]
    ticker_frame['duration_25'] = [x['duration_25'] for x in summary_stats_list]
    ticker_frame['duration_75'] = [x['duration_75'] for x in summary_stats_list]
    ticker_frame['num_crossovers'] = [x['num_crossovers'] for x in summary_stats_list]

    ticker_frame.to_pickle(output_dir + '/summary.pkl')

    return {'ticker_frame': ticker_frame, 'success': True}







