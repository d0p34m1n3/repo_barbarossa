
import quandl.get_data_quandl as gdq
import shared.directory_names as dn
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import os.path
import pandas as pd

presaved_cot_data_folder = dn.get_directory_name(ext='commitments_of_traders_data')

def presave_cot_data_4ticker_head(**kwargs):

    ticker_head = kwargs['ticker_head']

    if ticker_head == 'LN':
        quandl_ticker ='CFTC/LH_FO_ALL'
    elif ticker_head == 'B':
    # cannot seem to locate brent
        quandl_ticker ='CFTC/BZ_FO_ALL'
    else:
        quandl_ticker ='CFTC/' + ticker_head + '_FO_ALL'

    kwargs['quandl_ticker'] = quandl_ticker
    quandl_out = gdq.get_data(**kwargs)

    if quandl_out['success']:
        data_out = quandl_out['data_out']
        data_out.to_pickle(presaved_cot_data_folder + '/' + ticker_head + '.pkl')

def presave_cot_data():

    tickerhead_list = list(set(cmi.futures_butterfly_strategy_tickerhead_list + cmi.cme_futures_tickerhead_list))
    [presave_cot_data_4ticker_head(ticker_head=x) for x in tickerhead_list]


def get_cot_data(**kwargs):

    ticker_head = kwargs['ticker_head']

    if os.path.isfile(presaved_cot_data_folder + '/' + ticker_head + '.pkl'):
        data_out = pd.read_pickle(presaved_cot_data_folder + '/' + ticker_head + '.pkl')
        if 'date_from' in kwargs.keys():
            data_out = data_out[data_out.index >= cu.convert_doubledate_2datetime(kwargs['date_from'])]
        if 'date_to' in kwargs.keys():
            data_out = data_out[data_out.index <= cu.convert_doubledate_2datetime(kwargs['date_to'])]
    else:
        data_out = pd.DataFrame()
    return data_out


