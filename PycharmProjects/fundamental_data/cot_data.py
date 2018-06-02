
import quandl_data.get_data_quandl as gdq
import shared.directory_names as dn
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import os.path
import pandas as pd

presaved_cot_data_folder = dn.get_directory_name(ext='commitments_of_traders_data')

db_2_quandl_dictionary = {'GC': '088691',
                          'SI': '084691',
                          'EC': '099741',
                          'BP': '096742',
                          'JY': '097741',
                          'AD': '232741',
                          'CD': '090741',
                          'TU': '042601',
                          'FV': '044601',
                          'TY': '043602',
                          'US': '020601',
                          'ED': '132741',
                          'ES': '13874A',
                          'NQ': '209742',
                          'CL': '067651',
                          'HO': '022651',
                          'RB': '111659',
                          'NG': '023651',
                          'C': '002602',
                          'W': '001602',
                          'KW': '001612',
                          'S': '005602',
                          'SM': '026603',
                          'BO': '007601',
                          'LN': '054642',
                          'LC': '057642',
                          'FC': '061641',
                          'KC': '083731',
                          'CT': '033661',
                          'SB': '080732',
                          'CC': '073732',
                          'OJ': '040701'}


def presave_cot_data_4ticker_head(**kwargs):

    ticker_head = kwargs['ticker_head']

    quandl_ticker ='CFTC/' + db_2_quandl_dictionary[ticker_head] + '_FO_ALL'

    kwargs['quandl_ticker'] = quandl_ticker
    quandl_out = gdq.get_data(**kwargs)

    if quandl_out['success']:
        data_out = quandl_out['data_out']
        data_out.to_pickle(presaved_cot_data_folder + '/' + ticker_head + '.pkl')


def presave_cot_data():

    tickerhead_list = list(set(cmi.futures_butterfly_strategy_tickerhead_list + cmi.cme_futures_tickerhead_list))
    tickerhead_list.remove('B')
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


