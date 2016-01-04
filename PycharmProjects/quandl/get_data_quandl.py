__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects\contract_utilities')
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import pandas as pd
import Quandl as qndl

quandl_database = {'LN': 'CME',
                   'LC': 'CME',
                   'FC': 'CME',
                    'C': 'CME',
                    'S': 'CME',
                   'SM': 'CME',
                   'BO': 'CME',
                    'W': 'CME',
                   'KW': 'CME',
                   'SB': 'ICE',
                   'KC': 'ICE',
                   'CC': 'ICE',
                   'CT': 'ICE',
                   'OJ': 'ICE',
                   'CL': 'CME',
                   'B' : 'ICE',
                   'HO': 'CME',
                   'RB': 'CME',
                   'NG': 'CME',
                   'ED': 'CME'}

def get_quandl_database_4ticker(ticker):
    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    return quandl_database[ticker_head]

def get_daily_historic_data_quandl(**kwargs):

    ticker = kwargs['ticker']
    quandl_database_4ticker = get_quandl_database_4ticker(ticker)

    quandl_input = {'authtoken' : "zwBtPkKDycmg5jmYvK_s"}

    if 'date_from' in kwargs.keys():
        date_from_string = cu.convert_datestring_format({'date_string': str(kwargs['date_from']),
                                                         'format_from' : 'yyyymmdd', 'format_to' : 'yyyy-mm-dd'})
        quandl_input['trim_start'] = date_from_string

    if 'date_to' in kwargs.keys():
        date_to_string = cu.convert_datestring_format({'date_string': str(kwargs['date_to']),
                                                         'format_from' : 'yyyymmdd', 'format_to' : 'yyyy-mm-dd'})
        quandl_input['trim_end'] = date_to_string


    try:
        data_out = qndl.get(quandl_database_4ticker + '/' + ticker, **quandl_input)
        success = True
    except:
        print('Error Loading ' + quandl_database_4ticker + '/' + ticker + ': ' + str(sys.exc_info()[0]))
        success = False
        data_out = pd.DataFrame()

    new_column_names = ['Open Interest' if x =='Prev. Day Open Interest' else x for x in data_out.columns]
    data_out.columns = new_column_names

    return {'success': success, 'data_out': data_out}



