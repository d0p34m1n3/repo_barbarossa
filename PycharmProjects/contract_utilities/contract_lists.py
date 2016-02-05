__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')

import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import datetime
import my_sql_routines.my_sql_utilities as msu
import get_price.get_futures_price as gfp
import get_price.presave_price as psp
import opportunity_constructs.utilities as ut
import pandas as pd


def get_contract_list_4year_range(**kwargs):

    now = datetime.datetime.utcnow()
    start_year = kwargs['start_year']
    end_year = kwargs['end_year']
    tickerhead_list = cmi.futures_butterfly_strategy_tickerhead_list
    futures_contract_months = cmi.futures_contract_months
    contract_name_dict = cmi.contract_name
    ticker_class_dict = cmi.ticker_class
    year_list = range(start_year,end_year)
    ticker_list = []
    for i in tickerhead_list:
        contract_months = futures_contract_months[i]
        for j in contract_months:
            for k in year_list:
                ticker = i+j+str(k)
                ticker_list.append((ticker,i,k,cmi.letter_month_string.find(j)+1,exp.get_futures_expiration(ticker),
                                'futures',contract_name_dict[i],ticker_class_dict[i],'USD',now,now))

    return ticker_list


def get_db_contract_list_filtered(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT id, ticker, expiration_date FROM symbol'

    if ('ticker_year_from' in kwargs.keys()) and ('ticker_year_to' in kwargs.keys()):
        filter_string = 'ticker_year>=' + str(kwargs['ticker_year_from']) + ' and ticker_year<=' + str(kwargs['ticker_year_to'])
    elif ('expiration_date_from' in kwargs.keys()) and ('expiration_date_to' in kwargs.keys()):
        filter_string = 'expiration_date>=' + str(kwargs['expiration_date_from']) + ' and expiration_date<=' + str(kwargs['expiration_date_to'])

    if 'ticker_head' in kwargs.keys():
        filter_string = filter_string + ' and ticker_head=\'' + kwargs['ticker_head'] + '\''

    if 'ticker_month' in kwargs.keys():
        filter_string = filter_string + ' and ticker_month=' + str(kwargs['ticker_month'])

    sql_query = sql_query + ' WHERE ' + filter_string + ' ORDER BY id ASC'

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()
    return data


def symbol_id_ticker_conversion(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    ticker = kwargs['ticker']
    con = kwargs['con']

    cur = con.cursor()
    cur.execute('SELECT * FROM futures_master.symbol WHERE ticker=\'' + ticker + '\'')
    data = cur.fetchall()

    if data[0][2] == ticker:
        output = data[0][0]

    if 'con' not in kwargs.keys():
        con.close()

    return output


def generate_futures_list_dataframe(**kwargs):

    futures_dataframe = gfp.get_futures_prices_4date(**kwargs)

    futures_dataframe = pd.merge(futures_dataframe, psp.dirty_data_points, on=['settle_date', 'ticker'], how='left')
    futures_dataframe = futures_dataframe[futures_dataframe['discard'] != True]
    futures_dataframe = futures_dataframe.drop('discard', 1)

    futures_dataframe['ticker_class'] = [cmi.ticker_class[ticker_head] for ticker_head in futures_dataframe['ticker_head']]
    futures_dataframe['multiplier'] = [cmi.contract_multiplier[ticker_head] for ticker_head in futures_dataframe['ticker_head']]

    additional_tuple = [ut.get_aggregation_method_contracts_back({'ticker_class': ticker_class, 'ticker_head': ticker_head})
                        for ticker_class, ticker_head in zip(futures_dataframe['ticker_class'],futures_dataframe['ticker_head'])]

    additional_dataframe = pd.DataFrame(additional_tuple,
                                    columns = ['aggregation_method', 'contracts_back'],index=futures_dataframe.index)
    return pd.concat([futures_dataframe, additional_dataframe],axis=1)








