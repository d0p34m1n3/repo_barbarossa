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
import ta.trade_fill_loader as tfl
import shared.calendar_utilities as cu
import shared.directory_names as dn
import pandas as pd


def get_contract_list_4year_range(**kwargs):

    now = datetime.datetime.utcnow()
    start_year = kwargs['start_year']
    end_year = kwargs['end_year']

    if 'tickerhead_list' in kwargs.keys():
        tickerhead_list = kwargs['tickerhead_list']
    else:
        tickerhead_list = cmi.cme_futures_tickerhead_list

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


def get_option_contract_list_4year_range(**kwargs):

    now = datetime.datetime.utcnow()
    start_year = kwargs['start_year']
    end_year = kwargs['end_year']
    tickerhead_list = cmi.cme_option_tickerhead_list
    contract_name_dict = cmi.contract_name
    ticker_class_dict = cmi.ticker_class
    year_list = range(start_year,end_year)

    ticker_list = []
    for i in tickerhead_list:
        contract_months = cmi.get_option_contract_months(ticker_head=i)
        for j in contract_months:
            for k in year_list:
                ticker = i+j+str(k)
                print(ticker)
                ticker_list.append((ticker,i,k,cmi.letter_month_string.find(j)+1,exp.get_options_expiration(ticker),
                                'options',contract_name_dict[i],ticker_class_dict[i],'USD',now,now))

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

    if 'instrument' in kwargs.keys():
        filter_string = filter_string + ' and instrument=\'' + kwargs['instrument'] + '\''

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
                                    columns=['aggregation_method', 'contracts_back'],index=futures_dataframe.index)
    return pd.concat([futures_dataframe, additional_dataframe],axis=1)


def generate_liquid_options_list_dataframe(**kwargs):

    settle_date = kwargs['settle_date']
    con = msu.get_my_sql_connection(**kwargs)

    contract_list = []

    for ticker_head in cmi.option_tickerhead_list:
        for ticker_month in cmi.get_option_contract_months(ticker_head=ticker_head):
            ticker_month_num = cmi.letter_month_string.find(ticker_month)+1
            max_cal_dte = cmi.get_max_cal_dte(ticker_head=ticker_head, ticker_month=ticker_month_num)
            contract_list.extend(get_db_contract_list_filtered(expiration_date_from=settle_date,
                                                            expiration_date_to=cu.doubledate_shift(settle_date, -max_cal_dte),
                                                            ticker_head=ticker_head, ticker_month=ticker_month_num, con=con,
                                                                  instrument='options'))

    if 'con' not in kwargs.keys():
        con.close()

    return pd.DataFrame(contract_list,columns=['id', 'ticker', 'expiration_date'])


def get_liquid_outright_futures_frame(**kwargs):

    ticker_head_list = list(set(cmi.futures_butterfly_strategy_tickerhead_list) | set(cmi.cme_futures_tickerhead_list))

    data_dir = dn.get_dated_directory_extension(ext='intraday_ttapi_data', folder_date=kwargs['settle_date'])
    file_name = 'ContractList.csv'

    data_frame_out = pd.read_csv(data_dir + '/' + file_name)
    data_frame_out_filtered = data_frame_out[data_frame_out['ProductType'] == 'FUTURE']
    num_contracts = len(data_frame_out_filtered.index)

    reformat_out_list = [tfl.get_ticker_from_tt_instrument_name_and_product_name(instrument_name=data_frame_out_filtered['InstrumentName'].iloc[x],
                                                        product_name =data_frame_out_filtered['ProductName'].iloc[x] ) for x in range(num_contracts)]

    data_frame_out_filtered['ticker'] = [reformat_out_list[x]['ticker'] for x in range(num_contracts)]
    data_frame_out_filtered['ticker_head'] = [reformat_out_list[x]['ticker_head'] for x in range(num_contracts)]

    selection_indx = [data_frame_out_filtered['ticker_head'].iloc[x] in ticker_head_list for x in range(num_contracts)]
    data_frame_out_filtered2 = data_frame_out_filtered[selection_indx]

    data_frame_out_filtered2.sort(['ticker_head','Volume'],ascending=[True, False],inplace=True)
    return data_frame_out_filtered2.drop_duplicates('ticker_head')










