__author__ = 'kocat_000'

import shared.directory_names as dn
import shared.calendar_utilities as cu
import os.path
import pandas as pd
import shared.converters as conv
import datetime as dt
import my_sql_routines.my_sql_utilities as msu


def generate_db_strategy_from_strategy_sheet(**kwargs):

    id = kwargs['id']
    strategy_class = kwargs['strategy_class']

    strategy_output = load_strategy_file(**kwargs)
    strategy = strategy_output.loc[id]

    if strategy_class == 'futures_butterfly':
        if strategy['Q'] <= 49:
            strategy_alias = strategy['tickerHead'] + strategy['ticker1'][-5:] + strategy['ticker2'][-5:] + \
                             strategy['ticker2'][-5:] + strategy['ticker3'][-5:]
        elif strategy['Q'] >= 51:
            strategy_alias = strategy['tickerHead'] + strategy['ticker2'][-5:] + strategy['ticker3'][-5:] + \
                       strategy['ticker1'][-5:] + strategy['ticker2'][-5:]

    description_string = 'strategy_class=' + strategy_class + '&' + \
                          conv.convert_from_dictionary_to_string(dictionary_input=strategy)

    return generate_db_strategy_from_alias(alias=strategy_alias, description_string=description_string)

def generate_db_strategy_from_alias(**kwargs):

    alias = kwargs['alias']
    description_string = kwargs['description_string']

    con = msu.get_my_sql_connection(**kwargs)
    cur = con.cursor()

    for i in range(1, 10):

        if i > 1:
            alias_modified = alias + '_' + str(i)
        else:
            alias_modified = alias

        strategy_id = get_strategy_id_from_alias(alias=alias_modified, con=con)

        if not strategy_id:
            break

    now_date = dt.datetime.now().date()

    column_str = "alias, open_date, close_date, created_date, last_updated_date, description_string"
    insert_str = ("%s, " * 6)[:-2]

    tuple_to_load = (alias_modified, now_date, cu.convert_doubledate_2datetime(30000101),
                     now_date, now_date, description_string)

    final_str = "INSERT INTO strategy (%s) VALUES (%s)" % (column_str, insert_str)

    cur.execute(final_str,tuple_to_load)
    con.commit()

    strategy_id = get_strategy_id_from_alias(alias=alias_modified, con=con)

    if 'con' not in kwargs.keys():
        con.close()

    return strategy_id

def get_trades_4strategy_alias(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    cur = con.cursor()

    sql_query = 'SELECT tr.id, tr.ticker, tr.option_type, tr.strike_price, tr.trade_price, tr.trade_quantity, tr.trade_date, tr.instrument, tr.real_tradeQ ' + \
                 'FROM strategy as str INNER JOIN trades as tr ON tr.strategy_id=str.id ' + \
                 'WHERE str.alias=\'' + kwargs['alias'] + '\''
    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    trade_frame = pd.DataFrame(data, columns=['id', 'ticker', 'option_type', 'strike_price', 'trade_price', 'trade_quantity', 'trade_date', 'instrument', 'real_tradeQ'])
    trade_frame['trade_price'] = [float(x) if x is not None else float('NaN') for x in trade_frame['trade_price'].values]
    return trade_frame

def get_strategy_id_from_alias(**kwargs):
    alias = kwargs['alias']
    con = msu.get_my_sql_connection(**kwargs)
    cur = con.cursor()

    query_string = 'SELECT id FROM strategy WHERE alias=\'' + alias + '\''
    cur.execute(query_string)
    data = cur.fetchall()

    if not data:
        strategy_id = None
    else:
        strategy_id = data[0][0]

    if 'con' not in kwargs.keys():
        con.close()

    return strategy_id

def load_trades_2strategy(**kwargs):

    trade_frame = kwargs['trade_frame']
    strategy_id = kwargs['strategy_id']
    now_date = dt.datetime.now().date()

    column_str = "ticker, option_type, strike_price, strategy_id, trade_price, trade_quantity, trade_date, instrument, real_tradeQ, created_date, last_updated_date"
    insert_str = ("%s, " * 11)[:-2]

    final_str = "INSERT INTO trades (%s) VALUES (%s)" % (column_str, insert_str)

    column_names = trade_frame.columns.tolist()

    ticker_indx = column_names.index('ticker')
    option_type_indx = column_names.index('option_type')
    strike_price_indx = column_names.index('strike_price')
    trade_price_indx = column_names.index('trade_price')
    trade_quantity_indx = column_names.index('trade_quantity')
    instrument_indx = column_names.index('instrument')
    real_tradeQ_indx = column_names.index('real_tradeQ')

    tuples = [tuple([x[ticker_indx],x[option_type_indx], x[strike_price_indx], strategy_id,
              x[trade_price_indx], x[trade_quantity_indx],
              now_date,x[instrument_indx], x[real_tradeQ_indx],now_date,now_date]) for x in trade_frame.values]

    con = msu.get_my_sql_connection(**kwargs)

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in kwargs.keys():
        con.close()


def load_strategy_file(**kwargs):

    strategy_class = kwargs['strategy_class']
    report_date = kwargs['report_date']

    if strategy_class == 'futures_butterfly':
        output_dir = create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)
        strategy_output = pd.read_pickle(output_dir + '/summary.pkl')

    return strategy_output


def create_strategy_output_dir(**kwargs):

    strategy_class = kwargs['strategy_class']
    report_date = kwargs['report_date']

    strategy_output_folder = dn.get_directory_name(ext='strategy_output')

    if strategy_class == 'futures_butterfly':
        output_dir = strategy_output_folder + '/futures_butterfly/' + cu.get_directory_extension(report_date)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    return output_dir


