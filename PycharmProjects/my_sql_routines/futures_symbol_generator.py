__author__ = 'kocat_000'

import mysql.connector
from math import ceil
import contract_utilities.contract_lists as cl
import my_sql_routines.my_sql_utilities as msu


def generate_symbol_table(**kwargs):

    instrument = kwargs['instrument']

    if instrument == 'futures':
        ticker_list = cl.get_contract_list_4year_range(**kwargs)
    elif instrument == 'options':
        ticker_list = cl.get_option_contract_list_4year_range(**kwargs)

    con = msu.get_my_sql_connection(**kwargs)

    column_str = "ticker, ticker_head, ticker_year, ticker_month, expiration_date," \
             "instrument, name, ticker_class, currency, created_date, last_updated_date"

    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    cur = con.cursor()

    for i in range(0, int(ceil(len(ticker_list) / 100.0))):
        cur.executemany(final_str, ticker_list[i*100:(i+1)*100])

    con.commit()

    if 'con' not in kwargs.keys():
        con.close()


