__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import mysql.connector
from math import ceil
import contract_utilities.contract_lists as cl

def generate_futures_symbol_table(symbol_table_input):
    db_host = '127.0.0.1'
    db_user = 'ekocatulum'
    db_pass = 'pompei1789'
    db_name = 'futures_master'

    ticker_list = cl.get_contract_list_4year_range(symbol_table_input)

    con = mysql.connector.connect(user=db_user, password=db_pass, host=db_host, database=db_name)

    column_str = "ticker, ticker_head, ticker_year, ticker_month, expiration_date," \
             "instrument, name, ticker_class, currency, created_date, last_updated_date"

    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    cur = con.cursor()

    for i in range(0, int(ceil(len(ticker_list) / 100.0))):
        cur.executemany(final_str, ticker_list[i*100:(i+1)*100])
    con.commit()
    con.close()

