__author__ = 'kocat_000'

import sys
import quandl.get_data_quandl as gdq
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
import pandas as pd
import numpy as np
import datetime
from pandas.tseries.offsets import CustomBusinessDay

def load_price_data_4ticker(load_price_data_input):

    ticker = load_price_data_input['ticker']
    expiration_date = load_price_data_input['expiration_date']
    data_vendor_id = load_price_data_input['data_vendor_id']
    symbol_id = load_price_data_input['symbol_id']

    quandl_input = {'ticker': ticker}

    if 'date_to' in load_price_data_input.keys():
        quandl_input['date_to'] = load_price_data_input['date_to']

    if 'date_from' in load_price_data_input.keys() and load_price_data_input['date_from'] is not None:
        quandl_input['date_from'] = load_price_data_input['date_from']

    quandl_out = gdq.get_daily_historic_data_quandl(**quandl_input)

    if not quandl_out['success']:
        return

    price_data = quandl_out['data_out']

    if price_data.empty:
        print('Empty Results For ' + ticker)
        return

    contract_specs_output = cmi.get_contract_specs(ticker)
    bday_us = CustomBusinessDay(calendar=exp.get_calendar_4ticker_head(contract_specs_output['ticker_head']))
    dts = pd.date_range(start=price_data.index[0], end=expiration_date, freq=bday_us)
    dts = [x.date() for x in dts]

    now = datetime.datetime.utcnow()

    price_data['price_date'] = pd.Series(price_data.index, index=price_data.index)

    column_names = price_data.columns.tolist()

    open_indx = column_names.index('Open')
    high_indx = column_names.index('High')
    low_indx = column_names.index('Low')
    settle_indx = column_names.index('Settle')
    volume_indx = column_names.index('Volume')
    interest_indx = column_names.index('Open Interest')
    date_indx = column_names.index('price_date')

    tuples = [tuple([data_vendor_id, symbol_id,
                     contract_specs_output['ticker_head'],
                     contract_specs_output['ticker_month_num'],
                     x[date_indx].to_datetime().date(),
                     (expiration_date-x[date_indx].to_datetime().date()).days,
                     len([y for y in dts if y > x[date_indx].to_datetime().date()]),
                     now, now,
                     None if np.isnan(x[open_indx]) else x[open_indx],
                     None if np.isnan(x[high_indx]) else x[high_indx],
                     None if np.isnan(x[low_indx]) else x[low_indx],
                     None if np.isnan(x[settle_indx]) else x[settle_indx],
                     None if np.isnan(x[volume_indx]) else x[volume_indx],
                     None if np.isnan(x[interest_indx]) else x[interest_indx]]) for x in price_data.values]

    column_str = "data_vendor_id, symbol_id, ticker_head, ticker_month, price_date,cal_dte, tr_dte, created_date,last_updated_date, open_price, high_price, low_price, close_price, volume, open_interest"
    insert_str = ("%s, " * 15)[:-2]
    final_str = "REPLACE INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)

    con = msu.get_my_sql_connection(**load_price_data_input)

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in load_price_data_input.keys():
        con.close()

def load_entire_history(**kwargs):

    contract_list = cl.get_db_contract_list_filtered(ticker_year_from=1980, ticker_year_to=2022)
    # length of contract_list is 6594

    if 'start_indx' in kwargs.keys():
        start_indx = kwargs['start_indx']
    else:
        start_indx = 0

    if 'end_indx' in kwargs.keys():
        end_indx = kwargs['end_indx']
    else:
        end_indx = 6751

    con = msu.get_my_sql_connection(**kwargs)
    load_price_data_input = dict()
    load_price_data_input['con'] = con

    if 'date_to' in kwargs.keys():
        load_price_data_input['date_to'] = kwargs['date_to']

    for i in range(start_indx, end_indx):
        load_price_data_input['symbol_id'] = contract_list[i][0]
        load_price_data_input['data_vendor_id'] = 1
        load_price_data_input['ticker'] = contract_list[i][1]
        load_price_data_input['expiration_date'] = contract_list[i][2]

        load_price_data_4ticker(load_price_data_input)
        print('No : ' + str(i) + ', ' + contract_list[i][1] + ' loaded')

    if 'con' not in kwargs.keys():
        con.close()


def update_futures_price_database(**kwargs):

    import time
    run_date = int(time.strftime('%Y%m%d'))
    early_start_date = cu.doubledate_shift(run_date, 15)
    con = msu.get_my_sql_connection(**kwargs)

    contract_list = []

    for key,value in cmi.relevant_max_cal_dte.items():

        contract_list.extend(cl.get_db_contract_list_filtered(expiration_date_from = early_start_date,
                                                            expiration_date_to = cu.doubledate_shift(run_date, -value),
                                                            ticker_head=key, con=con))
    date_from_list = [gfp.get_futures_last_price_date_4ticker(ticker=x[1], con=con) for x in contract_list]

    load_price_data_input = dict()
    load_price_data_input['con'] = con

    for i in range(len(contract_list)):
        load_price_data_input['symbol_id'] = contract_list[i][0]
        load_price_data_input['data_vendor_id'] = 1
        load_price_data_input['ticker'] = contract_list[i][1]
        load_price_data_input['expiration_date'] = contract_list[i][2]
        load_price_data_input['date_from'] = date_from_list[i]

        load_price_data_4ticker(load_price_data_input)
        print('No : ' + str(i) + ', ' +  contract_list[i][1] + ' loaded')

    if 'con' not in kwargs.keys():
        con.close()



