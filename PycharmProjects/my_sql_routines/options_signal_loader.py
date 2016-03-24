
import contract_utilities.contract_lists as cl
import signals.option_signals as ops
import pandas as pd
import contract_utilities.contract_meta_info as cmi
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import datetime as dt
import numpy as np


def load_atm_vols_4settle_date(**kwargs):

    options_frame = cl.generate_liquid_options_list_dataframe(**kwargs)

    delta_vol_output = [ops.calc_delta_vol_4ticker(ticker=x,
                                settle_date=kwargs['settle_date'],
                                delta_target=0.5) for x in options_frame['ticker']]

    atm_vol_frame = pd.concat([options_frame, pd.DataFrame(delta_vol_output)], axis=1)

    contract_specs_output = [cmi.get_contract_specs(x) for x in atm_vol_frame['ticker']]

    atm_vol_frame['ticker_head'] = [x['ticker_head'] for x in contract_specs_output]
    atm_vol_frame['ticker_month'] = [x['ticker_month_num'] for x in contract_specs_output]
    atm_vol_frame['ticker_year'] = [x['ticker_year'] for x in contract_specs_output]

    column_names = atm_vol_frame.columns.tolist()

    ticker_indx = column_names.index('ticker')
    cal_dte_indx = column_names.index('cal_dte')
    tr_dte_indx = column_names.index('tr_dte')
    ticker_head_indx = column_names.index('ticker_head')
    ticker_month_indx = column_names.index('ticker_month')
    ticker_year_indx = column_names.index('ticker_year')
    atm_vol_indx = column_names.index('delta_vol')

    now = dt.datetime.now()
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    tuples = [tuple([x[ticker_indx], x[ticker_head_indx], x[ticker_month_indx] , x[ticker_year_indx],
                      None if np.isnan(x[tr_dte_indx]) else x[tr_dte_indx],
                      None if np.isnan(x[cal_dte_indx]) else x[cal_dte_indx],
                      None if np.isnan(x[atm_vol_indx]) else 100*x[atm_vol_indx],
                      settle_datetime.date(),now, now]) for x in atm_vol_frame.values]

    con = msu.get_my_sql_connection(**kwargs)

    column_str = "ticker, ticker_head, ticker_month, ticker_year, " \
                 " cal_dte, tr_dte, atm_vol, price_date, created_date, last_updated_date"

    insert_str = ("%s, " * len(column_str.split(',')))[:-2]
    final_str = "REPLACE INTO option_ticker_indicators (%s) VALUES (%s)" % (column_str, insert_str)

    msu.sql_execute_many_wrapper(final_str=final_str, tuples=tuples, con=con)

    if 'con' not in kwargs.keys():
            con.close()




