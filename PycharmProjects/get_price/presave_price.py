__author__ = 'kocat_000'

import os.path
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
from pandas.tseries.offsets import CustomBusinessDay
import shared.calendar_utilities as cu
import shared.directory_names as dn
import pandas as pd
import numpy as np
import datetime as dt
pd.options.mode.chained_assignment = None

presaved_futures_data_folder = dn.get_directory_name(ext='presaved_futures_data')

dirty_data_points = pd.DataFrame([('BM2006', dt.datetime(2006, 2, 1), True),
                                  ('BM2006', dt.datetime(2006, 2, 3), True),
                                  ('BM2006', dt.datetime(2006, 2, 6), True),
                                  ('CLZ2012', dt.datetime(2006, 12, 27), True),
                                  ('CLZ2012', dt.datetime(2006, 12, 28), True),
                                  ('CLZ2011', dt.datetime(2007, 1, 3), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 4), True),
                                  ('CLZ2011', dt.datetime(2007, 1, 4), True),
                                  ('CLZ2012', dt.datetime(2007, 1, 18), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 22), True),
                                  ('CLZ2011', dt.datetime(2007, 1, 22), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 23), True),
                                  ('CLZ2011', dt.datetime(2007, 1, 23), True),
                                  ('CLZ2012', dt.datetime(2007, 1, 23), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 24), True),
                                  ('CLZ2011', dt.datetime(2007, 1, 24), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 25), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 26), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 27), True),
                                  ('CLZ2010', dt.datetime(2007, 1, 29), True),
                                  ('CLZ2009', dt.datetime(2007, 2, 15), True),
                                  ('CLZ2010', dt.datetime(2007, 3, 15), True),
                                  ('CLZ2008', dt.datetime(2007, 3, 29), True),
                                  ('CLG2008', dt.datetime(2007, 4, 25), True),
                                  ('CLF2008', dt.datetime(2007, 4, 26), True),
                                  ('CLF2008', dt.datetime(2007, 5, 1), True),
                                  ('CLZ2008', dt.datetime(2007, 5, 21), True),
                                  ('CLG2008', dt.datetime(2007, 5, 31), True),
                                  ('CLH2008', dt.datetime(2007, 5, 31), True),
                                  ('CLM2008', dt.datetime(2007, 5, 31), True),
                                  ('CLZ2010', dt.datetime(2007, 6, 19), True),
                                  ('CLZ2011', dt.datetime(2007, 6, 19), True),
                                  ('CLG2008', dt.datetime(2007, 6, 21), True),
                                  ('CLN2008', dt.datetime(2007, 6, 21), True),
                                  ('CLV2008', dt.datetime(2007, 6, 21), True),
                                  ('CLX2008', dt.datetime(2007, 6, 21), True),
                                  ('CLK2008', dt.datetime(2007, 6, 25), True),
                                  ('CLX2008', dt.datetime(2007, 6, 25), True),
                                  ('CLN2008', dt.datetime(2007, 6, 28), True),
                                  ('CLM2008', dt.datetime(2007, 7, 3), True),
                                  ('CLN2008', dt.datetime(2007, 7, 5), True),
                                  ('CLZ2011', dt.datetime(2007, 7, 11), True),
                                  ('CLV2008', dt.datetime(2007, 7, 12), True),
                                  ('CLX2008', dt.datetime(2007, 7, 12), True),
                                  ('CLU2009', dt.datetime(2007, 7, 16), True),
                                  ('CLG2008', dt.datetime(2007, 7, 18), True),
                                  ('CLH2008', dt.datetime(2007, 7, 18), True),
                                  ('CLJ2008', dt.datetime(2007, 7, 18), True),
                                  ('CLK2008', dt.datetime(2007, 7, 18), True),
                                  ('CLM2008', dt.datetime(2007, 7, 18), True),
                                  ('CLN2008', dt.datetime(2007, 7, 18), True),
                                  ('CLJ2008', dt.datetime(2007, 7, 19), True),
                                  ('CLN2008', dt.datetime(2007, 7, 25), True),
                                  ('CLQ2008', dt.datetime(2007, 7, 25), True),
                                  ('CLG2008', dt.datetime(2007, 7, 26), True),
                                  ('CLH2008', dt.datetime(2007, 7, 26), True),
                                  ('CLU2008', dt.datetime(2007, 7, 26), True),
                                  ('CLU2008', dt.datetime(2007, 7, 30), True),
                                  ('CLU2008', dt.datetime(2007, 7, 31), True),
                                  ('CLN2008', dt.datetime(2007, 8, 6), True),
                                  ('CLZ2009', dt.datetime(2007, 8, 30), True),
                                  ('CLZ2010', dt.datetime(2007, 8, 30), True),
                                  ('CLZ2011', dt.datetime(2007, 8, 30), True),
                                  ('CLV2008', dt.datetime(2007, 9, 10), True),
                                  ('CLN2009', dt.datetime(2008, 10, 16), True),
                                  ('EDZ2017', dt.datetime(2014, 2, 24), True),
                                  ('HOF2007', dt.datetime(2005, 12, 1), True),
                                  ('HOF2007', dt.datetime(2006, 1, 12), True),
                                  ('HOF2007', dt.datetime(2006, 1, 19), True),
                                  ('HOF2007', dt.datetime(2006, 1, 24), True),
                                  ('HOF2007', dt.datetime(2006, 2, 1), True),
                                  ('HOF2007', dt.datetime(2006, 2, 7), True),
                                  ('HOF2007', dt.datetime(2006, 2, 8), True),
                                  ('HOF2007', dt.datetime(2006, 2, 15), True),
                                  ('HOF2007', dt.datetime(2006, 2, 27), True),
                                  ('HOF2007', dt.datetime(2006, 3, 7), True),
                                  ('HOF2007', dt.datetime(2006, 3, 15), True),
                                  ('HOF2007', dt.datetime(2006, 3, 16), True),
                                  ('HOF2007', dt.datetime(2006, 3, 21), True),
                                  ('HOF2007', dt.datetime(2006, 3, 23), True),
                                  ('HOF2007', dt.datetime(2006, 3, 31), True),
                                  ('HOF2007', dt.datetime(2006, 4, 18), True),
                                  ('HOF2007', dt.datetime(2006, 4, 20), True),
                                  ('HOF2007', dt.datetime(2006, 4, 21), True),
                                  ('HOF2008', dt.datetime(2006, 12, 27), True),
                                  ('HOF2008', dt.datetime(2007, 1, 3), True),
                                  ('HOF2008', dt.datetime(2007, 1, 4), True),
                                  ('HOF2008', dt.datetime(2007, 1, 18), True),
                                  ('HOF2008', dt.datetime(2007, 1, 23), True),
                                  ('HOF2008', dt.datetime(2007, 1, 25), True),
                                  ('HOF2008', dt.datetime(2007, 1, 29), True),
                                  ('HOF2008', dt.datetime(2007, 1, 30), True),
                                  ('HOF2008', dt.datetime(2007, 1, 31), True),
                                  ('HOX2007', dt.datetime(2007, 2, 1), True),
                                  ('HOF2008', dt.datetime(2007, 2, 1), True),
                                  ('HOZ2007', dt.datetime(2007, 2, 5), True),
                                  ('HOX2007', dt.datetime(2007, 2, 7), True),
                                  ('HOU2007', dt.datetime(2007, 2, 7), True),
                                  ('HON2007', dt.datetime(2007, 2, 8), True),
                                  ('HOU2007', dt.datetime(2007, 2, 8), True),
                                  ('HOU2007', dt.datetime(2007, 2, 12), True),
                                  ('HOZ2007', dt.datetime(2007, 2, 12), True),
                                  ('HOU2007', dt.datetime(2007, 2, 13), True),
                                  ('HOQ2007', dt.datetime(2007, 2, 13), True),
                                  ('HOF2008', dt.datetime(2007, 2, 13), True),
                                  ('HOV2007', dt.datetime(2007, 2, 13), True),
                                  ('HOF2008', dt.datetime(2007, 2, 14), True),
                                  ('HOV2007', dt.datetime(2007, 2, 14), True),
                                  ('HOV2007', dt.datetime(2007, 2, 15), True),
                                  ('HOX2007', dt.datetime(2007, 2, 15), True),
                                  ('HOV2007', dt.datetime(2007, 2, 20), True),
                                  ('HOV2007', dt.datetime(2007, 2, 21), True),
                                  ('HOV2007', dt.datetime(2007, 2, 23), True),
                                  ('HOV2007', dt.datetime(2007, 2, 27), True),
                                  ('HOV2007', dt.datetime(2007, 2, 28), True),
                                  ('HOV2007', dt.datetime(2007, 3, 1), True),
                                  ('HOX2007', dt.datetime(2007, 3, 5), True),
                                  ('HOF2008', dt.datetime(2007, 3, 5), True),
                                  ('HOX2007', dt.datetime(2007, 3, 7), True),
                                  ('HOV2007', dt.datetime(2007, 3, 12), True),
                                  ('HOX2007', dt.datetime(2007, 3, 12), True),
                                  ('HOF2008', dt.datetime(2007, 3, 13), True),
                                  ('HOU2007', dt.datetime(2007, 3, 14), True),
                                  ('HOU2007', dt.datetime(2007, 3, 15), True),
                                  ('HOX2007', dt.datetime(2007, 3, 15), True),
                                  ('HOV2007', dt.datetime(2007, 3, 20), True),
                                  ('HOX2007', dt.datetime(2007, 3, 21), True),
                                  ('HOX2007', dt.datetime(2007, 3, 22), True),
                                  ('HOX2007', dt.datetime(2007, 3, 23), True),
                                  ('HOM2007', dt.datetime(2007, 3, 27), True),
                                  ('HOQ2007', dt.datetime(2007, 3, 27), True),
                                  ('HOU2007', dt.datetime(2007, 3, 27), True),
                                  ('HOZ2007', dt.datetime(2007, 3, 27), True),
                                  ('HOV2007', dt.datetime(2007, 3, 29), True),
                                  ('HOV2007', dt.datetime(2007, 4, 3), True),
                                  ('HOX2007', dt.datetime(2007, 4, 3), True),
                                  ('HOV2007', dt.datetime(2007, 4, 4), True),
                                  ('HOX2007', dt.datetime(2007, 4, 9), True),
                                  ('HOX2007', dt.datetime(2007, 4, 10), True),
                                  ('HOU2007', dt.datetime(2007, 4, 12), True),
                                  ('HOZ2007', dt.datetime(2007, 4, 12), True),
                                  ('HOF2008', dt.datetime(2007, 4, 16), True),
                                  ('HOF2008', dt.datetime(2007, 4, 17), True),
                                  ('HOV2007', dt.datetime(2007, 4, 18), True),
                                  ('HOF2008', dt.datetime(2007, 4, 18), True),
                                  ('HOV2007', dt.datetime(2007, 4, 19), True),
                                  ('HOX2007', dt.datetime(2007, 4, 19), True),
                                  ('HOF2008', dt.datetime(2007, 4, 19), True),
                                  ('HOV2009', dt.datetime(2009, 3, 18), True),
                                  ('HOX2009', dt.datetime(2009, 3, 18), True),
                                  ('HOZ2009', dt.datetime(2009, 3, 18), True),
                                  ('HOG2010', dt.datetime(2009, 3, 18), True),
                                  ('NGF2008', dt.datetime(2006, 12, 4), True),
                                  ('NGU2007', dt.datetime(2006, 12, 6), True),
                                  ('NGU2007', dt.datetime(2006, 12, 8), True),
                                  ('NGX2007', dt.datetime(2006, 12, 12), True),
                                  ('NGU2007', dt.datetime(2006, 12, 27), True),
                                  ('NGX2007', dt.datetime(2006, 12, 29), True),
                                  ('NGF2008', dt.datetime(2006, 12, 29), True),
                                  ('NGF2008', dt.datetime(2007, 1, 4), True),
                                  ('NGU2007', dt.datetime(2007, 1, 11), True),
                                  ('NGX2007', dt.datetime(2007, 1, 11), True),
                                  ('NGU2007', dt.datetime(2007, 1, 17), True),
                                  ('NGF2008', dt.datetime(2007, 1, 17), True),
                                  ('NGU2007', dt.datetime(2007, 1, 22), True),
                                  ('NGF2008', dt.datetime(2007, 1, 24), True),
                                  ('NGX2007', dt.datetime(2007, 1, 25), True),
                                  ('NGF2008', dt.datetime(2007, 1, 25), True),
                                  ('NGX2007', dt.datetime(2007, 1, 30), True),
                                  ('NGX2007', dt.datetime(2007, 2, 8), True),
                                  ('NGX2007', dt.datetime(2007, 2, 20), True),
                                  ('NGX2007', dt.datetime(2007, 2, 27), True),
                                  ('NGX2007', dt.datetime(2007, 4, 3), True),
                                  ('NGX2007', dt.datetime(2007, 4, 4), True),
                                  ('KCU2008', dt.datetime(2008, 2, 19), True)
                                  ],columns=['ticker','settle_date','discard'])


def generate_and_update_futures_data_file_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']

    con = msu.get_my_sql_connection(**kwargs)

    if os.path.isfile(presaved_futures_data_folder + '/' + ticker_head + '.pkl'):
        old_data = pd.read_pickle(presaved_futures_data_folder + '/' + ticker_head + '.pkl')
        last_available_date = int(old_data['settle_date'].max().to_datetime().strftime('%Y%m%d'))
        date_from = cu.doubledate_shift(last_available_date, 60)
        data4_tickerhead = gfp.get_futures_price_4ticker(ticker_head=ticker_head, date_from=date_from, con=con)
    else:
        data4_tickerhead = gfp.get_futures_price_4ticker(ticker_head=ticker_head, con=con)

    data4_tickerhead = pd.merge(data4_tickerhead, dirty_data_points, on=['settle_date', 'ticker'],how='left')
    data4_tickerhead = data4_tickerhead[data4_tickerhead['discard'] != True]
    data4_tickerhead = data4_tickerhead.drop('discard', 1)

    data4_tickerhead['close_price'] = [float(x) if x is not None else float('NaN') for x in data4_tickerhead['close_price'].values]

    data4_tickerhead['cont_indx']= 100*data4_tickerhead['ticker_year']+data4_tickerhead['ticker_month']
    unique_cont_indx_list = data4_tickerhead['cont_indx'].unique()
    num_contracts = len(unique_cont_indx_list)
    unique_cont_indx_list = np.sort(unique_cont_indx_list)
    merged_dataframe_list = [None]*num_contracts

    bday_us = CustomBusinessDay(calendar=exp.get_calendar_4ticker_head('CL'))
    full_dates = pd.date_range(start=data4_tickerhead['settle_date'].min(),end=data4_tickerhead['settle_date'].max(), freq=bday_us)

    for i in range(num_contracts):

        contract_data = data4_tickerhead[data4_tickerhead['cont_indx']==unique_cont_indx_list[i]]

        contract_full_dates = full_dates[(full_dates>=contract_data['settle_date'].min()) & (full_dates<=contract_data['settle_date'].max())]
        full_date_frame = pd.DataFrame(contract_full_dates, columns=['settle_date'])
        merged_dataframe_list[i] = pd.merge(full_date_frame,contract_data,on='settle_date',how='left')

        merged_dataframe_list[i]['ticker'] = contract_data['ticker'][contract_data.index[0]]
        merged_dataframe_list[i]['ticker_head'] = contract_data['ticker_head'][contract_data.index[0]]
        merged_dataframe_list[i]['ticker_month'] = contract_data['ticker_month'][contract_data.index[0]]
        merged_dataframe_list[i]['ticker_year'] = contract_data['ticker_year'][contract_data.index[0]]
        merged_dataframe_list[i]['cont_indx'] = contract_data['cont_indx'][contract_data.index[0]]

        merged_dataframe_list[i]['change1'] = merged_dataframe_list[i]['close_price'].shift(-2)-merged_dataframe_list[i]['close_price'].shift(-1)
        merged_dataframe_list[i]['change2'] = merged_dataframe_list[i]['close_price'].shift(-3)-merged_dataframe_list[i]['close_price'].shift(-1)
        merged_dataframe_list[i]['change5'] = merged_dataframe_list[i]['close_price'].shift(-6)-merged_dataframe_list[i]['close_price'].shift(-1)
        merged_dataframe_list[i]['change10'] = merged_dataframe_list[i]['close_price'].shift(-11)-merged_dataframe_list[i]['close_price'].shift(-1)
        merged_dataframe_list[i]['change20'] = merged_dataframe_list[i]['close_price'].shift(-21)-merged_dataframe_list[i]['close_price'].shift(-1)
        merged_dataframe_list[i]['change_5'] = merged_dataframe_list[i]['close_price']-merged_dataframe_list[i]['close_price'].shift(5)
        merged_dataframe_list[i]['change_1'] = merged_dataframe_list[i]['close_price']-merged_dataframe_list[i]['close_price'].shift(1)

    data4_tickerhead = pd.concat(merged_dataframe_list)

    if os.path.isfile(presaved_futures_data_folder + '/' + ticker_head + '.pkl'):
        clean_data = data4_tickerhead[np.isfinite(data4_tickerhead['change_5'])]
        clean_data['frame_indx'] = 1
        old_data['frame_indx'] = 0
        merged_data = pd.concat([old_data,clean_data],ignore_index=True)
        merged_data.sort(['cont_indx','settle_date','frame_indx'],ascending=[True,True,False],inplace=True)
        merged_data.drop_duplicates(subset=['settle_date','cont_indx'],take_last=False,inplace=True)
        data4_tickerhead = merged_data.drop('frame_indx',1,inplace=False)

    data4_tickerhead.to_pickle(presaved_futures_data_folder + '/' + ticker_head + '.pkl')

    if 'con' not in kwargs.keys():
        con.close()


def generate_and_update_futures_data_files(**kwargs):

    for ticker_head in cmi.futures_butterfly_strategy_tickerhead_list:
        generate_and_update_futures_data_file_4tickerhead(ticker_head=ticker_head,**kwargs)


