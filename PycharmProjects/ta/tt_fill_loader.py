
import shared.directory_names as dn
import pandas as pd
import os as os
import datetime as dt
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
pd.options.mode.chained_assignment = None
import ta.strategy as ts
import numpy as np

conversion_from_tt_ticker_head = {'CL': 'CL',
                                  'HO': 'HO',
                                  'RB': 'RB',
                                  'ZC': 'C',
                                  'ZS': 'S',
                                  'ZM': 'SM',
                                  'ZL': 'BO',
                                  'KE': 'KW',
                                  'NG': 'NG',
                                  'LE': 'LC', 'HE': 'LN', 'GF': 'FC',
                                  'IPE e-Brent': 'B',
                                  'Coffee C': 'KC',
                                  'Sugar No 11': 'SB'}
product_type_instrument_conversion = {'Future': 'F'}


def convert_trade_price_from_tt(**kwargs):

    ticker_head = kwargs['ticker_head']
    price = kwargs['price']

    if ticker_head in ['CL','BO']:
        converted_price = price/100
    elif ticker_head in ['B', 'KC', 'SB']:
        converted_price = price
    elif ticker_head in ['HO','RB']:
        converted_price = price/10000
    elif ticker_head in ['LC','LN','FC','NG']:
        converted_price = price/1000
    elif ticker_head in ['C','S','KW']:
        converted_price = np.floor(price/10)+(price%10)*0.125
    elif ticker_head == 'SM':
        converted_price = price/10

    return converted_price


def load_latest_tt_fills(**kwargs):

    file_list = os.listdir(dn.tt_fill_directory)
    num_files = len(file_list)

    time_list = []

    for i in range(num_files):
        time_list.append(os.path.getmtime(dn.tt_fill_directory + '/' + file_list[i]))

    loc_latest_file = time_list.index(max(time_list))

    tt_export_frame = pd.read_csv(dn.tt_fill_directory + '/' + file_list[loc_latest_file])

    tt_export_frame_filtered = tt_export_frame[tt_export_frame['Product Type']=='Future']

    return tt_export_frame_filtered


def get_formatted_tt_fills(**kwargs):

    fill_frame = load_latest_tt_fills()

    str_indx = fill_frame['Contract'].values[0].find('-')

    if str_indx==2:
        date_format = '%y-%b'
    elif str_indx==-1:
        date_format = '%b%y'

    datetime_conversion = [dt.datetime.strptime(x,date_format) for x in fill_frame['Contract']]
    fill_frame['ticker_year'] = [x.year for x in datetime_conversion]
    fill_frame['ticker_month'] = [x.month for x in datetime_conversion]
    fill_frame['ticker_head'] = [conversion_from_tt_ticker_head[x] for x in fill_frame['Product']]

    fill_frame['ticker'] = [fill_frame.loc[x,'ticker_head'] +
                            cmi.full_letter_month_list[fill_frame.loc[x,'ticker_month']-1] +
                            str(fill_frame.loc[x,'ticker_year']) for x in fill_frame.index]

    fill_frame['trade_price'] = [convert_trade_price_from_tt(price=fill_frame.loc[x,'Price'],ticker_head=fill_frame.loc[x,'ticker_head'])
                                 for x in fill_frame.index]

    fill_frame['PQ'] = fill_frame['trade_price']*fill_frame['Qty']

    grouped = fill_frame.groupby(['ticker','B/S'])

    aggregate_trades = pd.DataFrame()
    aggregate_trades['trade_price'] = grouped['PQ'].sum()/grouped['Qty'].sum()
    aggregate_trades['trade_quantity'] = grouped['Qty'].sum()
    aggregate_trades.loc[(slice(None),'S'),'trade_quantity']=-aggregate_trades.loc[(slice(None),'S'),'trade_quantity']
    aggregate_trades['ticker'] = grouped['ticker'].first()
    aggregate_trades['ticker_head'] = grouped['ticker_head'].first()
    aggregate_trades['instrument'] = [product_type_instrument_conversion[x] for x in grouped['Product Type'].first()]

    aggregate_trades['option_type'] = None
    aggregate_trades['strike_price'] = None
    aggregate_trades['real_tradeQ'] = True

    return {'raw_trades': fill_frame, 'aggregate_trades': aggregate_trades}


def assign_trades_2strategies(**kwargs):

    tt_fill_out = get_formatted_tt_fills()
    aggregate_trades = tt_fill_out['aggregate_trades']

    allocation_frame = pd.read_excel(dn.get_directory_name(ext='daily') + '/' + 'trade_allocation.xlsx')
    combined_list = [None]*len(allocation_frame.index)

    for i in range(len(allocation_frame.index)):

        if allocation_frame['criteria'][i]=='tickerhead':

            selected_trades = aggregate_trades[aggregate_trades['ticker_head'] == allocation_frame['value'][i]]

        elif allocation_frame['criteria'][i]=='ticker':

            selected_trades = aggregate_trades[aggregate_trades['ticker'] == allocation_frame['value'][i]]

        combined_list[i] = selected_trades[['ticker','option_type','strike_price','trade_price','trade_quantity','instrument','real_tradeQ']]
        combined_list[i]['alias'] = allocation_frame['alias'][i]

    return pd.concat(combined_list).reset_index(drop=True)


def load_tt_trades(**kwargs):

    trade_frame = assign_trades_2strategies()
    con = msu.get_my_sql_connection(**kwargs)
    ts.load_trades_2strategy(trade_frame=trade_frame,con=con,**kwargs)

    if 'con' not in kwargs.keys():
        con.close()