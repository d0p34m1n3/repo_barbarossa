
import shared.directory_names as dn
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import ta.trade_fill_loader as tfl
import datetime as dt
import shared.utils as su
import os.path
import pandas as pd
import os.path
import numpy as np


def get_ttapi_filename(**kwargs):

    ticker = kwargs['ticker']
    contract_specs_output = cmi.get_contract_specs(ticker)

    ticker_head = contract_specs_output['ticker_head']
    exchange_traded = cmi.get_exchange_traded(ticker_head)

    ttapi_ticker_head = su.get_key_in_dictionary(dictionary_input=tfl.conversion_from_tt_ticker_head, value=ticker_head)

    if exchange_traded == 'CME':
        exchange_string = 'CME'
    elif exchange_traded == 'ICE':
        exchange_string = 'ICE_IPE'

    maturity_string = dt.date(contract_specs_output['ticker_year'],contract_specs_output['ticker_month_num'],1).strftime('%b%y')

    return exchange_string + ' ' + ttapi_ticker_head + ' ' + maturity_string + '.csv'


def load_csv_file_4ticker(**kwargs):

    ticker = kwargs['ticker']

    if 'folder_date' in kwargs.keys():
        folder_date = kwargs['folder_date']
    else:
        folder_date = exp.doubledate_shift_bus_days()

    data_dir = dn.get_dated_directory_extension(ext='intraday_ttapi_data', folder_date=folder_date)

    file_name = get_ttapi_filename(ticker=ticker)

    if os.path.isfile(data_dir + '/' + file_name):
        data_frame_out = pd.read_csv(data_dir + '/' + file_name,names=['time','field','value'],dtype={2: 'str'})
        data_frame_out['time'] = pd.to_datetime(data_frame_out['time'])
    else:
        data_frame_out = pd.DataFrame(columns=['time','field','value'])


    return data_frame_out


def get_book_snapshot_4ticker(**kwargs):

    if 'folder_date' in kwargs.keys():
        folder_date = kwargs['folder_date']
    else:
        folder_date = exp.doubledate_shift_bus_days()

    if 'freq_str' in kwargs.keys():
        freq_str = kwargs['freq_str']
    else:
        freq_str = 'T'

    ticker = kwargs['ticker']

    data_dir = dn.get_dated_directory_extension(ext='intraday_ttapi_data_fixed_interval', folder_date=folder_date)
    file_name = data_dir + '/' + ticker + '_' + freq_str + '.pkl'

    if os.path.isfile(file_name):
        book_snapshot = pd.read_pickle(file_name)
        return book_snapshot

    data_frame_out = load_csv_file_4ticker(**kwargs)

    if data_frame_out.empty:
        return pd.DataFrame(columns=['best_bid_p','best_bid_q','best_ask_p','best_ask_q'])

    start_datetime = dt.datetime.utcfromtimestamp(data_frame_out['time'].values[0].tolist()/1e9).replace(microsecond=0, second=0)
    end_datetime = dt.datetime.utcfromtimestamp(data_frame_out['time'].values[-1].tolist()/1e9).replace(microsecond=0, second=0)

    merged_index = pd.date_range(start=start_datetime,end=end_datetime,freq=freq_str)

    data_frame_out.set_index('time', inplace=True, drop=True)
    best_bid_p = data_frame_out[data_frame_out['field'] == 'BestBidPrice']
    best_bid_p = best_bid_p.groupby(best_bid_p.index).last()
    best_bid_p = best_bid_p.reindex(merged_index,method='pad')

    best_bid_q = data_frame_out[data_frame_out['field'] == 'BestBidQuantity']
    best_bid_q = best_bid_q.groupby(best_bid_q.index).last()
    best_bid_q = best_bid_q.reindex(merged_index,method='pad')

    best_ask_p = data_frame_out[data_frame_out['field'] == 'BestAskPrice']
    best_ask_p = best_ask_p.groupby(best_ask_p.index).last()
    best_ask_p = best_ask_p.reindex(merged_index,method='pad')

    best_ask_q = data_frame_out[data_frame_out['field'] == 'BestAskQuantity']
    best_ask_q = best_ask_q.groupby(best_ask_q.index).last()
    best_ask_q = best_ask_q.reindex(merged_index,method='pad')

    book_snapshot = pd.DataFrame(index=merged_index)

    book_snapshot['best_bid_p'] = best_bid_p['value'].astype('float64')
    book_snapshot['best_bid_q'] = best_bid_q['value']
    book_snapshot['best_ask_p'] = best_ask_p['value'].astype('float64')
    book_snapshot['best_ask_q'] = best_ask_q['value']

    ticker_head = cmi.get_contract_specs(kwargs['ticker'])['ticker_head']

    book_snapshot['best_bid_p'] = [tfl.convert_trade_price_from_tt(price=x,ticker_head=ticker_head) for x in book_snapshot['best_bid_p']]
    book_snapshot['best_ask_p'] = [tfl.convert_trade_price_from_tt(price=x,ticker_head=ticker_head) for x in book_snapshot['best_ask_p']]

    book_snapshot.to_pickle(file_name)
    return book_snapshot








