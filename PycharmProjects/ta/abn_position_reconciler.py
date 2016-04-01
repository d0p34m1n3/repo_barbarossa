
import pandas as pd
import shared.directory_names as dn
import contract_utilities.contract_meta_info as cmi
import ta.portfolio_manager as tpm
import math as m
position_file_name = 'abn_position.csv'

conversion_from_abn_ticker_head = {'CBT BEAN MEAL(0106)': 'SM',
                                   'CBT BEAN OIL(0107)': 'BO',
                                   'CBT CORN(01C)': 'C',
                                   'CBT KC RED WHT(01KW)': 'KW',
                                   'CBT SOYBEANS(01S)': 'S',
                                   'CBT WHEAT(01W)': 'W',
                                   'LIVE CATTLE(0248)': 'LC',
                                   'CME LEAN HOGS(0253)': 'LN',
                                   'IMM 3M EUR(03ED)': 'ED',
                                   'NY LT CRUDE(06CO)': 'CL',
                                   'NY NATURAL GAS(06NG)': 'NG',
                                   'NYM RBOB GAS(06RB)': 'RB',
                                   'NYCE COTTON(07CT)': 'CT',
                                   'ICEUS COCOA(09CC)': 'CC',
                                   'ICEUS COFFEEC(09KC)': 'KC',
                                   'ICEUS SUGAR11(09SB)': 'SB',
                                   'ICE BRENT CRD(16B)': 'B'}


def load_and_convert_abn_position_file(**kwargs):

    abn_frame = pd.read_csv(dn.get_directory_name(ext='daily') + '/' + position_file_name)
    abn_frame = abn_frame[abn_frame['Expiration'].notnull()]

    abn_frame['ticker_head'] = [conversion_from_abn_ticker_head[x] for x in abn_frame['Symbol']]

    abn_frame['ticker'] = [abn_frame['ticker_head'].iloc[x] +
                           cmi.full_letter_month_list[int(abn_frame['Expiration'].iloc[x]%100)-1] +
                           str(m.floor(abn_frame['Expiration'].iloc[x]/100))
                               for x in range(len(abn_frame.index))]

    abn_frame['Short'] = -abn_frame['Short']
    abn_frame.fillna(0, inplace=True)
    abn_frame['Long'] = abn_frame['Long'].astype('int')
    abn_frame['Short'] = abn_frame['Short'].astype('int')
    abn_frame['qty'] = abn_frame['Long']+abn_frame['Short']

    return abn_frame[['ticker', 'qty']]


def reconcile_position(**kwargs):

    abn_position = load_and_convert_abn_position_file(**kwargs)

    db_position = tpm.get_position_4portfolio(trade_date_to=20160331)

    merged_data = pd.merge(abn_position,db_position,how='outer',on='ticker')
    merged_data['qty_diff'] = merged_data['qty_x']-merged_data['qty_y']
    return merged_data[merged_data['qty_diff']!=0]









