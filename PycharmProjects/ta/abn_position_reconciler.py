
import pandas as pd
import shared.directory_names as dn
import contract_utilities.contract_meta_info as cmi
import ta.portfolio_manager as tpm
import math as m
import shared.calendar_utilities as cu
position_file_name = 'abn_position.csv'

conversion_from_abn_ticker_head = {'CBT BEAN MEAL(0106)': 'SM',
                                   'CBT BEAN OIL(0107)': 'BO',
                                   'CBT CORN(01C)': 'C',
                                   'CBT KC RED WHT(01KW)': 'KW',
                                   'CBT SOYBEANS(01S)': 'S',
                                   'CBT WHEAT(01W)': 'W',
                                   'LIVE CATTLE(0248)': 'LC',
                                   'CATTLE(0248)': 'LC',
                                   'CME FEEDERS(0262)': 'FC',
                                   'CME LEAN HOGS(0253)': 'LN',
                                   'IMM 3M EUR(03ED)': 'ED',
                                   'NY LT CRUDE(06CO)': 'CL',
                                   'NY NATURAL GAS(06NG)': 'NG',
                                   'NYM RBOB GAS(06RB)': 'RB',
                                   'NYM NYHRBRULSD(06HO)': 'HO',
                                   'NYCE COTTON(07CT)': 'CT',
                                   'ICEUS COCOA(09CC)': 'CC',
                                   'ICEUS COFFEEC(09KC)': 'KC',
                                   'ICEUS SUGAR11(09SB)': 'SB',
                                   'ICE BRENT CRD(16B)': 'B',
                                   'JAPANESE YEN(03J1)': 'JY',
                                   'CMX SILVER(08SI)': 'SI',
                                   'CMX GOLD(08GX)': 'GC',
                                   'IMM JPY(03J1)': 'JY',
                                   'IMM EURO FX(03EC)': 'EC',
                                   'EMINI S&P 500(03ES)': 'ES',
                                   'NYCE FROZEN OJ(07JO)': 'OJ'}

abn_strike_multiplier = {'W': 100,
                         'S': 100,
                         'C': 100,
                         'SI': 0.01,
                         'JY': 1000}


def get_abn_strike_multiplier(ticker_head):

    if ticker_head in abn_strike_multiplier.keys():
        strike_multiplier = abn_strike_multiplier[ticker_head]
    else:
        strike_multiplier = 1

    return strike_multiplier


def load_and_convert_abn_position_file(**kwargs):

    abn_frame = pd.read_csv(dn.get_directory_name(ext='daily') + '/' + position_file_name)
    abn_frame = abn_frame[abn_frame['Expiration'].notnull()]

    abn_frame['ticker_head'] = [conversion_from_abn_ticker_head[x] for x in abn_frame['Symbol']]

    abn_frame['strike_multiplier'] = [get_abn_strike_multiplier(x) for x in abn_frame['ticker_head']]

    abn_frame['ticker'] = [abn_frame['ticker_head'].iloc[x] +
                           cmi.full_letter_month_list[int(abn_frame['Expiration'].iloc[x]%100)-1] +
                           str(m.floor(abn_frame['Expiration'].iloc[x]/100))
                               for x in range(len(abn_frame.index))]

    abn_frame['Short'] = -abn_frame['Short']

    abn_frame['Long'] = abn_frame['Long'].fillna(0).astype('int')
    abn_frame['Short'] = abn_frame['Short'].fillna(0).astype('int')
    abn_frame['qty'] = abn_frame['Long']+abn_frame['Short']

    abn_frame.rename(columns={'Strike': 'strike_price','PutCall': 'option_type'},inplace=True)
    abn_frame['strike_price'] = abn_frame['strike_multiplier']*abn_frame['strike_price']

    abn_frame['instrumet'] = 'F'
    option_indx = abn_frame['strike_price'].notnull()
    abn_frame['instrumet'][option_indx] = 'O'

    abn_frame['generalized_ticker'] = abn_frame['ticker']
    abn_frame['generalized_ticker'][option_indx] = abn_frame['ticker'][option_indx] + '-' + \
                                                         abn_frame['option_type'][option_indx] + '-' + \
                                                         abn_frame['strike_price'][option_indx].astype(str)

    abn_frame = abn_frame[abn_frame['qty'] != 0]
    abn_frame['generalized_ticker'] = [x.rstrip('0').rstrip('.') for x in abn_frame['generalized_ticker']]

    return abn_frame[['generalized_ticker', 'qty']]


def reconcile_position(**kwargs):

    abn_position = load_and_convert_abn_position_file(**kwargs)

    db_position = tpm.get_position_4portfolio(trade_date_to=cu.get_doubledate())

    db_position['generalized_ticker'] = [x.rstrip('0').rstrip('.') for x in db_position['generalized_ticker']]

    merged_data = pd.merge(abn_position,db_position,how='outer',on='generalized_ticker')
    merged_data['qty_diff'] = merged_data['qty_x'].astype('float64')-merged_data['qty_y'].astype('float64')
    return merged_data[merged_data['qty_diff']!=0]









