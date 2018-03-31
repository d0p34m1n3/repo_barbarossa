
from ibapi.contract import *
import contract_utilities.contract_meta_info as cmi
import decimal as dec
import datetime as dt
import shared.utils as su
import math as m

conversion_from_ib_ticker_head = {'CL': 'CL',
                                  'HO': 'HO',
                                  'RB': 'RB',
                                  'GE': 'ED',
                                  'ZC': 'C',
                                  'ZW': 'W',
                                  'ZS': 'S',
                                  'ZM': 'SM',
                                  'ZL': 'BO',
                                  'KE': 'KW',
                                  'NG': 'NG',
                                  'LE': 'LC', 'HE': 'LN', 'GF': 'FC',
                                  'ES': 'ES', 'NQ': 'NQ',
                                  'AUD': 'AD', 'CAD': 'CD', 'EUR': 'EC', 'JPY': 'JY', 'GBP': 'BP',
                                  'M6E': 'MEC',
                                  'ZT': 'TU', 'ZF': 'FV', 'ZN': 'TY', 'ZB': 'US',
                                  'GC': 'GC', 'SI': 'SI',
                                  'IPE e-Brent': 'B',
                                  'Coffee C': 'KC',
                                  'Cocoa': 'CC',
                                  'Sugar No 11': 'SB',
                                  'Cotton No 2': 'CT',
                                  'FCOJ A': 'OJ'}

ib_option_trading_class_dictionary = {'AD': 'ADU','CD': 'CAU', 'EC': 'EUU', 'BP': 'GBU', 'JY': 'JPU',
                                    'C': 'OZC', 'S': 'OZS', 'BO': 'OZL', 'SM': 'OZM', 'W': 'OZW', 'LN': 'HE', 'LC': 'LE',
                                    'CL': 'LO', 'NG': 'ON', 'GC': 'OG', 'SI': 'SO', 'ES': 'ES',
                                    'TU': 'OZT', 'FV': 'OZF', 'TY': 'OZN', 'US': 'OZB'}

ib_strike_multiplier_dictionary = {'BO': 0.01, 'S': 0.01, 'C': 0.01, 'W': 0.01, 'JY': 0.0000001, 'LC': 0.01, 'LN': 0.01}

ib_underlying_multiplier_dictionary = {'JY': 0.0000001}


def get_ib_contract_from_db_ticker(**kwargs):

    sec_type = kwargs['sec_type']
    ticker = kwargs['ticker']
    contract_out = Contract()
    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']

    if sec_type in ['F', 'OF']:
        secType = "FUT"
        currency = 'USD'

        ib_ticker_head = su.get_key_in_dictionary(dictionary_input=conversion_from_ib_ticker_head,
                                                  value=contract_specs_output['ticker_head'])

        ib_contract_month = str(contract_specs_output['ticker_year']*100 + contract_specs_output['ticker_month_num'])

        exchange = cmi.get_ib_exchange_name(contract_specs_output['ticker_head'])


        contract_out.secType = secType
        contract_out.symbol = ib_ticker_head
        contract_out.exchange = exchange
        contract_out.currency = currency
        contract_out.lastTradeDateOrContractMonth = ib_contract_month
    if sec_type=='OF':
        contract_out.secType = "FOP"
        contract_out.right = kwargs['option_type']
        contract_out.strike = kwargs['strike']*dec.Decimal(ib_strike_multiplier_dictionary.get(ticker_head,1))
        contract_out.tradingClass = ib_option_trading_class_dictionary[ticker_head]

    return contract_out

def get_db_ticker_from_ib_contract(**kwargs):

    ib_contract = kwargs['ib_contract']
    contract_id_dictionary = kwargs['contract_id_dictionary']

    return su.get_key_in_dictionary(dictionary_input=contract_id_dictionary,value=ib_contract.conId)

def main():
    print(get_ib_contract_from_db_ticker(ticker='BOZ2017',sec_type='F'))

if __name__ == "__main__":
    main()