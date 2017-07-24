
from ibapi.contract import *
import contract_utilities.contract_meta_info as cmi
import datetime as dt
import ta.trade_fill_loader as tfl
import shared.utils as su
import math as m

def get_ib_contract_from_db_ticker(**kwargs):

    sec_type = kwargs['sec_type']
    ticker = kwargs['ticker']

    if sec_type=='F':
        secType = "FUT"
        currency = 'USD'
        contract_specs_output = cmi.get_contract_specs(ticker)
        ticker_head = contract_specs_output['ticker_head']

        ib_ticker_head = su.get_key_in_dictionary(dictionary_input=tfl.conversion_from_tt_ticker_head,
                                                  value=contract_specs_output['ticker_head'])

        ib_contract_month = str(contract_specs_output['ticker_year']*100 + contract_specs_output['ticker_month_num'])

        exchange = cmi.get_ib_exchange_name(contract_specs_output['ticker_head'])

    contract_out = Contract()
    contract_out.secType = secType
    contract_out.symbol = ib_ticker_head
    contract_out.exchange = exchange
    contract_out.currency = currency
    contract_out.lastTradeDateOrContractMonth = ib_contract_month

    return contract_out

def get_db_ticker_from_ib_contract(**kwargs):

    ib_contract = kwargs['ib_contract']
    contract_id_dictionary = kwargs['contract_id_dictionary']

    return su.get_key_in_dictionary(dictionary_input=contract_id_dictionary,value=ib_contract.conId)

def main():
    print(get_ib_contract_from_db_ticker(ticker='BOZ2017',sec_type='F'))

if __name__ == "__main__":
    main()