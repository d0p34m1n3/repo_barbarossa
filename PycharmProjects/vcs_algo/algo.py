
import signals.option_signals as ops
import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import my_sql_routines.my_sql_utilities as msu
import interest_curve.get_rate_from_stir as grfs
import ta.trade_fill_loader as tfl
import shared.calendar_utilities as cu
import shared.utils as su
import contract_utilities.expiration as exp
import opportunity_constructs.intraday_calculations as ic
import ib_api_utils.trade as ib_api_trade
import option_models.quantlib_option_models as qom
import ta.underlying_proxy as up
import decimal as dec
import option_models.utils as omu
import numpy as np
import pandas as pd
import math as m
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *

class Algo(subs.subscription):

    contractDetailReqIdDictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    bid_price_dictionary = {}
    ask_price_dictionary = {}

    market_data_ReqId_dictionary = {}
    contractIDDictionary = {}
    nonfinished_bid_price_list = []
    nonfinished_ask_price_list = []
    nonfinished_bid_iv_list = []
    nonfinished_ask_iv_list = []
    underlying_prices_accumulated_Q = False
    imp_vols_calculated_Q = False

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)

        if tickType==1:
            self.bid_price_dictionary[self.market_data_ReqId_dictionary[reqId]] = price
            if self.market_data_ReqId_dictionary[reqId] in self.nonfinished_bid_price_list:
                self.nonfinished_bid_price_list.remove(self.market_data_ReqId_dictionary[reqId])
        elif tickType==2:
            self.ask_price_dictionary[self.market_data_ReqId_dictionary[reqId]] = price
            if self.market_data_ReqId_dictionary[reqId] in self.nonfinished_ask_price_list:
                self.nonfinished_ask_price_list.remove(self.market_data_ReqId_dictionary[reqId])

        #print('Bid Price Left: ' + str(len(self.nonfinished_bid_price_list)) +  ' Ask Price Left: ' + str(len(self.nonfinished_ask_price_list)))

        if (len(self.nonfinished_bid_price_list) == 0) & (len(self.nonfinished_ask_price_list) == 0) & (not self.underlying_prices_accumulated_Q):
            self.underlying_prices_accumulated_Q = True
            vcs_pairs = self.vcs_pairs

            for i in range(len(vcs_pairs.index)):
                proxy_ticker1 = vcs_pairs.loc[i, 'proxy_ticker1']
                proxy_ticker2 = vcs_pairs.loc[i, 'proxy_ticker2']

                ib_underlying_multiplier = ib_contract.ib_underlying_multiplier_dictionary.get(vcs_pairs.loc[i, 'underlying_tickerhead'], 1)

                if (self.bid_price_dictionary[proxy_ticker1] > 0) and (self.ask_price_dictionary[proxy_ticker1] > 0):
                    vcs_pairs.loc[i, 'proxy_mid_price1'] = (self.bid_price_dictionary[proxy_ticker1] +self.ask_price_dictionary[proxy_ticker1]) / (2*ib_underlying_multiplier)

                if (self.bid_price_dictionary[proxy_ticker2] > 0) and (self.ask_price_dictionary[proxy_ticker2] > 0):
                    vcs_pairs.loc[i, 'proxy_mid_price2'] = (self.bid_price_dictionary[proxy_ticker2] +self.ask_price_dictionary[proxy_ticker2]) / (2*ib_underlying_multiplier)

            vcs_pairs['underlying_mid_price1'] = vcs_pairs['proxy_mid_price1']+ vcs_pairs['add_2_proxy1']
            vcs_pairs['underlying_mid_price2'] = vcs_pairs['proxy_mid_price2'] + vcs_pairs['add_2_proxy2']

            for i in range(len(vcs_pairs.index)):
                for j in [1,2]:

                    if np.isnan(vcs_pairs.loc[i, 'underlying_mid_price' + str(j)]):
                        continue

                    vcs_pairs.loc[i, 'current_strike' + str(j)] = omu.get_strike_4current_delta(ticker=vcs_pairs.loc[i, 'ticker' + str(j)],settle_date=self.report_date,
                                              underlying_current_price=vcs_pairs.loc[i, 'underlying_mid_price' + str(j)],
                                              call_delta_target=0.5,
                                              con=self.con,futures_data_dictionary=self.futures_data_dictionary)

                    print('id:' + str(self.next_val_id) + ', ' + vcs_pairs.loc[i, 'ticker' + str(j)] + ', ' + str(vcs_pairs.loc[i, 'current_strike' + str(j)]))
                    call_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_C_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])
                    self.market_data_ReqId_dictionary[self.next_val_id] = call_option_ticker_string
                    self.nonfinished_bid_price_list.append(call_option_ticker_string)
                    self.nonfinished_ask_price_list.append(call_option_ticker_string)
                    ib_call_option_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=vcs_pairs.loc[i, 'ticker' + str(j)],sec_type='OF', option_type='C', strike=dec.Decimal(vcs_pairs.loc[i, 'current_strike' + str(j)]))
                    vcs_pairs.loc[i, 'call_val_id' + str(j)] = self.next_val_id
                    self.reqMktData(self.next_valid_id(), ib_call_option_contract, "", False, False, [])

                    put_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_P_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])
                    self.market_data_ReqId_dictionary[self.next_val_id] = put_option_ticker_string
                    self.nonfinished_bid_price_list.append(put_option_ticker_string)
                    self.nonfinished_ask_price_list.append(put_option_ticker_string)
                    ib_put_option_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=vcs_pairs.loc[i, 'ticker' + str(j)],sec_type='OF', option_type='P',strike=dec.Decimal(vcs_pairs.loc[i, 'current_strike' + str(j)]))
                    vcs_pairs.loc[i, 'put_val_id' + str(j)] = self.next_val_id
                    self.reqMktData(self.next_valid_id(), ib_put_option_contract, "", False, False, [])

        if (len(self.nonfinished_bid_price_list) == 0) & (len(self.nonfinished_ask_price_list) == 0) & (self.underlying_prices_accumulated_Q) & (not self.imp_vols_calculated_Q):
            self.imp_vols_calculated_Q = True
            self.get_contract_ids()

    def get_contract_ids(self):
        vcs_pairs = self.vcs_pairs
        for i in range(len(vcs_pairs.index)):
            for j in [1, 2]:
                if np.isnan(vcs_pairs.loc[i, 'current_strike' + str(j)]):
                    continue

                call_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_C_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])
                ib_call_option_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=vcs_pairs.loc[i, 'ticker' + str(j)], sec_type='OF', option_type='C',strike=dec.Decimal(vcs_pairs.loc[i, 'current_strike' + str(j)]))
                self.contractDetailReqIdDictionary[self.next_val_id] = call_option_ticker_string
                self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
                self.reqContractDetails(self.next_valid_id(), ib_call_option_contract)
                put_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_P_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])
                ib_put_option_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=vcs_pairs.loc[i, 'ticker' + str(j)], sec_type='OF', option_type='P',strike=dec.Decimal(vcs_pairs.loc[i, 'current_strike' + str(j)]))
                self.contractDetailReqIdDictionary[self.next_val_id] = put_option_ticker_string
                self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
                self.reqContractDetails(self.next_valid_id(), ib_put_option_contract)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contractIDDictionary[self.contractDetailReqIdDictionary[reqId]] = contractDetails.summary.conId

    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        self.nonfinished_contract_detail_ReqId_list.remove(reqId)
        if len(self.nonfinished_contract_detail_ReqId_list)==0:
            self.calc_imp_vols()

    def calc_imp_vols(self):

        vcs_pairs = self.vcs_pairs
        print('YYYYUUUUUPPPP!')

        for i in range(len(vcs_pairs.index)):
            for j in [1,2]:
                if np.isnan(vcs_pairs.loc[i, 'current_strike' + str(j)]):
                    continue
                call_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_C_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])
                put_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_P_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])

                ib_underlying_multiplier = ib_contract.ib_underlying_multiplier_dictionary.get(vcs_pairs.loc[i, 'tickerHead'], 1)

                if (self.bid_price_dictionary[call_option_ticker_string]>0) and (self.ask_price_dictionary[call_option_ticker_string]>0):
                    vcs_pairs.loc[i, 'call_mid_price' + str(j)] = (self.bid_price_dictionary[call_option_ticker_string]+self.ask_price_dictionary[call_option_ticker_string])/(2*ib_underlying_multiplier)

                    option_greeks = qom.get_option_greeks(underlying=vcs_pairs.loc[i, 'underlying_mid_price' + str(j)],
                                                      option_price=vcs_pairs.loc[i, 'call_mid_price' + str(j)],
                                                      strike=vcs_pairs.loc[i, 'current_strike' + str(j)],
                                                      risk_free_rate=vcs_pairs.loc[i, 'interest_date' + str(j)],
                                                      expiration_date=vcs_pairs.loc[i, 'expiration_date' + str(j)],
                                                      calculation_date=self.todays_date,
                                                      option_type='C',
                                                      exercise_type=vcs_pairs.loc[i, 'exercise_type'])

                    vcs_pairs.loc[i, 'call_iv' + str(j)] = 100*option_greeks['implied_vol']

                if (self.bid_price_dictionary[put_option_ticker_string] > 0) and (self.ask_price_dictionary[put_option_ticker_string] > 0):
                    vcs_pairs.loc[i, 'put_mid_price' + str(j)] = (self.bid_price_dictionary[put_option_ticker_string]+self.ask_price_dictionary[put_option_ticker_string])/(2*ib_underlying_multiplier)

                    option_greeks = qom.get_option_greeks(underlying=vcs_pairs.loc[i, 'underlying_mid_price' + str(j)],
                                                      option_price=vcs_pairs.loc[i, 'put_mid_price' + str(j)],
                                                      strike=vcs_pairs.loc[i, 'current_strike' + str(j)],
                                                      risk_free_rate=vcs_pairs.loc[i, 'interest_date' + str(j)],
                                                      expiration_date=vcs_pairs.loc[i, 'expiration_date' + str(j)],
                                                      calculation_date=self.todays_date,
                                                      option_type='P',
                                                      exercise_type=vcs_pairs.loc[i, 'exercise_type'])

                    vcs_pairs.loc[i, 'put_iv' + str(j)] = 100 * option_greeks['implied_vol']

        for j in [1,2]:
            vcs_pairs['straddle_iv' + str(j)] = (vcs_pairs['put_iv' + str(j)] + vcs_pairs['call_iv' + str(j)])/2
            vcs_pairs['straddle_price' + str(j)] = (vcs_pairs['call_mid_price' + str(j)] + vcs_pairs['put_mid_price' + str(j)])

        vcs_pairs['current_atm_vol_ratio'] = vcs_pairs['straddle_iv1']/vcs_pairs['straddle_iv2']

        for i in range(len(vcs_pairs.index)):
            if np.isnan(vcs_pairs.loc[i, 'current_atm_vol_ratio']):
                continue

            intraday_vcs_output = ic.get_intraday_vcs(report_date=self.report_date,ticker1=vcs_pairs.loc[i, 'ticker1'],ticker2=vcs_pairs.loc[i, 'ticker2'],atm_vol_ratio=vcs_pairs.loc[i, 'current_atm_vol_ratio'])
            vcs_pairs.loc[i,'QC'] = intraday_vcs_output['Q']
            vcs_pairs.loc[i, 'Q1C'] = intraday_vcs_output['Q1']
            vcs_pairs.loc[i, 'validQ'] = intraday_vcs_output['validQ']

        writer = pd.ExcelWriter('C:\Research\daily\kuzu.xlsx')
        vcs_pairs.to_excel(writer, 'Sheet1')
        writer.save()
        self.vcs_pairs = vcs_pairs
        self.prepare_orders()

    def prepare_orders(self):
        vcs_pairs = self.vcs_pairs
        for i in range(len(vcs_pairs.index)):
            if vcs_pairs.loc[i,'validQ']:
                if (vcs_pairs.loc[i,'QC']<=30) and (vcs_pairs.loc[i,'Q']<=30):
                    trade_decision='SELL'
                    quantity = round(vcs_pairs.loc[i,'long_quantity']/2)
                elif (vcs_pairs.loc[i,'QC']>=70) and (vcs_pairs.loc[i,'Q']>=70):
                    trade_decision = 'BUY'
                    quantity =  round(vcs_pairs.loc[i,'short_quantity']/2)
                else:
                    continue

                exchange = cmi.get_ib_exchange_name(vcs_pairs.loc[i,'tickerHead'])
                option_tick_size = cmi.option_tick_size[vcs_pairs.loc[i,'tickerHead']]

                ib_ticker_head = su.get_key_in_dictionary(dictionary_input=tfl.conversion_from_tt_ticker_head,
                                                      value=vcs_pairs.loc[i,'tickerHead'])

                ib_underlying_multiplier = ib_contract.ib_underlying_multiplier_dictionary.get(vcs_pairs.loc[i, 'tickerHead'], 1)

                spread_contract = Contract()
                spread_contract.symbol = ib_ticker_head
                spread_contract.secType = "BAG"
                spread_contract.currency = "USD"
                spread_contract.exchange = exchange
                spread_contract.comboLegs = []
                action_dict = {1: 'SELL',2:'BUY'}

                for j in [1,2]:

                    call_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_C_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])
                    put_option_ticker_string = vcs_pairs.loc[i, 'ticker' + str(j)] + '_P_' + str(vcs_pairs.loc[i, 'current_strike' + str(j)])

                    leg1 = ComboLeg()
                    leg1.conId = self.contractIDDictionary[call_option_ticker_string]
                    leg1.ratio = 1
                    leg1.action = action_dict[j]
                    leg1.exchange = exchange

                    leg2 = ComboLeg()
                    leg2.conId = self.contractIDDictionary[put_option_ticker_string]
                    leg2.ratio = 1
                    leg2.action = action_dict[j]
                    leg2.exchange = exchange

                    spread_contract.comboLegs.append(leg1)
                    spread_contract.comboLegs.append(leg2)

                mid_price_db_raw = vcs_pairs.loc[i, 'call_mid_price2'] + vcs_pairs.loc[i, 'put_mid_price2'] - vcs_pairs.loc[i, 'call_mid_price1'] - vcs_pairs.loc[i, 'put_mid_price1']
                print('mid_price_db_raw: ' + str(mid_price_db_raw))
                mid_price_db_raw_ticks = mid_price_db_raw/option_tick_size
                print('mid_price_db_raw_ticks: ' + str(mid_price_db_raw_ticks))

                if trade_decision == 'BUY':
                    order_price = ib_underlying_multiplier*m.floor(mid_price_db_raw_ticks)*option_tick_size
                elif trade_decision == 'SELL':
                    order_price = ib_underlying_multiplier * m.ceil(mid_price_db_raw_ticks) * option_tick_size

                print(vcs_pairs.loc[i, 'ticker1'] + '_' + vcs_pairs.loc[i, 'ticker2']  + '_' +  str(vcs_pairs.loc[i, 'current_strike1'])  + '_' +  str(vcs_pairs.loc[i, 'current_strike2']))
                print(trade_decision + ', quantity: ' + str(quantity)+ ', price: ' + str(order_price))
                continue_q = 'n'
                continue_q = input('Continue? (y/n): ')

                if continue_q == 'y':
                    self.placeOrder(self.next_valid_id(), spread_contract,ib_api_trade.ComboLimitOrder(trade_decision, quantity, order_price, False))


    def main_run(self):

        vcs_pairs = self.vcs_pairs
        proxy_ticker_list = list(set(vcs_pairs['proxy_ticker1'].unique())|set(vcs_pairs['proxy_ticker2'].unique()))

        self.nonfinished_bid_price_list.extend(proxy_ticker_list)
        self.nonfinished_ask_price_list.extend(proxy_ticker_list)

        for i in range(len(proxy_ticker_list)):
            print('id:' + str(self.next_val_id) + ', ' + proxy_ticker_list[i])
            self.market_data_ReqId_dictionary[self.next_val_id] = proxy_ticker_list[i]
            outright_ib_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=proxy_ticker_list[i],sec_type='F')
            self.reqMktData(self.next_valid_id(), outright_ib_contract, "", False, False, [])


        print('emre')