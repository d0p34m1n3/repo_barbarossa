
import ib_api_utils.subscription as subs
import contract_utilities.contract_meta_info as cmi
import ib_api_utils.ib_contract as ib_contract
import ib_api_utils.trade as ib_api_trade
import ib_api_utils.execution as exec
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
import copy as cpy
import shared.log as lg
import numpy as np
import math as mth

class Algo(subs.subscription):

    ticker_list = []
    tick_size_dictionary = {}
    contractDetailReqIdDictionary = {}
    market_data_ReqId_dictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    contractIDDictionary = {}
    bid_price_dictionary = {}
    ask_price_dictionary = {}
    bid_quantity_dictionary = {}
    ask_quantity_dictionary = {}
    ib_contract_dictionary = {}

    nonfinished_bid_price_list = []
    nonfinished_ask_price_list = []
    nonfinished_bid_quantity_list = []
    nonfinished_ask_quantity_list = []
    trade_call_initiated_q = False

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contractIDDictionary[self.contractDetailReqIdDictionary[reqId]] = contractDetails.summary.conId

    def contractDetailsEnd(self, reqId: int):
            super().contractDetailsEnd(reqId)
            self.nonfinished_contract_detail_ReqId_list.remove(reqId)
            self.nonfinished_ticker_list.remove(self.contractDetailReqIdDictionary[reqId])
            if len(self.nonfinished_contract_detail_ReqId_list)==0:
                self.request_market_data()

    def orderStatus(self, orderId: OrderId, status: str, filled: float,remaining: float, avgFillPrice: float, permId: int,parentId: int, lastFillPrice: float, clientId: int,whyHeld: str):
        super().orderStatus(orderId, status, filled, remaining,avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld)

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)

        ticker_str = self.market_data_ReqId_dictionary[reqId]

        if tickType==1:
            self.bid_price_dictionary[ticker_str] = price
            if ticker_str in self.nonfinished_bid_price_list:
                self.nonfinished_bid_price_list.remove(ticker_str)
        elif tickType==2:
            self.ask_price_dictionary[ticker_str] = price
            if ticker_str in self.nonfinished_ask_price_list:
                self.nonfinished_ask_price_list.remove(ticker_str)

        if (len(self.nonfinished_bid_price_list)==0)&(len(self.nonfinished_ask_price_list)==0)&\
                (len(self.nonfinished_bid_quantity_list)==0)&(len(self.nonfinished_ask_quantity_list)==0)&(~self.trade_call_initiated_q):
            self.trade_call()

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)

        ticker_str = self.market_data_ReqId_dictionary[reqId]

        if tickType == 0:
            self.bid_quantity_dictionary[ticker_str] = size
            if ticker_str in self.nonfinished_bid_quantity_list:
                self.nonfinished_bid_quantity_list.remove(ticker_str)
        if tickType == 3:
            self.ask_quantity_dictionary[ticker_str] = size
            if ticker_str in self.nonfinished_ask_quantity_list:
                self.nonfinished_ask_quantity_list.remove(ticker_str)

    def trade_call(self):

        self.trade_call_initiated_q = True
        ticker_str = 'HOK2018'

        bid_price = self.bid_price_dictionary[ticker_str]
        ask_price = self.ask_price_dictionary[ticker_str]

        bid_quantity = self.bid_quantity_dictionary[ticker_str]
        ask_quantity = self.ask_quantity_dictionary[ticker_str]

        tick_size = self.tick_size_dictionary[ticker_str]

        bid_ask_in_ticks = int(round((ask_price-bid_price)/tick_size))
        half_bid_ask = int(mth.floor(bid_ask_in_ticks/2))

        limit_price = exec.get_midprice(bid_price=bid_price,ask_price=ask_price,bid_quantity=bid_quantity,ask_quantity=ask_quantity,tick_size=tick_size,direction=1)
        print(limit_price)
        orderID = self.next_val_id
        self.placeOrder(self.next_valid_id(), self.ib_contract_dictionary[ticker_str],ib_api_trade.LimitOrder('BUY', 1, limit_price-15*tick_size))
        self.placeOrder(orderID,self.ib_contract_dictionary[ticker_str],ib_api_trade.LimitOrder('BUY', 2, limit_price-17*tick_size))



    def request_market_data(self):

        ticker_list = self.ticker_list
        self.nonfinished_bid_price_list.extend(ticker_list)
        self.nonfinished_ask_price_list.extend(ticker_list)
        self.nonfinished_bid_quantity_list.extend(ticker_list)
        self.nonfinished_ask_quantity_list.extend(ticker_list)

        for i in range(len(ticker_list)):
            self.bid_price_dictionary[ticker_list[i]] = np.nan
            self.ask_price_dictionary[ticker_list[i]] = np.nan
            self.bid_quantity_dictionary[ticker_list[i]] = np.nan
            self.ask_quantity_dictionary[ticker_list[i]] = np.nan

            outright_ib_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.ib_contract_dictionary[ticker_list[i]] = outright_ib_contract
            self.market_data_ReqId_dictionary[self.next_val_id] = ticker_list[i]
            self.log.info('req id: ' + str(self.next_val_id) + ', outright_ticker:' + str(ticker_list[i]));
            self.reqMktData(self.next_valid_id(), outright_ib_contract, "", False, False, [])




    def main_run(self):
        ticker_list = self.ticker_list
        self.nonfinished_ticker_list = cpy.deepcopy(ticker_list)

        for i in range(len(ticker_list)):
            contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.contractDetailReqIdDictionary[self.next_val_id] = ticker_list[i]
            self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
            self.reqContractDetails(self.next_valid_id(), contract_i)



def main():
    app = Algo()

    ticker_list = ['HOK2018']
    app.tick_size_dictionary = {x: cmi.tick_size[cmi.get_contract_specs(x)['ticker_head']] for x in ticker_list}
    app.log = lg.get_logger(file_identifier='ib_itf', log_level='INFO')
    app.ticker_list = ticker_list
    app.connect(client_id=6)
    app.run()

if __name__ == "__main__":
    main()
