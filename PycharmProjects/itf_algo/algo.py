

import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
import opportunity_constructs.overnight_calendar_spreads as ocs
import matplotlib.pyplot as plt
import copy as cpy
import ta.trade_fill_loader as tfl
import ta.strategy as ts
import ta.position_manager as pm
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import ib_api_utils.trade as ib_api_trade
import api_utils.portfolio as aup
import shared.utils as su
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import math as mth
import threading as thr
import datetime as dt
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
from ibapi.order import *
from ibapi.order_state import *
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.order_condition import *
import ib_api_utils.execution as exec

class Algo(subs.subscription):

    contractDetailReqIdDictionary = {}
    market_data_ReqId_dictionary = {}
    bar_data_ReqId_dictionary = {}
    contractIDDictionary = {}
    bid_price_dictionary = {}
    ask_price_dictionary = {}
    bid_quantity_dictionary = {}
    ask_quantity_dictionary = {}
    ib_contract_dictionary = {}

    high_price_dictionary = {}
    low_price_dictionary = {}
    close_price_dictionary = {}
    open_price_dictionary = {}
    bar_date_dictionary = {}
    candle_frame_dictionary = {}

    long_breakout_price_dictionary = {}
    short_breakout_price_dictionary = {}

    long_stop_price_dictionary = {}
    short_stop_price_dictionary = {}

    long_target_price_dictionary = {}
    short_target_price_dictionary = {}

    long_trade_possible_dictionary = {}
    short_trade_possible_dictionary = {}

    stop_adjustment_possible_dictionary = {}
    stop_direction_dictionary = {}
    stop_price_dictionary = {}
    stop_ticker_dictionary = {}

    trade_entry_price_dictionary = {}

    tick_size_dictionary = {}
    daily_sd_dictionary = {}

    working_order_id_dictionary = {}
    order_filled_dictionary = {}
    order_size_dictionary = {}

    current_high_price_dictionary = {}
    current_low_price_dictionary = {}

    spread_contract_dictionary = {}
    order_filled_dictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    ticker_list = []
    price_request_dictionary = {'spread': [] ,'outright': []}
    nonfinished_bid_price_list = []
    nonfinished_ask_price_list = []
    nonfinished_bid_quantity_list = []
    nonfinished_ask_quantity_list = []
    nonfinished_historical_data_list = []
    period_call_initiated_q = False
    min_avg_volume_limit = 100
    bet_size = 240
    total_traded_volume_max_before_user_confirmation = 90
    total_traded_volume_since_last_confirmation = 0
    total_volume_traded = 0
    max_num_bets = 1
    num_bets = 0
    realized_pnl = 0
    total_contracts_traded = 0
    max_total_contracts_traded = 10
    db_alias = ''

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contractIDDictionary[self.contractDetailReqIdDictionary[reqId]] = contractDetails.summary.conId

    def contractDetailsEnd(self, reqId: int):
            super().contractDetailsEnd(reqId)
            self.nonfinished_contract_detail_ReqId_list.remove(reqId)
            self.nonfinished_ticker_list.remove(self.contractDetailReqIdDictionary[reqId])
            if len(self.nonfinished_contract_detail_ReqId_list)==0:
                self.request_market_data()
                self.request_historical_bar_data()

    def historicalData(self, reqId: TickerId, date: str, open: float, high: float,
        low: float, close: float, volume: int, barCount: int,
        WAP: float, hasGaps: int):

        #print("HistoricalData. ", self.bar_data_ReqId_dictionary[reqId], " Date:", date, "Open:", open,"High:", high, "Low:", low, "Close:", close, "Volume:", volume,"Count:", barCount, "WAP:", WAP)

        ticker_str = self.bar_data_ReqId_dictionary[reqId]

        self.low_price_dictionary[ticker_str].append(low)
        self.high_price_dictionary[ticker_str].append(high)
        self.close_price_dictionary[ticker_str].append(close)
        self.open_price_dictionary[ticker_str].append(open)
        self.bar_date_dictionary[ticker_str].append(dt.datetime.strptime(date, '%Y%m%d %H:%M:%S'))



    def historicalDataEnd(self, reqId: int, start: str, end: str):
             super().historicalDataEnd(reqId, start, end)
             ticker_str = self.bar_data_ReqId_dictionary[reqId]

             candle_frame = pd.DataFrame.from_items([('bar_datetime', self.bar_date_dictionary[ticker_str]),
                                                     ('open_price', self.open_price_dictionary[ticker_str]),
                                                     ('close_price', self.close_price_dictionary[ticker_str]),
                                                     ('low_price', self.low_price_dictionary[ticker_str]),
                                                     ('high_price', self.high_price_dictionary[ticker_str])])

             candle_frame['ewma300'] = pd.ewma(candle_frame['close_price'], span=300, min_periods=250)
             candle_frame['ewma50'] = pd.ewma(candle_frame['close_price'], span=50, min_periods=40)
             candle_frame['ma5'] = pd.rolling_mean(candle_frame['close_price'], 5)

             candle_frame['ewma300D'] = candle_frame['ewma300']-candle_frame['ewma300'].shift(60)
             candle_frame['ewma50D'] = candle_frame['ewma50'] - candle_frame['ewma50'].shift(10)

             candle_frame['min14'] = pd.rolling_min(candle_frame['low_price'], 14)
             candle_frame['max14'] = pd.rolling_max(candle_frame['high_price'], 14)

             candle_frame['william'] = -100*(candle_frame['max14']-candle_frame['close_price'])/(candle_frame['max14']-candle_frame['min14'])
             candle_frame['william_1'] = candle_frame['william'].shift(1)

             self.candle_frame_dictionary[ticker_str] = candle_frame.dropna().reset_index(drop=True, inplace=False)

             if ticker_str in self.nonfinished_historical_data_list:
                 self.nonfinished_historical_data_list.remove(ticker_str)

    def calculate_mid_price(self,ticker_str):

        if (self.bid_quantity_dictionary[ticker_str]==0) or (self.ask_quantity_dictionary[ticker_str]==0):
            return np.nan
        else:
            return (self.bid_price_dictionary[ticker_str] * self.bid_quantity_dictionary[ticker_str] + \
         self.ask_price_dictionary[ticker_str] * self.ask_quantity_dictionary[ticker_str]) / \
        (self.bid_quantity_dictionary[ticker_str] + self.ask_quantity_dictionary[ticker_str])



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

        if (tickType==1) or (tickType==2):
            mid_price = self.calculate_mid_price(ticker_str)

            if mth.isnan(self.current_high_price_dictionary[ticker_str]) or (mid_price>self.current_high_price_dictionary[ticker_str]):
                self.current_high_price_dictionary[ticker_str] = mid_price

            if mth.isnan(self.current_low_price_dictionary[ticker_str]) or (mid_price<self.current_low_price_dictionary[ticker_str]):
                self.current_low_price_dictionary[ticker_str] = mid_price

        if (len(self.nonfinished_bid_price_list)==0)&(len(self.nonfinished_ask_price_list)==0)&\
                (len(self.nonfinished_bid_quantity_list)==0)&(len(self.nonfinished_ask_quantity_list)==0)&(len(self.nonfinished_historical_data_list)==0)&(~self.period_call_initiated_q):

            self.periodic_call()

    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)

        ticker_str = self.market_data_ReqId_dictionary[reqId]

        if tickType==0:
            self.bid_quantity_dictionary[ticker_str] = size
            if ticker_str in self.nonfinished_bid_quantity_list:
                self.nonfinished_bid_quantity_list.remove(ticker_str)
        if tickType==3:
            self.ask_quantity_dictionary[ticker_str] = size
            if ticker_str in self.nonfinished_ask_quantity_list:
                self.nonfinished_ask_quantity_list.remove(ticker_str)

        if (tickType==0) or (tickType==3):
            mid_price = self.calculate_mid_price(ticker_str)

            if mth.isnan(self.current_high_price_dictionary[ticker_str]) or (mid_price>self.current_high_price_dictionary[ticker_str]):
                self.current_high_price_dictionary[ticker_str] = mid_price

            if mth.isnan(self.current_low_price_dictionary[ticker_str]) or (mid_price<self.current_low_price_dictionary[ticker_str]):
                self.current_low_price_dictionary[ticker_str] = mid_price

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        print("OpenOrder. ID:", orderId, contract.symbol, contract.secType,
        "@", contract.exchange, ":", order.action, order.orderType,order.totalQuantity, orderState.status)

    def orderStatus(self, orderId: OrderId, status: str, filled: float,remaining: float, avgFillPrice: float, permId: int,parentId: int, lastFillPrice: float, clientId: int,whyHeld: str):
        super().orderStatus(orderId, status, filled, remaining,avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld)

        with open(self.output_dir + '/fills.csv', 'a') as file:
            file.write(str(orderId) + ',' + str(permId)+ ',' + str(parentId)+ ',' + str(filled)+ ',' + str(avgFillPrice))
            file.write('\n')

        ticker_str = ''
        for x,y in self.working_order_id_dictionary.items():
            if y==orderId:
                ticker_str = x

        if len(ticker_str)==0:
            return

        working_alias_long = self.alias_portfolio.position_with_working_orders[ticker_str + '_long']
        working_alias_short = self.alias_portfolio.position_with_working_orders[ticker_str + '_short']

        alias_portfolio_name = ''
        trade_quantity = 0

        if remaining==0:
            self.stop_direction_dictionary.pop(orderId,None)
            self.stop_price_dictionary.pop(orderId,None)
            self.stop_ticker_dictionary.pop(ticker_str,None)

        if (status == 'Cancelled') & (filled==0):
            self.num_bets -= 1

        if filled==0 or (filled==self.order_filled_dictionary[orderId]):
            return

        filled_change = filled - self.order_filled_dictionary[orderId]
        self.order_filled_dictionary[orderId] = filled

        if working_alias_long>0:
            alias_portfolio_name = ticker_str + '_long'
            trade_quantity = filled_change
        elif working_alias_short>0:
            alias_portfolio_name = ticker_str + '_short'
            trade_quantity = filled_change
        elif working_alias_long<0:
            alias_portfolio_name = ticker_str + '_long'
            trade_quantity = -filled_change
        elif working_alias_short<0:
            alias_portfolio_name = ticker_str + '_short'
            trade_quantity = -filled_change

        self.alias_portfolio.order_fill(ticker=alias_portfolio_name, qty=trade_quantity)

        #print('Order filled!')
        #total_alias = self.ocs_alias_portfolio.position_with_all_orders[alias_portfolio_name]
        #working_alias = self.ocs_alias_portfolio.position_with_working_orders[alias_portfolio_name]
        #filled_alias = self.ocs_alias_portfolio.position_with_filled_orders[alias_portfolio_name]
        #print('Total position: ' + str(total_alias), ', Working Position: ' + str(working_alias) + ', Filled Position: ' + str(filled_alias))


    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        try:
            self.log.info('ExecDetails: ' + str(reqId) + ', ' + contract.symbol + ', ' +  contract.secType + ', ' + contract.currency + ', ' + str(execution.execId) + ', ' + str(execution.orderId) + ', ' + str(execution.shares))

            ticker = ib_contract.get_db_ticker_from_ib_contract(ib_contract=contract,contract_id_dictionary=self.contractIDDictionary)

            if execution.side=='BOT':
                trade_quantity = execution.shares
            elif execution.side=='SLD':
                trade_quantity = -execution.shares

            trade_frame = pd.DataFrame.from_items([('alias', [self.db_alias]),
                                               ('ticker', [ticker]),
                                               ('option_type', [None]),
                                               ('strike_price', [np.nan]),
                                               ('trade_price', [execution.price]),
                                               ('trade_quantity', [trade_quantity]),
                                               ('instrument', ['F']),
                                               ('real_tradeQ', [True])])

            ts.load_trades_2strategy(trade_frame=trade_frame, con=self.con)
        except Exception:
            self.log.error('execDetails failed', exc_info=True)


    def periodic_call(self):

        self.period_call_initiated_q = True
        alias_portfolio = self.alias_portfolio

        datetime_now = dt.datetime.now()

        minute_now = datetime_now.minute
        second_now = datetime_now.second

        if (minute_now%5==4) and (second_now>=50):
            candle_end_q = True
        else:
            candle_end_q = False

        if candle_end_q:
            self.log.info(datetime_now.strftime('%Y-%m-%d %H:%M:%S'))
            self.log.info('Realized Pnl: ' + str(self.realized_pnl))
            self.log.info('Total Contracts Trades: ' + str(self.total_contracts_traded))



        ticker_list = self.ticker_list

        for i in range(len(ticker_list)):
            mid_price = self.calculate_mid_price(ticker_list[i])
            bid_price = self.bid_price_dictionary[ticker_list[i]]
            ask_price = self.ask_price_dictionary[ticker_list[i]]

            bid_quantity = self.bid_quantity_dictionary[ticker_list[i]]
            ask_quantity = self.ask_quantity_dictionary[ticker_list[i]]
            tick_size = self.tick_size_dictionary[ticker_list[i]]

            candle_frame = self.candle_frame_dictionary[ticker_list[i]]

            ticker_head = self.ticker_head_dictionary[ticker_list[i]]
            latest_trade_exit_datetime = self.latest_trade_exit_datetime_dictionary[ticker_head]

            total_alias_long = alias_portfolio.position_with_all_orders[ticker_list[i] + '_long']
            working_alias_long = alias_portfolio.position_with_working_orders[ticker_list[i] + '_long']
            filled_alias_long = alias_portfolio.position_with_filled_orders[ticker_list[i] + '_long']

            total_alias_short = alias_portfolio.position_with_all_orders[ticker_list[i] + '_short']
            working_alias_short = alias_portfolio.position_with_working_orders[ticker_list[i] + '_short']
            filled_alias_short = alias_portfolio.position_with_filled_orders[ticker_list[i] + '_short']

            order_quantity = 1

            if candle_end_q:

                candle_stick_start_datetime = dt.datetime(datetime_now.year, datetime_now.month, datetime_now.day,
                                                          datetime_now.hour, minute_now - 4, 0)


                if candle_frame.loc[len(candle_frame.index) - 1, 'bar_datetime'] != candle_stick_start_datetime:
                    candle_frame.loc[len(candle_frame.index),'bar_datetime'] = candle_stick_start_datetime

                candle_frame.loc[len(candle_frame.index) - 1, 'open_price'] = candle_frame.loc[
                    len(candle_frame.index) - 1, 'close_price']
                candle_frame.loc[len(candle_frame.index) - 1, 'close_price'] = mid_price
                candle_frame.loc[len(candle_frame.index) - 1, 'high_price'] = self.current_high_price_dictionary[
                    ticker_list[i]]
                candle_frame.loc[len(candle_frame.index) - 1, 'low_price'] = self.current_low_price_dictionary[
                    ticker_list[i]]

                short_ewma_multiplier = 2 / 51
                long_ewma_multiplier = 2 / 301

                candle_frame.loc[len(candle_frame.index) - 1, 'ewma300'] = \
                    long_ewma_multiplier * (mid_price - candle_frame.loc[len(candle_frame.index) - 2, 'ewma300']) + \
                    candle_frame.loc[len(candle_frame.index) - 2, 'ewma300']

                candle_frame.loc[len(candle_frame.index) - 1, 'ewma50'] = \
                    short_ewma_multiplier * (mid_price - candle_frame.loc[len(candle_frame.index) - 2, 'ewma50']) + \
                    candle_frame.loc[len(candle_frame.index) - 2, 'ewma300']

                candle_frame.loc[len(candle_frame.index) - 1, 'ma5'] = candle_frame['close_price'].iloc[-5:].mean()

                candle_frame.loc[len(candle_frame.index) - 1, 'ewma300D'] = candle_frame.loc[len(
                    candle_frame.index) - 1, 'ewma300'] - candle_frame.loc[len(candle_frame.index) - 61, 'ewma300']

                candle_frame.loc[len(candle_frame.index) - 1, 'ewma50D'] = candle_frame.loc[
                                                                               len(candle_frame.index) - 1, 'ewma50'] - \
                                                                           candle_frame.loc[
                                                                               len(candle_frame.index) - 11, 'ewma50']

                candle_frame.loc[len(candle_frame.index) - 1, 'min14'] = candle_frame['low_price'].iloc[-14:].min()
                candle_frame.loc[len(candle_frame.index) - 1, 'max14'] = candle_frame['high_price'].iloc[-14:].max()

                candle_frame.loc[len(candle_frame.index) - 1, 'william'] = -100 * (
                candle_frame.loc[len(candle_frame.index) - 1, 'max14'] - mid_price) / \
                                                                           (candle_frame.loc[
                                                                                len(candle_frame.index) - 1, 'max14'] -
                                                                            candle_frame.loc[
                                                                                len(candle_frame.index) - 1, 'min14'])

                self.current_high_price_dictionary[ticker_list[i]] = mid_price
                self.current_low_price_dictionary[ticker_list[i]] = mid_price

                ewma300D = candle_frame.loc[len(candle_frame.index) - 1, 'ewma300D']
                william = candle_frame.loc[len(candle_frame.index) - 1, 'william']
                william_1 = candle_frame.loc[len(candle_frame.index) - 2, 'william']
                high_price = candle_frame.loc[len(candle_frame.index) - 1, 'high_price']
                low_price = candle_frame.loc[len(candle_frame.index) - 1, 'low_price']

                if ((ewma300D<0)|(william<=-80))&self.long_trade_possible_dictionary[ticker_list[i]]:
                    self.long_trade_possible_dictionary[ticker_list[i]]=False

                if ((ewma300D>0)|(william>=-20))&self.short_trade_possible_dictionary[ticker_list[i]]:
                    self.short_trade_possible_dictionary[ticker_list[i]]=False

                if (total_alias_long==0) and (total_alias_short==0) and (ewma300D>0) and (william_1<=-80) and (william>-80) and \
                        (datetime_now<=self.latest_trade_entry_datetime):

                    for j in range(len(candle_frame.index)):
                        if j==0:
                            swing_low = candle_frame['low_price'].iloc[-1]
                        elif candle_frame['low_price'].iloc[-1-j]<=swing_low:
                            swing_low = candle_frame['low_price'].iloc[-1-j]
                        elif candle_frame['low_price'].iloc[-1-j]>swing_low:
                            break

                    if high_price-swing_low<0.5*self.daily_sd_dictionary[ticker_list[i]]:

                        self.long_breakout_price_dictionary[ticker_list[i]] = high_price
                        self.long_trade_possible_dictionary[ticker_list[i]] = True
                        self.long_stop_price_dictionary[ticker_list[i]] = swing_low
                        self.log.info('Long trade possible for ' + ticker_list[i] + ', breakout price: ' + str(self.long_breakout_price_dictionary[ticker_list[i]]) + ', stop price: ' + str(self.long_stop_price_dictionary[ticker_list[i]]))

                if (total_alias_long==0) and (total_alias_short==0) and (ewma300D<0) and (william_1>=-20) and (william<-20) and \
                        (datetime_now<=self.latest_trade_entry_datetime):

                    for j in range(len(candle_frame.index)):
                        if j==0:
                            swing_high = candle_frame['high_price'].iloc[-1]
                        elif candle_frame['high_price'].iloc[-1-j]>=swing_high:
                            swing_high = candle_frame['high_price'].iloc[-1-j]
                        elif candle_frame['high_price'].iloc[-1-j]<swing_high:
                            break

                    if swing_high-low_price<0.5*self.daily_sd_dictionary[ticker_list[i]]:

                        self.short_breakout_price_dictionary[ticker_list[i]] = low_price
                        self.short_trade_possible_dictionary[ticker_list[i]] = True
                        self.short_stop_price_dictionary[ticker_list[i]] = swing_high
                        self.log.info('Short trade possible for ' + ticker_list[i] + ', breakout price: ' + str(self.short_breakout_price_dictionary[ticker_list[i]]) + ', stop price: ' + str(self.short_stop_price_dictionary[ticker_list[i]]))

                if (total_alias_long>0) and (self.stop_adjustment_possible_dictionary[ticker_list[i]]) \
                    and (candle_frame['close_price'].iloc[-1]<candle_frame['ma5'].iloc[-1]) and (candle_frame['close_price'].iloc[-2]<candle_frame['ma5'].iloc[-2]):
                    self.long_stop_price_dictionary[ticker_list[i]] = candle_frame['low_price'].iloc[-2:].min()
                    self.log.info('Long stop for ' + ticker_list[i] + ' is adjusted to ' + str(self.long_stop_price_dictionary[ticker_list[i]]))

                if (total_alias_short>0) and (self.stop_adjustment_possible_dictionary[ticker_list[i]]) \
                    and (candle_frame['close_price'].iloc[-1]>candle_frame['ma5'].iloc[-1]) and (candle_frame['close_price'].iloc[-2]>candle_frame['ma5'].iloc[-2]):
                    self.short_stop_price_dictionary[ticker_list[i]] = candle_frame['high_price'].iloc[-2:].max()
                    self.log.info('Short stop for ' + ticker_list[i] + ' is adjusted to ' + str(self.short_stop_price_dictionary[ticker_list[i]]))

                    # cancel working long entry orders to ensure correct accounting

                if (working_alias_long > 0) and (total_alias_long > 0) and ((william > -50) | (william < -95)):
                    self.cancelOrder(int(self.working_order_id_dictionary[ticker_list[i]]))
                    self.log.info(ticker_list[i] + '_long opportunity is missed and entry order is cancelled')
                    alias_portfolio.order_send(ticker=ticker_list[i] + '_long', qty=-working_alias_long)
                    self.working_order_id_dictionary[ticker_list[i]] = np.nan

                if (working_alias_short > 0) and (total_alias_short > 0) and ((william < -50) | (william > -5)):
                    self.cancelOrder(int(self.working_order_id_dictionary[ticker_list[i]]))
                    self.log.info(ticker_list[i] + '_short opportunity is missed and entry order is cancelled')
                    alias_portfolio.order_send(ticker=ticker_list[i] + '_long', qty=-working_alias_short)
                    self.working_order_id_dictionary[ticker_list[i]] = np.nan

                if total_alias_long>0:
                    self.log.info('Running Pnl for ' + ticker_list[i] + ' long is: ' + str(self.contract_multiplier_dictionary[ticker_list[i]]*total_alias_long*(self.bid_price_dictionary[ticker_list[i]]-self.trade_entry_price_dictionary[ticker_list[i]])))
                if total_alias_short>0:
                    self.log.info('Running Pnl for ' + ticker_list[i] + ' short is: ' + str(self.contract_multiplier_dictionary[ticker_list[i]] * total_alias_short *(self.trade_entry_price_dictionary[ticker_list[i]]-self.ask_price_dictionary[ticker_list[i]])))

                #self.log.info(ticker_list[i] + ' ewma300D: ' + str(ewma300D))
                #self.log.info(ticker_list[i] + ' william: ' + str(william))
                #self.log.info(ticker_list[i] + ' william_1: ' + str(william_1))

            # new long trade

            if (self.total_contracts_traded<=self.max_total_contracts_traded)&(self.long_trade_possible_dictionary[ticker_list[i]]) & (mid_price>self.long_breakout_price_dictionary[ticker_list[i]]+self.tick_size_dictionary[ticker_list[i]]) and (self.num_bets<self.max_num_bets):

                alias_portfolio.order_send(ticker=ticker_list[i] + '_long', qty=order_quantity)

                self.trade_entry_price_dictionary[ticker_list[i]] = self.ask_price_dictionary[ticker_list[i]]

                self.long_target_price_dictionary[ticker_list[i]] = self.trade_entry_price_dictionary[ticker_list[i]]+ 1*(self.trade_entry_price_dictionary[ticker_list[i]]-self.long_stop_price_dictionary[ticker_list[i]])
                self.log.info('New Long Ticker: ' + ticker_list[i] + ', Breakout Price: ' + str(self.long_breakout_price_dictionary[ticker_list[i]]) + ', Entry Price: ' + str(self.ask_price_dictionary[ticker_list[i]]) +
                              ', Stop Price: ' + str(self.long_stop_price_dictionary[ticker_list[i]]) + ', ' + ', Target Price: ' + str(self.long_target_price_dictionary[ticker_list[i]]))
                self.long_trade_possible_dictionary[ticker_list[i]] = False
                self.long_breakout_price_dictionary[ticker_list[i]] = np.nan
                self.num_bets += 1
                self.working_order_id_dictionary[ticker_list[i]] = self.next_val_id
                self.order_filled_dictionary[self.next_val_id] = 0
                self.order_size_dictionary[self.next_val_id] = order_quantity

                limit_price = exec.get_midprice(bid_price=bid_price, ask_price=ask_price, bid_quantity=bid_quantity,ask_quantity=ask_quantity, tick_size=tick_size, direction=1)

                self.placeOrder(self.next_valid_id(), self.ib_contract_dictionary[ticker_list[i]], ib_api_trade.LimitOrder('BUY', order_quantity, limit_price))
                self.total_contracts_traded += order_quantity

            # new short trade
            if (self.total_contracts_traded<=self.max_total_contracts_traded)&(self.short_trade_possible_dictionary[ticker_list[i]]) & (mid_price < self.short_breakout_price_dictionary[ticker_list[i]] - self.tick_size_dictionary[ticker_list[i]]) and (self.num_bets<self.max_num_bets):

                alias_portfolio.order_send(ticker=ticker_list[i] + '_short', qty=order_quantity)

                self.trade_entry_price_dictionary[ticker_list[i]] = self.bid_price_dictionary[ticker_list[i]]

                self.short_target_price_dictionary[ticker_list[i]] = self.trade_entry_price_dictionary[ticker_list[i]] + 1 * (self.trade_entry_price_dictionary[ticker_list[i]] - self.short_stop_price_dictionary[ticker_list[i]])
                self.log.info('New Short Ticker: ' + ticker_list[i] + ', Breakout Price: ' + str(self.short_breakout_price_dictionary[ticker_list[i]]) + ', Entry Price: ' + str(self.bid_price_dictionary[ticker_list[i]]) +
                              ', Stop Price: ' + str(self.short_stop_price_dictionary[ticker_list[i]]) + ', ' + ', Target Price: ' + str(self.short_target_price_dictionary[ticker_list[i]]))
                self.short_trade_possible_dictionary[ticker_list[i]] = False
                self.short_breakout_price_dictionary[ticker_list[i]] = np.nan
                self.num_bets += 1
                self.working_order_id_dictionary[ticker_list[i]] = self.next_val_id
                self.order_filled_dictionary[self.next_val_id] = 0
                self.order_size_dictionary[self.next_val_id] = order_quantity

                limit_price = exec.get_midprice(bid_price=bid_price, ask_price=ask_price, bid_quantity=bid_quantity,ask_quantity=ask_quantity, tick_size=tick_size, direction=-1)

                self.placeOrder(self.next_valid_id(), self.ib_contract_dictionary[ticker_list[i]], ib_api_trade.LimitOrder('SELL', order_quantity, limit_price))
                self.total_contracts_traded += order_quantity

            # long trade stop out
            if (self.total_contracts_traded<=self.max_total_contracts_traded)&((total_alias_long>0) & (filled_alias_long > 0)&((mid_price<=self.long_stop_price_dictionary[ticker_list[i]]-self.tick_size_dictionary[ticker_list[i]]) or (datetime_now>=latest_trade_exit_datetime))):

                realized_pnl = self.contract_multiplier_dictionary[ticker_list[i]]*total_alias_long*(self.bid_price_dictionary[ticker_list[i]]-self.trade_entry_price_dictionary[ticker_list[i]])
                self.log.info('Closed ' + ticker_list[i] + ' long with realized pnl: ' + str(realized_pnl))

                self.realized_pnl += realized_pnl
                self.total_contracts_traded += total_alias_long

                alias_portfolio.order_send(ticker=ticker_list[i] + '_long', qty=-filled_alias_long)
                self.working_order_id_dictionary[ticker_list[i]] = self.next_val_id
                self.order_filled_dictionary[self.next_val_id] = 0
                self.order_size_dictionary[self.next_val_id] = filled_alias_long


                limit_price = exec.get_midprice(bid_price=bid_price, ask_price=ask_price, bid_quantity=bid_quantity,ask_quantity=ask_quantity, tick_size=tick_size, direction=-1)
                self.stop_direction_dictionary[self.next_val_id] = -1
                self.stop_price_dictionary[self.next_val_id] = limit_price
                self.stop_ticker_dictionary[ticker_list[i]] = self.next_val_id

                self.placeOrder(self.next_valid_id(), self.ib_contract_dictionary[ticker_list[i]],ib_api_trade.LimitOrder('SELL', filled_alias_long, limit_price))

                self.stop_adjustment_possible_dictionary[ticker_list[i]] = False
                self.trade_entry_price_dictionary[ticker_list[i]] = np.nan
                self.long_target_price_dictionary[ticker_list[i]] = np.nan
                self.long_stop_price_dictionary[ticker_list[i]] = np.nan


            # short trade stop out
            if (self.total_contracts_traded<=self.max_total_contracts_traded)&((total_alias_short>0) & (filled_alias_short>0) &((mid_price>=self.short_stop_price_dictionary[ticker_list[i]]+self.tick_size_dictionary[ticker_list[i]]) or (datetime_now>=latest_trade_exit_datetime))):

                realized_pnl = self.contract_multiplier_dictionary[ticker_list[i]] * total_alias_short *(self.trade_entry_price_dictionary[ticker_list[i]]-self.ask_price_dictionary[ticker_list[i]])
                self.log.info('Closed ' + ticker_list[i] + ' short with realized pnl: ' + str(realized_pnl))
                self.realized_pnl += realized_pnl
                self.total_contracts_traded += total_alias_short

                alias_portfolio.order_send(ticker=ticker_list[i] + '_short', qty=-filled_alias_short)
                self.working_order_id_dictionary[ticker_list[i]] = self.next_val_id
                self.order_filled_dictionary[self.next_val_id] = 0
                self.order_size_dictionary[self.next_val_id] = filled_alias_short

                limit_price = exec.get_midprice(bid_price=bid_price, ask_price=ask_price, bid_quantity=bid_quantity,ask_quantity=ask_quantity, tick_size=tick_size, direction=1)
                self.stop_direction_dictionary[self.next_val_id] = 1
                self.stop_price_dictionary[self.next_val_id] = limit_price
                self.stop_ticker_dictionary[ticker_list[i]] = self.next_val_id
                self.placeOrder(self.next_valid_id(), self.ib_contract_dictionary[ticker_list[i]],ib_api_trade.LimitOrder('BUY', filled_alias_short, limit_price))

                self.stop_adjustment_possible_dictionary[ticker_list[i]] = False
                self.trade_entry_price_dictionary[ticker_list[i]] = np.nan
                self.short_target_price_dictionary[ticker_list[i]] = np.nan
                self.short_stop_price_dictionary[ticker_list[i]] = np.nan

            if (total_alias_long>0) & (mid_price>=self.long_target_price_dictionary[ticker_list[i]]) & (not self.stop_adjustment_possible_dictionary[ticker_list[i]]):
                self.stop_adjustment_possible_dictionary[ticker_list[i]] = True
                self.log.info('Target is reached for long trade: ' + ticker_list[i])

            if (total_alias_short>0) & (mid_price<=self.short_target_price_dictionary[ticker_list[i]]) & (not self.stop_adjustment_possible_dictionary[ticker_list[i]]):
                self.stop_adjustment_possible_dictionary[ticker_list[i]] = True
                self.log.info('Target is reached for short trade: ' + ticker_list[i])

            # Update Stop Orders
            if ticker_list[i] in self.stop_ticker_dictionary:
                order_id = self.stop_ticker_dictionary[ticker_list[i]]
                order_direction = self.stop_direction_dictionary[order_id]
                order_price = self.stop_price_dictionary[order_id]
                order_size = self.order_size_dictionary[order_id]

                if (order_direction>0)&(int(round((bid_price-order_price)/tick_size))>=2):
                    self.log.info(ticker_list[i] + ' stop price is being adjusted')
                    limit_price = exec.get_midprice(bid_price=bid_price, ask_price=ask_price, bid_quantity=bid_quantity,ask_quantity=ask_quantity, tick_size=tick_size, direction=order_direction)
                    # this part might have to be adjusted, what's the correct quentity if order is already partially executed?
                    self.placeOrder(order_id, self.ib_contract_dictionary[ticker_list[i]], ib_api_trade.LimitOrder('BUY', order_size, limit_price))
                    self.stop_price_dictionary[order_id] = limit_price
                elif (order_direction<0)&(int(round((order_price-ask_price)/tick_size))>=2):
                    self.log.info(ticker_list[i] + ' stop price is being adjusted')
                    limit_price = exec.get_midprice(bid_price=bid_price, ask_price=ask_price, bid_quantity=bid_quantity,ask_quantity=ask_quantity, tick_size=tick_size,direction=order_direction)
                    # this part might have to be adjusted, what's the correct quentity if order is already partially executed?
                    self.placeOrder(order_id, self.ib_contract_dictionary[ticker_list[i]],ib_api_trade.LimitOrder('SELL', order_size, limit_price))
                    self.stop_price_dictionary[order_id] = limit_price

        thr.Timer(10, self.periodic_call).start()

    def main_run(self):
        ticker_list = self.ticker_list
        self.nonfinished_ticker_list = cpy.deepcopy(ticker_list)

        for i in range(len(ticker_list)):
            contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.contractDetailReqIdDictionary[self.next_val_id] = ticker_list[i]
            self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
            self.reqContractDetails(self.next_valid_id(), contract_i)


        #self.reqExecutions(10001, ExecutionFilter())

        #self.run()
        #self.disconnect()

    def request_historical_bar_data(self):

        ticker_list = self.ticker_list

        for i in range(len(ticker_list)):
            self.high_price_dictionary[ticker_list[i]] = []
            self.low_price_dictionary[ticker_list[i]] = []
            self.close_price_dictionary[ticker_list[i]] = []
            self.open_price_dictionary[ticker_list[i]] = []
            self.bar_date_dictionary[ticker_list[i]] = []

            outright_ib_contract = self.ib_contract_dictionary[ticker_list[i]]
            self.bar_data_ReqId_dictionary[self.next_val_id] = ticker_list[i]
            self.log.info('req id: ' + str(self.next_val_id) + ', outright_ticker:' + str(ticker_list[i]));

            self.reqHistoricalData(reqId=self.next_valid_id(), contract=outright_ib_contract,endDateTime= '', durationStr="1 M", barSizeSetting='5 mins', whatToShow='MIDPOINT', useRTH=0, formatDate=1,chartOptions=[])


    def request_market_data(self):

        ticker_list = self.ticker_list

        self.nonfinished_bid_price_list.extend(ticker_list)
        self.nonfinished_ask_price_list.extend(ticker_list)
        self.nonfinished_bid_quantity_list.extend(ticker_list)
        self.nonfinished_ask_quantity_list.extend(ticker_list)
        self.nonfinished_historical_data_list.extend(ticker_list)

        for i in range(len(ticker_list)):


            self.bid_price_dictionary[ticker_list[i]] = np.nan
            self.ask_price_dictionary[ticker_list[i]] = np.nan
            self.bid_quantity_dictionary[ticker_list[i]] = np.nan
            self.ask_quantity_dictionary[ticker_list[i]] = np.nan
            self.current_high_price_dictionary[ticker_list[i]] = np.nan
            self.current_low_price_dictionary[ticker_list[i]] = np.nan

            self.long_breakout_price_dictionary[ticker_list[i]] = np.nan
            self.short_breakout_price_dictionary[ticker_list[i]] = np.nan
            self.long_stop_price_dictionary[ticker_list[i]] = np.nan
            self.short_stop_price_dictionary[ticker_list[i]] = np.nan
            self.long_target_price_dictionary[ticker_list[i]] = np.nan
            self.short_target_price_dictionary[ticker_list[i]] = np.nan
            self.long_trade_possible_dictionary[ticker_list[i]] = False
            self.short_trade_possible_dictionary[ticker_list[i]] = False
            self.stop_adjustment_possible_dictionary[ticker_list[i]] = False
            self.trade_entry_price_dictionary[ticker_list[i]] = np.nan

            outright_ib_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.ib_contract_dictionary[ticker_list[i]] = outright_ib_contract
            self.market_data_ReqId_dictionary[self.next_val_id] = ticker_list[i]
            self.log.info('req id: ' + str(self.next_val_id) + ', outright_ticker:' + str(ticker_list[i]));
            self.reqMktData(self.next_valid_id(), outright_ib_contract, "", False, False, [])

