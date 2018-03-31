

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
import os.path
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

class Algo(subs.subscription):

    contractDetailReqIdDictionary = {}
    market_data_ReqId_dictionary = {}
    bar_data_ReqId_dictionary = {}
    contractIDDictionary = {}

    high_price_dictionary = {}
    low_price_dictionary = {}
    close_price_dictionary = {}
    open_price_dictionary = {}
    bar_date_dictionary = {}
    volume_dictionary = {}
    candle_frame_dictionary = {}

    trade_entry_price_dictionary = {}

    tick_size_dictionary = {}

    current_high_price_dictionary = {}
    current_low_price_dictionary = {}

    spread_contract_dictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    spread_ticker_list = []
    price_request_dictionary = {'spread': [] ,'outright': []}

    nonfinished_historical_data_list = []
    period_call_initiated_q = False
    durationStr = ''

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.contractIDDictionary[self.contractDetailReqIdDictionary[reqId]] = contractDetails.summary.conId

    def contractDetailsEnd(self, reqId: int):
            super().contractDetailsEnd(reqId)
            self.nonfinished_contract_detail_ReqId_list.remove(reqId)
            self.nonfinished_ticker_list.remove(self.contractDetailReqIdDictionary[reqId])
            if len(self.nonfinished_contract_detail_ReqId_list)==0:
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
        self.volume_dictionary[ticker_str].append(volume)
        self.bar_date_dictionary[ticker_str].append(dt.datetime.strptime(date, '%Y%m%d %H:%M:%S'))



    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        ticker_str = self.bar_data_ReqId_dictionary[reqId]
        candle_frame = pd.DataFrame.from_items([('bar_datetime', self.bar_date_dictionary[ticker_str]),
                                         ('open_price', self.open_price_dictionary[ticker_str]),
                                         ('close_price', self.close_price_dictionary[ticker_str]),
                                         ('low_price', self.low_price_dictionary[ticker_str]),
                                         ('high_price', self.high_price_dictionary[ticker_str]),
                                         ('volume', self.volume_dictionary[ticker_str])])

        if os.path.isfile(self.output_dir + '/' + ticker_str + '.pkl'):
            old_data = pd.read_pickle(self.output_dir + '/' + ticker_str + '.pkl')
            candle_frame['frame_indx'] = 1
            old_data['frame_indx'] = 0
            merged_data = pd.concat([old_data, candle_frame], ignore_index=True)
            merged_data.sort(['bar_datetime', 'frame_indx'], ascending=[True, False], inplace=True)
            merged_data.drop_duplicates(subset=['bar_datetime'], take_last=False, inplace=True)
            candle_frame = merged_data.drop('frame_indx', 1, inplace=False)
            candle_frame.reset_index(drop=True,inplace=True)

        candle_frame.to_pickle(self.output_dir + '/' + ticker_str + '.pkl')



        if ticker_str in self.nonfinished_historical_data_list:
            self.nonfinished_historical_data_list.remove(ticker_str)

        if len(self.nonfinished_historical_data_list)==0:
            self.disconnect()


    def main_run(self):
        ticker_list = self.ticker_list
        self.nonfinished_ticker_list = cpy.deepcopy(ticker_list)

        for i in range(len(ticker_list)):
            contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.contractDetailReqIdDictionary[self.next_val_id] = ticker_list[i]
            self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
            self.reqContractDetails(self.next_valid_id(), contract_i)

    def request_historical_bar_data(self):

        ticker_list = self.ticker_list
        self.nonfinished_historical_data_list = ticker_list

        for i in range(len(ticker_list)):
            self.high_price_dictionary[ticker_list[i]] = []
            self.low_price_dictionary[ticker_list[i]] = []
            self.close_price_dictionary[ticker_list[i]] = []
            self.open_price_dictionary[ticker_list[i]] = []
            self.volume_dictionary[ticker_list[i]] = []
            self.bar_date_dictionary[ticker_list[i]] = []

            outright_ib_contract = ib_contract.get_ib_contract_from_db_ticker(ticker=ticker_list[i], sec_type='F')
            self.bar_data_ReqId_dictionary[self.next_val_id] = ticker_list[i]
            print('req id: ' + str(self.next_val_id) + ', outright_ticker:' + str(ticker_list[i]));

            self.reqHistoricalData(reqId=self.next_valid_id(), contract=outright_ib_contract,endDateTime= '', durationStr=self.durationStr, barSizeSetting='5 mins', whatToShow='TRADES', useRTH=0, formatDate=1,chartOptions=[])
