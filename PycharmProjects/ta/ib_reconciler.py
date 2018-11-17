
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import statsmodels.api

import ib_api_utils.subscription as subs
import my_sql_routines.my_sql_utilities as msu
import ta.portfolio_manager as tpm
import shared.calendar_utilities as cu
import ib_api_utils.ib_contract as ib_contract
import decimal as dec
from ibapi.contract import *
import sys
import pandas as pd
import numpy as np

class ib_reconciler(subs.subscription):

    contractIDDictionary = {}
    contractDetailReqIdDictionary = {}
    nonfinished_contract_detail_ReqId_list = []
    position_frame = pd.DataFrame()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.position_frame.loc[self.contractDetailReqIdDictionary[reqId],'con_id'] = contractDetails.contract.conId
        self.position_frame.loc[self.contractDetailReqIdDictionary[reqId], 'ib_symbol'] = contractDetails.contract.localSymbol

    def contractDetailsEnd(self, reqId: int):
            super().contractDetailsEnd(reqId)
            self.nonfinished_contract_detail_ReqId_list.remove(reqId)
            if len(self.nonfinished_contract_detail_ReqId_list) == 0:
                self.reqPositionsMulti(self.next_val_id, 'U2135216', "")

    def positionMulti(self, reqId: int, account: str, modelCode: str,contract: Contract, pos: float, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)


        position_frame = self.position_frame
        selection_index = position_frame['con_id']==contract.conId
        if sum(selection_index)==0:
            position_frame.loc[len(position_frame.index),'ib_position'] = pos
            position_frame.loc[len(position_frame.index)-1, 'qty'] = 0
            position_frame.loc[len(position_frame.index)-1, 'con_id'] = contract.conId
            position_frame.loc[len(position_frame.index) - 1, 'ib_symbol'] = contract.localSymbol
        else:
            position_frame.loc[selection_index,'ib_position'] = pos

        self.position_frame = position_frame


    # ! [positionmultiend]
    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        position_frame = self.position_frame
        net_position = position_frame[position_frame['qty']!=position_frame['ib_position']]
        if len(net_position.index)==0:
            print('RECONCILED!')
        else:
            print('NOT RECONCILED!')
            print(net_position[['generalized_ticker','qty','ib_position','ib_symbol']].to_string())

    def main_run(self):

        position_frame = self.position_frame

        for i in range(len(position_frame.index)):

            if position_frame.loc[i,'instrument'] == 'F':
                contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=position_frame.loc[i,'ticker'], sec_type='F')
            elif position_frame.loc[i,'instrument'] == 'O':
                contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=position_frame.loc[i, 'ticker'],sec_type='OF',
                                                                        option_type=position_frame.loc[i, 'option_type'],
                                                                        strike=float(position_frame.loc[i, 'strike_price']))
            elif position_frame.loc[i,'instrument'] == 'S':
                contract_i = ib_contract.get_ib_contract_from_db_ticker(ticker=position_frame.loc[i,'ticker'], sec_type='S')

            self.contractDetailReqIdDictionary[self.next_val_id] = i
            self.nonfinished_contract_detail_ReqId_list.append(self.next_val_id)
            self.reqContractDetails(self.next_valid_id(), contract_i)

def test_ib_reconciler():
    app = ib_reconciler()
    con = msu.get_my_sql_connection()
    app.con = con
    position_frame = tpm.get_position_4portfolio(trade_date_to=cu.get_doubledate())
    position_frame['con_id'] = np.nan
    position_frame['ib_position'] = np.nan
    position_frame['ib_symbol'] = ''
    position_frame.reset_index(drop=True, inplace=True)
    app.position_frame = position_frame
    app.connect(client_id=1)
    app.run()


if __name__ == "__main__":
    test_ib_reconciler()