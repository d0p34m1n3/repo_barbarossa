
import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.contract import *
from ibapi.common import *

class ib_order_follow_up(subs.subscription):

    def orderStatus(self, orderId: OrderId, status: str, filled: float,remaining: float, avgFillPrice: float, permId: int,parentId: int, lastFillPrice: float, clientId: int,whyHeld: str):
        super().orderStatus(orderId, status, filled, remaining,avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld)
        print("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled,
        "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
        "PermId:", permId, "ParentId:", parentId, "LastFillPrice:",
        lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld)

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        print(contract.localSymbol)

        db_ticker_output = ib_contract.get_db_ticker_from_ib_contract(ib_contract=contract)
        print(execution)




    def main_run(self):
        print('HERE')
        self.reqAllOpenOrders()
        self.reqExecutions(self.next_valid_id(), ExecutionFilter())


def test_ib_order_follow_up():
    app = ib_order_follow_up()
    app.connect(client_id=7)
    app.run()

if __name__ == "__main__":
    test_ib_order_follow_up()