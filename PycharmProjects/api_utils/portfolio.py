
class portfolio:
    def __init__(self,ticker_list):

        self.position_with_filled_orders = {}
        self.position_with_working_orders = {}
        self.position_with_all_orders = {}

        for ticker in ticker_list:
            self.position_with_filled_orders[ticker] = 0
            self.position_with_working_orders[ticker] = 0
            self.position_with_all_orders[ticker] = 0


    def order_send(self,**kwargs):
        ticker = kwargs['ticker']
        qty = kwargs['qty']
        self.position_with_working_orders[ticker] = self.position_with_working_orders[ticker] + qty
        self.position_with_all_orders[ticker] = self.position_with_all_orders[ticker] + qty

    def order_fill(self,**kwargs):
        ticker = kwargs['ticker']
        qty = kwargs['qty']
        self.position_with_filled_orders[ticker] = self.position_with_filled_orders[ticker] + qty
        self.position_with_working_orders[ticker] = self.position_with_working_orders[ticker] - qty






