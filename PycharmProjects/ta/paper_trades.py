__author__ = 'kocat_000'

import ta.strategy as ts
import pandas as pd


def generate_paper_trades_from_strategy_sheet(**kwargs):

    id = kwargs['id']
    strategy_class = kwargs['strategy_class']
    size = kwargs['size']

    strategy_output = ts.load_strategy_file(**kwargs)
    strategy = strategy_output.loc[id]

    if strategy_class == 'futures_butterfly':

        if strategy['Q'] <= 49:
            quantity = round(size/abs(strategy['downside']))
        elif strategy['Q'] >= 51:
            quantity = -round(size/abs(strategy['upside']))

        trade_frame =  pd.DataFrame.from_items([('ticker', [strategy['ticker1'], strategy['ticker2'], strategy['ticker3']]),
                                        ('option_type', [None]*3), ('strike_price', [None]*3),
                                        ('trade_price', [strategy['price1'], strategy['price2'], strategy['price3']]),
                                        ('trade_quantity', [quantity, -round(quantity*(1+strategy['second_spread_weight_1'])),
                                                                      round(quantity*strategy['second_spread_weight_1'])]),
                                        ('instrument', ['F', 'F', 'F']), ('real_tradeQ', [False, False, False])])

    return trade_frame

def create_paper_strategy_from_strategy_sheet(**kwargs):

    id = kwargs['id']
    strategy_class = kwargs['strategy_class']
    size = kwargs['size']
    report_date = kwargs['report_date']

    strategy_id = ts.generate_db_strategy_from_strategy_sheet(id=id,strategy_class=strategy_class,
                                                              report_date=report_date)

    trade_frame = generate_paper_trades_from_strategy_sheet(id=id,strategy_class=strategy_class,size=size,report_date=report_date)

    ts.load_trades_2strategy(trade_frame=trade_frame,strategy_id=strategy_id)



