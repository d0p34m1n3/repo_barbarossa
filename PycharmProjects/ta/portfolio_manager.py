
import ta.strategy as ts
import ta.pnl as tapnl
import my_sql_routines.my_sql_utilities as msu
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'

def get_daily_pnl_snapshot(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    strategy_frame = ts.get_open_strategies(**kwargs)

    pnl_output = [tapnl.get_strategy_pnl(alias=x,con=con) for x in strategy_frame['alias']]

    strategy_frame['daily_pnl'] = [x['daily_pnl'] for x in pnl_output]
    strategy_frame['total_pnl'] = [x['total_pnl'] for x in pnl_output]

    if 'con' not in kwargs.keys():
        con.close()

    strategy_frame = strategy_frame[['alias','daily_pnl','total_pnl']]
    strategy_frame.sort('daily_pnl',ascending=False,inplace=True)
    strategy_frame.loc[max(strategy_frame.index)+1] = ['TOTAL', strategy_frame['daily_pnl'].sum(), strategy_frame['total_pnl'].sum()]

    return strategy_frame