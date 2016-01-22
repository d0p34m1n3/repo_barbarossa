
import ta.strategy as ts
import pandas as pd
import ta.pnl as tapnl
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import pandas as pd
import shared.directory_names as dn
import os.path

pd.options.mode.chained_assignment = None  # default='warn'

def get_daily_pnl_snapshot(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if  'as_of_date' not in kwargs.keys():
        as_of_date = exp.doubledate_shift_bus_days()
        kwargs['as_of_date'] = as_of_date
    else:
        as_of_date = kwargs['as_of_date']

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date,ext='ta')

    if os.path.isfile(ta_output_dir + '/portfolio_pnl.pkl'):
        strategy_frame = pd.read_pickle(ta_output_dir + '/portfolio_pnl.pkl')
        return strategy_frame

    strategy_frame = ts.get_open_strategies(**kwargs)
    pnl_output = [tapnl.get_strategy_pnl(alias=x,con=con,**kwargs) for x in strategy_frame['alias']]

    strategy_frame['daily_pnl'] = [x['daily_pnl'] for x in pnl_output]
    strategy_frame['total_pnl'] = [x['total_pnl'] for x in pnl_output]

    if 'con' not in kwargs.keys():
        con.close()

    strategy_frame = strategy_frame[['alias','daily_pnl','total_pnl']]
    strategy_frame.sort('daily_pnl',ascending=False,inplace=True)
    strategy_frame.loc[max(strategy_frame.index)+1] = ['TOTAL', strategy_frame['daily_pnl'].sum(), strategy_frame['total_pnl'].sum()]

    strategy_frame.to_pickle(ta_output_dir + '/portfolio_pnl.pkl')

    return strategy_frame