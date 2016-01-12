
import ta.strategy as ts
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import pandas as pd

def get_strategy_pnl_4day(**kwargs):

    alias = kwargs['alias']
    pnl_date = kwargs['pnl_date']

    pnl_datetime = cu.convert_doubledate_2datetime(pnl_date)

    con = msu.get_my_sql_connection(**kwargs)

    if 'trades_frame' in kwargs.keys():
        trades_frame = kwargs['trades_frame']
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]
    else:
        trades_frame = ts.get_trades_4strategy_alias(alias=alias,con=con)
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]
        trades_frame['contract_multiplier'] = [cmi.contract_multiplier[x] for x in ticker_head_list]
        trades_frame['t_cost'] = [cmi.t_cost[x] for x in ticker_head_list]

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        unique_ticker_head_list = list(set(ticker_head_list))
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in unique_ticker_head_list}

    pnl_date_1 = exp.doubledate_shift_bus_days(double_date=pnl_date)

    trades_frame['price_1'] = [gfp.get_futures_price_preloaded(ticker=x,
                                futures_data_dictionary=futures_data_dictionary,
                                settle_date=pnl_date_1)['close_price'].values[0] for x in trades_frame['ticker']]

    trades_frame['price'] = [gfp.get_futures_price_preloaded(ticker=x,
                                futures_data_dictionary=futures_data_dictionary,
                                settle_date=pnl_date)['close_price'].values[0] for x in trades_frame['ticker']]


    position_frame = trades_frame[trades_frame['trade_date']<pnl_datetime]
    intraday_frame = trades_frame[trades_frame['trade_date']==pnl_datetime]

    if len(position_frame)==0:
        position_pnl = 0
    else:
        position_frame['pnl'] = position_frame['contract_multiplier']*\
                                position_frame['trade_quantity']*\
                                (position_frame['price']-position_frame['price_1'])
        position_pnl = position_frame['pnl'].sum()

    if len(intraday_frame)==0:
        intraday_pnl = 0
        t_cost = 0
    else:
        intraday_frame['pnl'] = intraday_frame['contract_multiplier']*\
                                intraday_frame['trade_quantity']*\
                                (intraday_frame['price']-intraday_frame['trade_price'])
        intraday_pnl = intraday_frame['pnl'].sum()
        t_cost = (abs(intraday_frame['trade_quantity']*intraday_frame['t_cost'])).sum()

    if 'con' not in kwargs.keys():
        con.close()

    return {'total_pnl': int(position_pnl+intraday_pnl - t_cost),
            'position_pnl': int(position_pnl),
            'intraday_pnl': int(intraday_pnl),
            't_cost': int(t_cost)}

def get_strategy_pnl(**kwargs):

    alias = kwargs['alias']

    con = msu.get_my_sql_connection(**kwargs)

    strategy_info = ts.get_strategy_info_from_alias(alias=alias, con=con)

    open_date = int(strategy_info['open_date'].strftime('%Y%m%d'))
    close_date = int(strategy_info['close_date'].strftime('%Y%m%d'))

    last_available_date = exp.doubledate_shift_bus_days()

    if close_date>last_available_date:
        close_date = last_available_date

    bus_day_list = exp.get_bus_day_list(date_from=open_date,date_to=close_date)

    trades_frame = ts.get_trades_4strategy_alias(alias=alias,con=con)
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        unique_ticker_head_list = list(set(ticker_head_list))
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in unique_ticker_head_list}

    trades_frame['contract_multiplier'] = [cmi.contract_multiplier[x] for x in ticker_head_list]
    trades_frame['t_cost'] = [cmi.t_cost[x] for x in ticker_head_list]

    pnl_path = [get_strategy_pnl_4day(alias=alias,pnl_date=x,con=con,
                                      trades_frame=trades_frame,
                                      futures_data_dictionary=futures_data_dictionary) for x in bus_day_list]

    pnl_frame = pd.DataFrame(pnl_path)
    pnl_frame['settle_date'] = bus_day_list

    if 'con' not in kwargs.keys():
        con.close()

    return {'pnl_frame' : pnl_frame[['settle_date','position_pnl','intraday_pnl','t_cost','total_pnl']],
            'daily_pnl' : pnl_frame['total_pnl'].values[-1],
            'total_pnl' : pnl_frame['total_pnl'].sum()}




