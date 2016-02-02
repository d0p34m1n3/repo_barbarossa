
import ta.strategy as ts
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None  # default='warn'


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

    trades_frame['ticker_head'] = ticker_head_list

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

    position_frame = trades_frame[trades_frame['trade_date'] < pnl_datetime]
    intraday_frame = trades_frame[trades_frame['trade_date'] == pnl_datetime]

    position_pnl_per_ticker = pd.DataFrame(columns=['ticker','pnl_position'])
    intraday_pnl_per_ticker = pd.DataFrame(columns=['ticker','pnl_intraday'])

    position_pnl_per_tickerhead = pd.DataFrame(columns=['ticker_head','pnl_position'])
    intraday_pnl_per_tickerhead = pd.DataFrame(columns=['ticker_head','pnl_intraday'])

    if len(position_frame) == 0:
        position_pnl = 0
    else:
        position_frame['pnl'] = position_frame['contract_multiplier']*\
                                position_frame['trade_quantity']*\
                                (position_frame['price']-position_frame['price_1'])
        position_pnl = position_frame['pnl'].sum()
        position_grouped_per_ticker = position_frame.groupby('ticker')
        position_grouped_per_tickerhead = position_frame.groupby('ticker_head')
        position_pnl_per_ticker['pnl_position'] = (position_grouped_per_ticker['pnl'].sum()).values
        position_pnl_per_ticker['ticker'] = (position_grouped_per_ticker['ticker'].first()).values

        position_pnl_per_tickerhead['pnl_position'] = (position_grouped_per_tickerhead['pnl'].sum()).values
        position_pnl_per_tickerhead['ticker_head'] = (position_grouped_per_tickerhead['ticker_head'].first()).values

    if len(intraday_frame) == 0:
        intraday_pnl = 0
        t_cost = 0
    else:
        intraday_frame['pnl'] = intraday_frame['contract_multiplier']*\
                                intraday_frame['trade_quantity']*\
                                (intraday_frame['price']-intraday_frame['trade_price'])
        intraday_frame['pnl_wtcost'] = intraday_frame['pnl']-abs(intraday_frame['trade_quantity']*intraday_frame['t_cost'])
        intraday_pnl = intraday_frame['pnl'].sum()
        t_cost = (abs(intraday_frame['trade_quantity']*intraday_frame['t_cost'])).sum()

        intraday_grouped_per_ticker = intraday_frame.groupby('ticker')
        intraday_grouped_per_tickerhead = intraday_frame.groupby('ticker_head')
        intraday_pnl_per_ticker['pnl_intraday'] = (intraday_grouped_per_ticker['pnl_wtcost'].sum()).values
        intraday_pnl_per_ticker['ticker'] = (intraday_grouped_per_ticker['ticker'].first()).values

        intraday_pnl_per_tickerhead['pnl_intraday'] = (intraday_grouped_per_tickerhead['pnl_wtcost'].sum()).values
        intraday_pnl_per_tickerhead['ticker_head'] = (intraday_grouped_per_tickerhead['ticker_head'].first()).values

    pnl_per_ticker = pd.merge(position_pnl_per_ticker,intraday_pnl_per_ticker,how='outer',on='ticker')
    intraday_zero_indx = [x not in intraday_pnl_per_ticker['ticker'].values for x in pnl_per_ticker['ticker']]
    position_zero_indx = [x not in position_pnl_per_ticker['ticker'].values for x in pnl_per_ticker['ticker']]
    pnl_per_ticker['pnl_position'][position_zero_indx] = 0
    pnl_per_ticker['pnl_intraday'][intraday_zero_indx] = 0
    pnl_per_ticker['pnl_total'] = pnl_per_ticker['pnl_position']+pnl_per_ticker['pnl_intraday']
    pnl_per_ticker.set_index('ticker', drop=True, inplace=True)

    pnl_per_tickerhead = pd.merge(position_pnl_per_tickerhead,intraday_pnl_per_tickerhead,how='outer',on='ticker_head')
    intraday_zero_indx = [x not in intraday_pnl_per_tickerhead['ticker_head'].values for x in pnl_per_tickerhead['ticker_head']]
    position_zero_indx = [x not in position_pnl_per_tickerhead['ticker_head'].values for x in pnl_per_tickerhead['ticker_head']]
    pnl_per_tickerhead['pnl_position'][position_zero_indx] = 0
    pnl_per_tickerhead['pnl_intraday'][intraday_zero_indx] = 0
    pnl_per_tickerhead['pnl_total'] = pnl_per_tickerhead['pnl_position']+pnl_per_tickerhead['pnl_intraday']
    pnl_per_tickerhead.set_index('ticker_head', drop=True, inplace=True)

    if 'con' not in kwargs.keys():
        con.close()

    return {'total_pnl': int(position_pnl+intraday_pnl - t_cost),
            'position_pnl': int(position_pnl),
            'intraday_pnl': int(intraday_pnl),
            't_cost': int(t_cost),
            'pnl_per_ticker': pnl_per_ticker,
            'pnl_per_tickerhead':pnl_per_tickerhead}


def get_strategy_pnl(**kwargs):

    alias = kwargs['alias']
    con = msu.get_my_sql_connection(**kwargs)

    strategy_info = ts.get_strategy_info_from_alias(alias=alias, con=con)

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    open_date = int(strategy_info['open_date'].strftime('%Y%m%d'))
    close_date = int(strategy_info['close_date'].strftime('%Y%m%d'))

    if close_date>as_of_date:
        close_date = as_of_date

    bus_day_list = exp.get_bus_day_list(date_from=open_date,date_to=close_date)

    trades_frame = ts.get_trades_4strategy_alias(alias=alias,con=con)
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in trades_frame['ticker']]
    unique_ticker_head_list = list(set(ticker_head_list))

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in unique_ticker_head_list}

    trades_frame['contract_multiplier'] = [cmi.contract_multiplier[x] for x in ticker_head_list]
    trades_frame['t_cost'] = [cmi.t_cost[x] for x in ticker_head_list]

    pnl_path = [get_strategy_pnl_4day(alias=alias,pnl_date=x,con=con,
                                      trades_frame=trades_frame,
                                      futures_data_dictionary=futures_data_dictionary) for x in bus_day_list]

    pnl_per_tickerhead_list = [x['pnl_per_tickerhead'] for x in pnl_path]
    pnl_per_tickerhead = pd.concat(pnl_per_tickerhead_list, axis=1)
    pnl_per_tickerhead = pnl_per_tickerhead['pnl_total']
    pnl_per_tickerhead = pnl_per_tickerhead.transpose()

    if len(unique_ticker_head_list)>1:
        zero_indx = [[x not in y.index for y in pnl_per_tickerhead_list] for x in pnl_per_tickerhead.columns]

        for i in range(len(pnl_per_tickerhead.columns)):
            pnl_per_tickerhead.iloc[:, i][zero_indx[i]] = 0


    pnl_per_tickerhead['settle_date'] = bus_day_list
    pnl_per_tickerhead.reset_index(inplace=True,drop=True)

    pnl_frame = pd.DataFrame(pnl_path)
    pnl_frame['settle_date'] = bus_day_list

    if 'con' not in kwargs.keys():
        con.close()

    return {'pnl_frame': pnl_frame[['settle_date','position_pnl','intraday_pnl','t_cost','total_pnl']],
            'pnl_per_tickerhead': pnl_per_tickerhead,
            'daily_pnl': pnl_frame['total_pnl'].values[-1],
            'total_pnl': pnl_frame['total_pnl'].sum()}


def close_strategy(**kwargs):

    alias = kwargs['alias']

    if 'close_date' in kwargs.keys():
        close_date = kwargs['close_date']
    else:
        close_date = exp.doubledate_shift_bus_days()

    now_time = dt.datetime.now()
    now_date = now_time.date()

    con = msu.get_my_sql_connection(**kwargs)

    net_position = ts.get_net_position_4strategy_alias(alias=alias, con=con, as_of_date=close_date)

    if net_position.empty:

        pnl_output = get_strategy_pnl(alias=alias, con=con, as_of_date=close_date)
        total_pnl = pnl_output['total_pnl']
        cur = con.cursor()

        query_str = 'UPDATE strategy SET pnl=' + str(total_pnl) + \
                  ', close_date=' + str(close_date) + \
                  ', last_updated_date=' + now_date.strftime('%Y%m%d') + \
                  ' WHERE alias=\'' + alias + '\''
        cur.execute(query_str)
        con.commit()

    else:
        print(alias + ' is not empty ! ')
        return

    if 'con' not in kwargs.keys():
        con.close()
