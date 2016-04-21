
import ta.strategy as tas
import shared.calendar_utilities as cu
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import get_price.get_options_price as gop
import option_models.utils as omu
import pandas as pd
import numpy as np
import ta.get_intraday_prices as gip
import contract_utilities.contract_meta_info as cmi
pd.options.mode.chained_assignment = None  # default='warn'
import shared.converters as sc


def get_hedge_4strategy(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    current_date = cu.get_doubledate()
    settle_price_date = exp.doubledate_shift_bus_days(double_date=current_date, shift_in_days=1)

    position_frame = tas.get_net_position_4strategy_alias(alias=kwargs['alias'],as_of_date=current_date,con=con)

    intraday_price_frame = gip.get_cme_direct_prices()
    intraday_price_frame.rename(columns={'ticker': 'underlying_ticker'},inplace=True)

    options_frame = position_frame[position_frame['instrument'] == 'O']
    futures_frame = position_frame[position_frame['instrument'] == 'F']

    if options_frame.empty:
        futures_frame.rename(columns={'ticker': 'underlying_ticker', 'qty': 'underlying_delta'},inplace=True)
        futures_frame = futures_frame[['underlying_ticker', 'underlying_delta']]
        net_position = pd.merge(futures_frame, intraday_price_frame, how='left', on='underlying_ticker')
        net_position['hedge_price'] = (net_position['bid_price']+net_position['ask_price'])/2
        net_position['hedge'] = -net_position['underlying_delta']
        return net_position


    imp_vol_list = [gop.get_options_price_from_db(ticker=options_frame['ticker'].iloc[x],
                                  settle_date=settle_price_date,
                                  strike=options_frame['strike_price'].iloc[x],
                                  option_type=options_frame['option_type'].iloc[x],
                                  column_names=['imp_vol'],
                                  con=con) for x in range(len(options_frame.index))]

    imp_vol_frame = pd.concat(imp_vol_list)
    imp_vol_frame.reset_index(drop=True, inplace=True)
    options_frame.reset_index(drop=True, inplace=True)

    options_frame = pd.concat([options_frame, imp_vol_frame], axis=1)

    options_frame['underlying_ticker'] = [omu.get_option_underlying(ticker=x) for x in options_frame['ticker']]

    options_frame = pd.merge(options_frame, intraday_price_frame, how='left', on='underlying_ticker')

    options_frame['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in options_frame['ticker']]
    options_frame['exercise_type'] = [cmi.get_option_exercise_type(ticker_head=x) for x in options_frame['ticker_head']]
    options_frame['strike_price'] = options_frame['strike_price'].astype('float64')

    options_frame['mid_price'] = (options_frame['bid_price']+options_frame['ask_price'])/2

    options_frame['delta'] = [omu.option_model_wrapper(ticker=options_frame['ticker'].iloc[x],
                             calculation_date=current_date,
                             interest_rate_date=settle_price_date,
                             underlying=options_frame['mid_price'].iloc[x],
                             strike=options_frame['strike_price'].iloc[x],
                             implied_vol=options_frame['imp_vol'].iloc[x],
                             option_type=options_frame['option_type'].iloc[x],
                             exercise_type=options_frame['exercise_type'].iloc[x],
                             con=con)['delta'] for x in range(len(options_frame.index))]

    options_frame['total_delta'] = options_frame['qty']*options_frame['delta']

    grouped = options_frame.groupby('underlying_ticker')

    net_position = pd.DataFrame()

    net_position['underlying_ticker'] = (grouped['underlying_ticker'].first()).values
    net_position['hedge_price'] = (grouped['mid_price'].first()).values
    net_position['option_delta'] = (grouped['total_delta'].sum()).values
    net_position['option_delta'] = net_position['option_delta'].round(2)

    if futures_frame.empty:
        net_position['total_delta'] = net_position['option_delta']
    else:
        futures_frame.rename(columns={'ticker': 'underlying_ticker', 'qty': 'underlying_delta'},inplace=True)
        futures_frame = futures_frame[['underlying_ticker', 'underlying_delta']]
        net_position = pd.merge(net_position, futures_frame, how='outer', on='underlying_ticker')
        net_position['total_delta'] = net_position['option_delta']+net_position['underlying_delta']

    net_position['hedge'] = -net_position['total_delta']

    if 'con' not in kwargs.keys():
        con.close()

    return net_position


def hedge_strategy_against_delta(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    hedge_results = get_hedge_4strategy(alias=kwargs['alias'], con=con)

    trade_frame = pd.DataFrame()
    trade_frame['ticker'] = hedge_results['underlying_ticker']
    trade_frame['option_type'] = None
    trade_frame['strike_price'] = np.NaN
    trade_frame['trade_price'] = hedge_results['hedge_price']
    trade_frame['trade_quantity'] = hedge_results['hedge']
    trade_frame['instrument'] = 'F'
    trade_frame['real_tradeQ'] = True
    trade_frame['alias'] = kwargs['alias']

    tas.load_trades_2strategy(trade_frame=trade_frame,con=con)

    trade_frame['trade_quantity'] = -trade_frame['trade_quantity']
    trade_frame['alias'] = 'Delta'
    tas.load_trades_2strategy(trade_frame=trade_frame,con=con)

    if 'con' not in kwargs.keys():
        con.close()


def strategy_hedge_report(**kwargs):

    current_date = cu.get_doubledate()

    con = msu.get_my_sql_connection(**kwargs)

    strategy_frame = tas.get_open_strategies(as_of_date=current_date,con=con)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    vcs_indx = [x == 'vcs' for x in strategy_class_list]
    hedge_frame = strategy_frame[vcs_indx]

    [hedge_strategy_against_delta(alias=x, con=con) for x in hedge_frame['alias']]

    if 'con' not in kwargs.keys():
        con.close()











