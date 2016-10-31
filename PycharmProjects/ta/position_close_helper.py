
import ta.strategy as tas
import contract_utilities.expiration as exp
import get_price.get_options_price as gop
import my_sql_routines.my_sql_utilities as msu
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import option_models.utils as oputil

def calc_intrday_pnl_from_prices(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    net_position_frame = tas.get_net_position_4strategy_alias(alias=kwargs['alias'], as_of_date=as_of_date)

    net_position_frame['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in net_position_frame['ticker']]
    net_position_frame['contract_multiplier'] = [cmi.contract_multiplier[x] for x in net_position_frame['ticker_head']]

    con = msu.get_my_sql_connection(**kwargs)

    option_frame = net_position_frame[net_position_frame['instrument'] == 'O']

    #option_frame = option_frame[(option_frame['strike_price'] == 54)|(option_frame['strike_price'] == 60)]
    #option_frame = option_frame[option_frame['strike_price'] == 112]
    #option_frame['qty'].loc[option_frame['ticker']=='LNV2016'] = -20

    option_frame['close_price'] = [gop.get_options_price_from_db(ticker=option_frame['ticker'].iloc[x],
                                  strike=option_frame['strike_price'].iloc[x],
                                  option_type=option_frame['option_type'].iloc[x],
                                   con=con,settle_date=as_of_date)['close_price'][0] for x in range(len(option_frame.index))]

    structure_quantity = abs(option_frame['qty']).unique()[0]
    structure_multiplier = option_frame['contract_multiplier'].unique()[0]

    structure_price = sum(option_frame['close_price']*option_frame['qty'])/structure_quantity

    if structure_price<0:
        structure_quantity = -structure_quantity
        structure_price = -structure_price

    structure_pnl = structure_quantity*structure_multiplier*(kwargs['structure_price']-structure_price)

    futures_frame = net_position_frame[net_position_frame['instrument'] == 'F']

    futures_frame['close_price'] = [gfp.get_futures_price_preloaded(ticker=x, settle_date=as_of_date)['close_price'].iloc[0] for x in futures_frame['ticker']]

    futures_frame['intraday_price'] = [kwargs[x] for x in futures_frame['ticker']]

    futures_frame['intraday_pnl'] = (futures_frame['intraday_price']-futures_frame['close_price'])*futures_frame['qty']*futures_frame['contract_multiplier']

    if 'con' not in kwargs.keys():
        con.close()

    return {'structure_pnl': structure_pnl, 'futures_pnl': futures_frame['intraday_pnl'].sum(),'structure_settle': structure_price}


def calc_intraday_structure_pnl_from_prices(**kwargs):

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    con = msu.get_my_sql_connection(**kwargs)

    structure_type = kwargs['structure_type']
    structure_price = kwargs['structure_price']
    ticker_list = kwargs['ticker_list']
    strike_list = kwargs['strike_list']
    underlying_price_list = kwargs['underlying_price_list']
    qty = kwargs['qty']

    if structure_type == 'straddle_spread':
        option_frame = pd.DataFrame.from_items([('ticker', [ticker_list[0], ticker_list[0], ticker_list[1], ticker_list[1]]),
                                                ('option_type', ['C', 'P', 'C', 'P']),
                                                ('strike_price', [strike_list[0], strike_list[0], strike_list[1],strike_list[1]]),
                                                ('qty', [-1, -1, 1, 1])])

    option_price_output = [gop.get_options_price_from_db(ticker=option_frame['ticker'].iloc[x],
                                  strike=option_frame['strike_price'].iloc[x],
                                  option_type=option_frame['option_type'].iloc[x],
                                   con=con,settle_date=as_of_date,column_names=['close_price','delta']) for x in range(len(option_frame.index))]

    option_frame['delta'] = [option_price_output[x]['delta'][0] for x in range(len(option_frame.index))]
    option_frame['close_price'] = [option_price_output[x]['close_price'][0] for x in range(len(option_frame.index))]

    option_frame['PQ'] = option_frame['close_price']*option_frame['qty']
    option_frame['signed_delta'] = option_frame['delta']*option_frame['qty']

    delta_list = [option_frame[option_frame['ticker'] == x]['signed_delta'].sum() for x in ticker_list]

    ticker_head = cmi.get_contract_specs(ticker_list[0])['ticker_head']
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    structure_price_yesterday = option_frame['PQ'].sum()
    structure_pnl = qty*(structure_price-structure_price_yesterday)*contract_multiplier

    underlying_ticker_list = [oputil.get_option_underlying(ticker=x) for x in ticker_list]
    underlying_price_list_yesterday = [gfp.get_futures_price_preloaded(ticker=x, settle_date=as_of_date)['close_price'].iloc[0] for x in underlying_ticker_list]
    delta_pnl = contract_multiplier*sum([-delta_list[x]*qty*(underlying_price_list[x]-underlying_price_list_yesterday[x]) for x in range(len(delta_list))])

    return {'total_pnl': structure_pnl+delta_pnl, 'structure_pnl': structure_pnl,
            'delta_pnl': delta_pnl, 'structure_price_yesterday': structure_price_yesterday }












