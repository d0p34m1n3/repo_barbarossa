

import contract_utilities.expiration as exp
import get_price.save_stock_data as ssd
import signals.stock_mr as smr
import ta.strategy as ts
import pandas as pd
import os.path

def get_symbols(**kwargs):

    settle_date = kwargs['settle_date']

    other_frame = ssd.get_symbol_frame(frame_type='other', settle_date=settle_date)
    nasdaq_frame = ssd.get_symbol_frame(frame_type='nasdaq', settle_date=settle_date)

    other_frame = other_frame[(['$' not in x and '.' not in x for x in other_frame['ACT Symbol']])&(other_frame['ETF']=='N')]
    nasdaq_frame = nasdaq_frame[(['$' not in x and '.' not in x for x in nasdaq_frame['Symbol']])&(nasdaq_frame['ETF']=='N')]
    symbol_list = list(set(list(nasdaq_frame['Symbol'].unique()) + list(other_frame['ACT Symbol'].unique())))

    return symbol_list

def get_smrl_signal_frame(**kwargs):

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
    else:
        settle_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='smrl', report_date=settle_date)

    if os.path.isfile(output_dir + '/summary.pkl'):
        stocks = pd.read_pickle(output_dir + '/summary.pkl')
        return {'stocks': stocks, 'success': True}

    symbol_list = get_symbols(settle_date=settle_date)

    result_list = [smr.get_stock_mrl_signals(symbol=symbol_list[i],settle_date=settle_date) for i in range(len(symbol_list))]

    stocks = pd.DataFrame(result_list)

    stocks.to_pickle(output_dir + '/summary.pkl')

    return {'stocks': stocks, 'success': True}

def get_smrs_signal_frame(**kwargs):

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
    else:
        settle_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='smrs', report_date=settle_date)

    if os.path.isfile(output_dir + '/summary.pkl'):
        stocks = pd.read_pickle(output_dir + '/summary.pkl')
        return {'stocks': stocks, 'success': True}

    symbol_list = get_symbols(settle_date=settle_date)

    result_list = [smr.get_stock_mrs_signals(symbol=symbol_list[i],settle_date=settle_date) for i in range(len(symbol_list))]

    stocks = pd.DataFrame(result_list)

    stocks.to_pickle(output_dir + '/summary.pkl')

    return {'stocks': stocks, 'success': True}












