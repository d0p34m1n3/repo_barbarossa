

import contract_utilities.contract_meta_info as cmi
import signals.futures_directional_signals as fds
import ta.strategy as ts
import pandas as pd
import os.path


def generate_fm_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='fm', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        futures = pd.read_pickle(output_dir + '/summary.pkl')
        return {'futures': futures,'success': True}

    ticker_head_list = list(set(cmi.cme_futures_tickerhead_list)|set(cmi.futures_butterfly_strategy_tickerhead_list))

    signals_list = [fds.get_fm_signals(ticker_head=x, date_to=date_to) for x in ticker_head_list]

    futures = pd.DataFrame()

    futures['ticker'] = [x['ticker'] for x in signals_list]
    futures['ticker_head'] = ticker_head_list
    futures['comm_cot_index_slow'] = [x['comm_cot_index_slow'] for x in signals_list]
    futures['comm_cot_index_fast'] = [x['comm_cot_index_fast'] for x in signals_list]
    futures['trend_direction'] = [x['trend_direction'] for x in signals_list]
    futures['curve_slope'] = [x['curve_slope'] for x in signals_list]

    futures['rsi_3'] = [x['rsi_3'] for x in signals_list]
    futures['rsi_7'] = [x['rsi_7'] for x in signals_list]
    futures['rsi_14'] = [x['rsi_14'] for x in signals_list]

    futures['change1'] = [x['change1'] for x in signals_list]
    futures['change1_instant'] = [x['change1_instant'] for x in signals_list]
    futures['change5'] = [x['change5'] for x in signals_list]
    futures['change10'] = [x['change10'] for x in signals_list]
    futures['change20'] = [x['change20'] for x in signals_list]

    futures['change1_dollar'] = [x['change1_dollar'] for x in signals_list]
    futures['change1_instant_dollar'] = [x['change1_instant_dollar'] for x in signals_list]
    futures['change5_dollar'] = [x['change5_dollar'] for x in signals_list]
    futures['change10_dollar'] = [x['change10_dollar'] for x in signals_list]
    futures['change20_dollar'] = [x['change20_dollar'] for x in signals_list]

    writer = pd.ExcelWriter(output_dir + '/summary.xlsx', engine='xlsxwriter')
    futures.to_excel(writer, sheet_name='all')

    futures.to_pickle(output_dir + '/summary.pkl')

    return {'futures': futures,'success': True}

