
import contract_utilities.contract_lists as cl
import signals.futures_directional_signals as fds
import os.path
import ta.strategy as ts
import pandas as pd


def get_tickers_4date(**kwargs):
    data_out = cl.generate_futures_list_dataframe(**kwargs)
    data_out = data_out[(data_out['tr_dte'] > 30)&(data_out['ticker_head']!='B')]
    data_out.sort(['ticker_head', 'tr_dte'],ascending=[True,True], inplace=True)
    data_out.drop_duplicates(subset=['ticker_head'], take_last=False, inplace=True)
    data_out.reset_index(drop=True,inplace=True)
    return data_out[['ticker', 'ticker_head', 'ticker_class']]


def get_cot_sheet_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='cot', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        cot_sheet = pd.read_pickle(output_dir + '/summary.pkl')
        return {'cot_sheet': cot_sheet,'success': True}

    cot_sheet = get_tickers_4date(**kwargs)
    signals_output = [fds.get_cot_strategy_signals(ticker=x, date_to=date_to) for x in cot_sheet['ticker']]
    cot_sheet = pd.concat([cot_sheet, pd.DataFrame(signals_output)], axis=1)
    cot_sheet = cot_sheet[cot_sheet['success']]

    cot_sheet = cot_sheet[['ticker','ticker_head','ticker_class',
                         'change_5_normalized','change_10_normalized','change_20_normalized','change_40_normalized', 'change_60_normalized',
                         'comm_net_change_1_normalized','comm_net_change_2_normalized', 'comm_net_change_4_normalized',
                         'spec_net_change_1_normalized', 'spec_net_change_2_normalized','spec_net_change_4_normalized',
                         'volume_5_normalized','volume_10_normalized','volume_20_normalized','comm_z','spec_z',
                         'normalized_pnl_5', 'normalized_pnl_10','normalized_pnl_20']]

    cot_sheet.to_pickle(output_dir + '/summary.pkl')

    return {'cot_sheet': cot_sheet,'success': True}

