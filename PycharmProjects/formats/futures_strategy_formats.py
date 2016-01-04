__author__ = 'kocat_000'

import contract_utilities.expiration as exp
import opportunity_constructs.futures_butterfly as fb
import signals.futures_filters as ff
import shared.directory_names as dn
import pandas as pd
import ta.strategy as ts

def generate_futures_butterfly_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    butterfly_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = butterfly_output['butterflies']

    filter_out = ff.get_futures_butterfly_filters(data_frame_input=butterflies, filter_list=['long1', 'short1'])
    good_butterflies = filter_out['selected_frame']

    butterflies_w_selected_columns = butterflies[['ticker1', 'ticker2', 'ticker3',
                                                  'tickerHead', 'trDte1', 'trDte2', 'trDte3',
                                                  'Q', 'QF', 'z1', 'z2', 'theo_pnl', 'r1', 'r2', 'bf_price',
                                                  'RC', 'second_spread_weight_1', 'upside', 'downside',
                                                  'recent_vol_ratio', 'recent_5day_pnl']]

    good_butterflies_w_selected_columns = good_butterflies[['ticker1', 'ticker2', 'ticker3',
                                                  'tickerHead', 'trDte1', 'trDte2', 'trDte3',
                                                  'Q', 'QF', 'z1', 'z2', 'theo_pnl', 'r1', 'r2', 'bf_price',
                                                  'RC', 'second_spread_weight_1', 'upside', 'downside',
                                                  'recent_vol_ratio', 'recent_5day_pnl']]

    writer = pd.ExcelWriter(output_dir + '/butterflies.xlsx', engine='xlsxwriter')

    butterflies_w_selected_columns.to_excel(writer, sheet_name='all')
    good_butterflies_w_selected_columns.to_excel(writer, sheet_name='good')

    worksheet_good = writer.sheets['good']
    worksheet_all = writer.sheets['all']

    worksheet_good.freeze_panes(1, 0)
    worksheet_all.freeze_panes(1, 0)

    worksheet_good.autofilter(0, 0, len(good_butterflies_w_selected_columns.index),
                              len(good_butterflies_w_selected_columns.columns))

    worksheet_all.autofilter(0, 0, len(butterflies_w_selected_columns.index),
                                   len(butterflies_w_selected_columns.columns))