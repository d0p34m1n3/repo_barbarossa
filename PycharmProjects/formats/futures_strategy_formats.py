__author__ = 'kocat_000'

import contract_utilities.expiration as exp
import opportunity_constructs.futures_butterfly as fb
import opportunity_constructs.curve_pca as cpc
import signals.futures_filters as ff
import shared.directory_names as dn
import pandas as pd
import ta.strategy as ts

xls_file_names = {'futures_butterfly': 'butterflies',
                  'curve_pca': 'curve_pca'}


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
                                                  'Q', 'QF', 'z1', 'z2','z3','z4', 'theo_pnl', 'r1', 'r2', 'bf_price',
                                                  'RC', 'seasonality','second_spread_weight_1', 'upside', 'downside',
                                                  'recent_vol_ratio', 'recent_5day_pnl']]

    good_butterflies_w_selected_columns = good_butterflies[['ticker1', 'ticker2', 'ticker3',
                                                  'tickerHead', 'trDte1', 'trDte2', 'trDte3',
                                                  'Q', 'QF', 'z1', 'z2','z3','z4', 'theo_pnl', 'r1', 'r2', 'bf_price',
                                                  'RC', 'seasonality', 'second_spread_weight_1', 'upside', 'downside',
                                                  'recent_vol_ratio', 'recent_5day_pnl']]

    writer = pd.ExcelWriter(output_dir + '/' + xls_file_names['futures_butterfly'] + '.xlsx', engine='xlsxwriter')

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


def generate_curve_pca_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='curve_pca', report_date=report_date)

    ticker_head_list = ['CL', 'B']
    selected_column_list = ['ticker1', 'ticker2', 'monthSpread', 'tr_dte_front', 'residuals', 'price', 'yield', 'z', 'factor_load1', 'factor_load2']
    writer = pd.ExcelWriter(output_dir + '/' + xls_file_names['curve_pca'] + '.xlsx', engine='xlsxwriter')

    for ticker_head in ticker_head_list:

        curve_pca_output = cpc.get_curve_pca_report(ticker_head=ticker_head, date_to=report_date)

        if curve_pca_output['success']:

            all_spreads = curve_pca_output['pca_results']
            filter_out = ff.get_curve_pca_filters(data_frame_input=all_spreads, filter_list=['long1', 'short1'])
            good_spreads = filter_out['selected_frame']

            all_spreads = all_spreads[selected_column_list]
            good_spreads = good_spreads[selected_column_list]

            all_spreads.to_excel(writer, sheet_name=ticker_head + '-all')
            good_spreads.to_excel(writer, sheet_name=ticker_head + '-good')

            worksheet_good = writer.sheets[ticker_head + '-good']
            worksheet_all = writer.sheets[ticker_head + '-all']

            worksheet_good.freeze_panes(1, 0)
            worksheet_all.freeze_panes(1, 0)

            worksheet_good.autofilter(0, 0, len(good_spreads.index), len(selected_column_list))

            worksheet_all.autofilter(0, 0, len(all_spreads.index), len(selected_column_list))




