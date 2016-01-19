__author__ = 'kocat_000'

import shared.utils as su


def get_futures_butterfly_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    if 'long1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['Q'] <= 15) & (data_frame_input['QF'] <= 40))

    if 'short1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['Q'] >= 85) & (data_frame_input['QF'] >= 60))

    if 'long2' in filter_list:
        selection_indx = selection_indx|((data_frame_input['Q'] <= 15) & (data_frame_input['QF'] <= 40) &
                                         (data_frame_input['recent_5day_pnl'] > 2*data_frame_input['downside']))

    if 'short2' in filter_list:
        selection_indx = selection_indx|((data_frame_input['Q'] >= 85) & (data_frame_input['QF'] >= 60) &
                                         (data_frame_input['recent_5day_pnl'] < 2*data_frame_input['upside']))

    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }


def get_curve_pca_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    median_factor_load2 = data_frame_input['factor_load2'].median()

    if median_factor_load2 > 0:
        daily_report_filtered = data_frame_input[data_frame_input['factor_load2'] >= 0]
    else:
        daily_report_filtered = data_frame_input[data_frame_input['factor_load2'] <= 0]

    daily_report_filtered.sort('z', ascending=True, inplace=True)
    num_contract_4side = round(len(daily_report_filtered.index)/4)

    if 'long1' in filter_list:
        selected_trades = daily_report_filtered.iloc[:num_contract_4side]
        selection_indx = su.list_or(selection_indx, [x in selected_trades['ticker1'].values for x in data_frame_input['ticker1'].values])
    if 'short1' in filter_list:
        selected_trades = daily_report_filtered.iloc[-num_contract_4side:]
        selection_indx = su.list_or(selection_indx, [x in selected_trades['ticker1'].values for x in data_frame_input['ticker1'].values])

    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }





