__author__ = 'kocat_000'


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






