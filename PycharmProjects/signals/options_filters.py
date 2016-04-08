
def get_vcs_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    if 'long1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Ag') & (data_frame_input['Q'] <= 13))
        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'CL') & (data_frame_input['Q'] <= 2))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'FX') & (data_frame_input['Q'] <= 4))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Livestock') & (data_frame_input['Q'] <= 10))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Metal') & (data_frame_input['Q'] <= 15))

    if 'short1' in filter_list:
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Ag') & (data_frame_input['Q'] >= 90))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Treasury') & (data_frame_input['Q'] >= 93))
        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'CL') & (data_frame_input['Q'] >= 60))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Index') & (data_frame_input['Q'] >= 84))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Livestock') & (data_frame_input['Q'] >= 70))
        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Metal') & (data_frame_input['Q'] >= 64))

    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }