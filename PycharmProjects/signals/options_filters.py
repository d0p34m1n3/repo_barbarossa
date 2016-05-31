
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

    if 'long2' in filter_list:

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Ag') &
                                         (data_frame_input['Q1'] <= 34) &
                                         (data_frame_input['Q'] <= 21))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Livestock') &
                                         (data_frame_input['Q1'] <= 37) &
                                         (data_frame_input['Q'] <= 16))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Metal') &
                                         (data_frame_input['Q1'] <= 28) &
                                         (data_frame_input['Q'] <= 31))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'FX') &
                                         (data_frame_input['Q1'] <= 24) &
                                         (data_frame_input['Q'] <= 12))

        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'CL') &
                                         (data_frame_input['Q1'] <= 31) &
                                         (data_frame_input['Q'] <= 4))

    if 'short2' in filter_list:

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Ag') &
                                         (data_frame_input['Q1'] >= 66) &
                                         (data_frame_input['Q'] >= 65))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Livestock') &
                                         (data_frame_input['Q1'] >= 70) &
                                         (data_frame_input['Q'] >= 65))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Metal') &
                                         (data_frame_input['Q1'] >= 66) &
                                         (data_frame_input['Q'] >= 61))

        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'CL') &
                                         (data_frame_input['Q1'] >= 67) &
                                         (data_frame_input['Q'] >= 33))

        selection_indx = selection_indx|((data_frame_input['tickerHead'] == 'NG') &
                                         (data_frame_input['Q1'] >= 66) &
                                         (data_frame_input['Q'] >= 69))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Treasury') &
                                         (data_frame_input['Q1'] >= 71) &
                                         (data_frame_input['Q'] >= 93))

        selection_indx = selection_indx|((data_frame_input['tickerClass'] == 'Index') &
                                         (data_frame_input['Q1'] >= 60) &
                                         (data_frame_input['Q'] >= 74))

    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }


def get_scv_filters(**kwargs):

    data_frame_input = kwargs['data_frame_input']
    filter_list = kwargs['filter_list']

    selection_indx = [False]*len(data_frame_input.index)

    if 'long1' in filter_list:

        selection_indx = selection_indx|((data_frame_input['premium'] <= -28) & (data_frame_input['Q'] <= 29))

        selection_indx = selection_indx|((data_frame_input['premium'] > -28) &
                                         (data_frame_input['premium'] <= -7) &
                                         (data_frame_input['Q'] <= 14))

    if 'short1' in filter_list:

        selection_indx = selection_indx|((data_frame_input['premium'] >= 21) & (data_frame_input['Q'] >= 80))

        selection_indx = selection_indx|((data_frame_input['premium'] < 21) &
                                         (data_frame_input['premium'] >= 12) &
                                         (data_frame_input['Q'] >= 91))

    return {'selected_frame': data_frame_input[selection_indx],'selection_indx': selection_indx }

