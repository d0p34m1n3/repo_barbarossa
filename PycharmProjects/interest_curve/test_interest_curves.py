
import contract_utilities.contract_lists as cl
import contract_utilities.expiration as exp
import interest_curve.get_rate_from_stir as grfs
import pandas as pd
import numpy as np
import shared.directory_names as dn
import os.path


def generate_test_file_4stir_rates(**kwargs):

    output_dir = dn.get_directory_name(ext='test_data')
    full_dates = exp.get_bus_day_list(date_from=20100101,date_to=20160821)
    #full_dates = exp.get_bus_day_list(date_from=20160812,date_to=20160821)
    bus_dates_select = full_dates[0::6]
    data_frame_list = []
    #bus_dates_select = bus_dates_select[:200]

    for i in range(len(bus_dates_select)):
        #print(bus_dates_select[i])
        date_file_name = output_dir + '/' + str(bus_dates_select[i]) + '.pkl'

        if os.path.isfile(date_file_name):
            liquid_options = pd.read_pickle(date_file_name)
        else:
            liquid_options = cl.generate_liquid_options_list_dataframe(settle_date=bus_dates_select[i])
            liquid_options.drop_duplicates('expiration_date',inplace=True)
            liquid_options = liquid_options[['expiration_date']]
            liquid_options['settle_date'] = bus_dates_select[i]
            liquid_options['exp_date'] = liquid_options['expiration_date'].apply(lambda x: 10000*x.year+100*x.month+x.day)
            liquid_options['int_rate'] = liquid_options.apply(lambda x: grfs.get_simple_rate(as_of_date=x['settle_date'], date_to=x['exp_date'])['rate_output'],axis=1)
            liquid_options.to_pickle(date_file_name)

        data_frame_list.append(liquid_options)

    merged_data = pd.concat(data_frame_list)
    merged_data.reset_index(inplace=True,drop=True)
    merged_data = merged_data[['settle_date', 'exp_date','int_rate']]

    writer = pd.ExcelWriter(output_dir + '/' + 'stir_option_rate_test' + '.xlsx', engine='xlsxwriter')
    merged_data.to_excel(writer, sheet_name='all')

