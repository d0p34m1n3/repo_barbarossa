
import opportunity_constructs.intraday_breakouts as ibo
import contract_utilities.expiration as exp
import shared.directory_names as dn
import pandas as pd
import ta.strategy as ts


def generate_ibo_formatted_output(**kwargs):

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=report_date)

    out_dictionary = ibo.generate_ibo_sheet_4date(date_to=report_date)
    cov_matrix = out_dictionary['cov_output']['cov_matrix']

    cov_matrix.reset_index(drop=False,inplace=True)

    writer = pd.ExcelWriter(output_dir + '/' + 'cov_matrix.xlsx', engine='xlsxwriter')
    cov_matrix.to_excel(writer, sheet_name='cov_matrix')
    writer.save()

    cov_data_integrity = round(out_dictionary['cov_output']['cov_data_integrity'], 2)

    with open(output_dir + '/' + 'covDataIntegrity.txt','w') as text_file:
        text_file.write(str(cov_data_integrity))
