
import contract_utilities.expiration as exp
import ta.strategy as ts
import shutil as sutil
import shared.directory_names as dn

daily_dir = dn.get_directory_name(ext='daily')


def prepare_futures_butterfly(**kwargs):

    report_date = exp.doubledate_shift_bus_days()
    output_dir = ts.create_strategy_output_dir(strategy_class='futures_butterfly', report_date=report_date)

    sutil.copyfile(output_dir + '/butterflies.xlsx', daily_dir + '/butterfly_' + str(report_date) + '.xlsx')



