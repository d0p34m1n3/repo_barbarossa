__author__ = 'kocat_000'

import os
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu

root_home = r'C:\Users\kocat_000\quantFinance'
root_work = r'C:\Research'
tt_fill_directory = r'C:\tt\datfiles\Export'


extension_dict = {'presaved_futures_data': '/data/futures_data',
                  'intraday_ttapi_data': '/data/intraday_data/tt_api',
                  'intraday_ttapi_data_fixed_interval': '/data/intraday_data/tt_api_fixed_interval',
                  'raw_options_data': '/data/options_data_raw',
                  'options_backtesting_data': '/data/options_backtesting_data',
                  'option_model_test_data': '/data/option_model_tests',
                  'aligned_time_series_output': '/data/alignedTimeSeriesOutputTemp',
                  'ta': '/ta',
                  'strategy_output': '/strategy_output',
                  'backtest_results': '/backtest_results',
                  'daily': '/daily',
                  'python_file': '/PycharmProjects'}


def get_directory_name(**kwargs):

    computer_name = os.environ['COMPUTERNAME']

    ext = kwargs['ext']

    if computer_name == '601-TREKW71' or computer_name == '601-TREKW72':
        root_dir = root_work
    else:
        root_dir = root_home

    directory_name = root_dir + extension_dict[ext]

    if not os.path.exists(directory_name):
        os.makedirs(directory_name)

    return directory_name


def get_dated_directory_extension(**kwargs):

    if 'folder_date' in kwargs.keys():
        folder_date = kwargs['folder_date']
    else:
        folder_date = exp.doubledate_shift_bus_days()

    if 'ext' not in kwargs.keys():
        print('Need to provide a valid ext !')
        return

    directory_name = get_directory_name(**kwargs)

    dated_directory_name = directory_name + '/' + cu.get_directory_extension(folder_date)

    if not os.path.exists(dated_directory_name):
        os.makedirs(dated_directory_name)

    return dated_directory_name












