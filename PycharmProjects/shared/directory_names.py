__author__ = 'kocat_000'

import os
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu

root_home = r'C:\Users\kocat_000\quantFinance'
root_work = r'C:\Research'
tt_fill_directory = r'C:\tt\datfiles\Export'


extension_dict = {'presaved_futures_data': '/data/futures_data',
                  'ta': '/ta',
                  'strategy_output': '/strategy_output',
                  'backtest_results': '/backtest_results',
                  'daily': '/daily'}


def get_directory_name(**kwargs):

    computer_name = os.environ['COMPUTERNAME']

    ext = kwargs['ext']

    if computer_name=='601-TREKW71' or computer_name=='601-TREKW72':
        root_dir = root_work
    else:
        root_dir = root_home

    return root_dir + extension_dict[ext]


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












