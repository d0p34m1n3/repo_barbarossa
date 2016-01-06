__author__ = 'kocat_000'

import os

root_home = r'C:\Users\kocat_000\quantFinance'
root_work = r'C:\Research'

extension_dict = {'presaved_futures_data': '/data/futures_data',
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















