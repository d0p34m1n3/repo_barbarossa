
import shared.directory_names as dn
import shared.calendar_utilities as cu
import pandas as pd


def get_stock_price_preloaded(**kwargs):

    ticker = kwargs['ticker']

    if 'stock_data_dictionary' in kwargs.keys():
        data_out = kwargs['stock_data_dictionary'][ticker]
    else:
        file_dir = dn.get_directory_name(ext='stock_data')
        data_out = pd.read_pickle(file_dir + '/' + kwargs['ticker'] + '.pkl')

    if 'settle_date' in kwargs.keys():
        settle_date = kwargs['settle_date']
        if isinstance(settle_date,int):
            data_out = data_out[data_out['settle_datetime'] == cu.convert_doubledate_2datetime(settle_date)]
        elif isinstance(settle_date,dt.datetime):
            data_out = data_out[data_out['settle_datetime'] == settle_date]

    if 'settle_date_from' in kwargs.keys():
        data_out = data_out[data_out['settle_datetime']>=cu.convert_doubledate_2datetime(kwargs['settle_date_from'])]

    if 'settle_date_to' in kwargs.keys():
        data_out = data_out[data_out['settle_datetime']<=cu.convert_doubledate_2datetime(kwargs['settle_date_to'])]

    return data_out

