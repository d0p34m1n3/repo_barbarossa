

import get_price.save_stock_data as ssd
import contract_utilities.expiration as exp
from pandas_finance import Equity
import shared.directory_names as dn
import pandas as pd
import os.path

directory_name = dn.get_directory_name(ext='fundamental_data')

def create_sector_classification_file(**kwargs):

    file_name = directory_name + '\sector_classification.pkl'

    if os.path.isfile(file_name):
        return pd.read_pickle(file_name)

    if 'report_date' in kwargs.keys():
        report_date = kwargs['report_date']
    else:
        report_date = exp.doubledate_shift_bus_days()

    symbol_list = ssd.get_symbol_list_4date(settle_date=report_date)

    for i in range(len(symbol_list)):
        eqty = Equity(symbol_list[i])
        try:
            sector_list.append(eqty.sector)
            industry_list.append(eqty.industry)
        except:
            sector_list.append(None)
            industry_list.append(None)

    sector_classification = pd.DataFrame.from_dict({'ticker': symbol_list, 'sector': sector_list, 'industry': industry_list})
    sector_classification.to_pickle(file_name)
    return sector_classification

def get_sector_classification(**kwargs):

    ticker = kwargs['ticker']
    sector_classification = pd.read_pickle(directory_name + '\sector_classification.pkl')

    selected_row = sector_classification[sector_classification['ticker']==ticker]
    return{'sector':selected_row['sector'].iloc[0],'industry':selected_row['industry'].iloc[0]}




