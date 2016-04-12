
import shared.directory_names as dn
import pandas as pd
price_file_name = 'cme_direct_prices.csv'

def get_cme_direct_prices(**kwargs):

    cme_frame = pd.read_csv(dn.get_directory_name(ext='daily') + '/' + price_file_name)
    return cme_frame

