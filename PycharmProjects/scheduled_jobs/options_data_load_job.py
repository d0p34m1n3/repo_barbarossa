
import shared.directory_names as dn
import shared.downloads as sd
import shared.calendar_utilities as cu
import pickle as pickle
import my_sql_routines.futures_price_loader as fpl
import my_sql_routines.options_price_loader as opl
import my_sql_routines.my_sql_utilities as msu
import get_price.presave_price as pp

commodity_address = 'ftp://ftp.cmegroup.com/pub/settle/stlags'
equity_address = 'ftp://ftp.cmegroup.com/pub/settle/stleqt'
fx_address = 'ftp://ftp.cmegroup.com/pub/settle/stlcur'
interest_rate_address = 'ftp://ftp.cmegroup.com/pub/settle/stlint'
comex_futures_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/comex_future.csv'
comex_options_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/comex_option.csv'
nymex_futures_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/nymex_future.csv'
nymex_options_csv_address = 'ftp://ftp.cmegroup.com/pub/settle/nymex_option.csv'

folder_date = cu.get_doubledate()

options_data_dir = dn.get_dated_directory_extension(folder_date=folder_date, ext='raw_options_data')

commodity_output = sd.download_txt_from_web(web_address=commodity_address)
equity_output = sd.download_txt_from_web(web_address=equity_address)
fx_output = sd.download_txt_from_web(web_address=fx_address)
interest_rate_output = sd.download_txt_from_web(web_address=interest_rate_address)

comex_futures_output = sd.download_csv_from_web(web_address=comex_futures_csv_address)
comex_options_output = sd.download_csv_from_web(web_address=comex_options_csv_address)

nymex_futures_output = sd.download_csv_from_web(web_address=nymex_futures_csv_address)
nymex_options_output = sd.download_csv_from_web(web_address=nymex_options_csv_address)

with open(options_data_dir + '/commodity.pkl', 'wb') as handle:
        pickle.dump(commodity_output, handle)

with open(options_data_dir + '/equity.pkl', 'wb') as handle:
        pickle.dump(equity_output, handle)

with open(options_data_dir + '/fx.pkl', 'wb') as handle:
        pickle.dump(fx_output, handle)

with open(options_data_dir + '/interest_rate.pkl', 'wb') as handle:
        pickle.dump(interest_rate_output, handle)

with open(options_data_dir + '/comex_futures.pkl', 'wb') as handle:
        pickle.dump(comex_futures_output, handle)

with open(options_data_dir + '/comex_options.pkl', 'wb') as handle:
        pickle.dump(comex_options_output, handle)

with open(options_data_dir + '/nymex_futures.pkl', 'wb') as handle:
        pickle.dump(nymex_futures_output, handle)

with open(options_data_dir + '/nymex_options.pkl', 'wb') as handle:
        pickle.dump(nymex_options_output, handle)

con = msu.get_my_sql_connection()

try:
    fpl.update_futures_price_database_from_cme_file(con=con)
    pp.generate_and_update_futures_data_files(ticker_head_list='cme_futures')
except Exception:
    pass

try:
    opl.update_options_price_database_from_cme_files(con=con, settle_date=folder_date)
except Exception:
    pass

con = msu.get_my_sql_connection()
