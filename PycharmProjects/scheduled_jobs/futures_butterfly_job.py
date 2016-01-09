__author__ = 'kocat_000'

import formats.futures_strategy_formats as fsf
import my_sql_routines.futures_price_loader as fpl
import get_price.presave_price as pp
import opportunity_constructs.futures_butterfly as fb
import contract_utilities.expiration as exp
import ta.prepare_daily as prep


fpl.update_futures_price_database()
pp.generate_and_update_futures_data_files()

report_date = exp.doubledate_shift_bus_days()
fb.generate_futures_butterfly_sheet_4date(date_to=report_date)

fsf.generate_futures_butterfly_formatted_output()

prep.prepare_futures_butterfly()
