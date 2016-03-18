__author__ = 'kocat_000'

import formats.futures_strategy_formats as fsf
import formats.strategy_followup_formats as sff
import formats.risk_pnl_formats as rpf
import my_sql_routines.futures_price_loader as fpl
import my_sql_routines.my_sql_utilities as msu
import get_price.presave_price as pp
import opportunity_constructs.futures_butterfly as fb
import contract_utilities.expiration as exp
import ta.prepare_daily as prep

con = msu.get_my_sql_connection()

fpl.update_futures_price_database(con=con)
pp.generate_and_update_futures_data_files(ticker_head_list='butterfly')

report_date = exp.doubledate_shift_bus_days()
fb.generate_futures_butterfly_sheet_4date(date_to=report_date, con=con)

fsf.generate_futures_butterfly_formatted_output()

prep.prepare_strategy_daily(strategy_class='futures_butterfly')

try:
    fsf.generate_curve_pca_formatted_output()
    prep.prepare_strategy_daily(strategy_class='curve_pca')
except Exception:
    pass

try:
    rpf.generate_historic_risk_report(as_of_date=report_date, con=con)
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='risk', folder_date=report_date)
except Exception:
    pass

try:
    rpf.generate_portfolio_pnl_report(as_of_date=report_date, con=con)
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='pnl', folder_date=report_date)
except Exception:
    pass

try:
    writer_out = sff.generate_futures_butterfly_followup_report(as_of_date=report_date, con=con)
    sff.generate_spread_carry_followup_report(as_of_date=report_date, con=con, writer=writer_out)
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='followup', folder_date=report_date)
except Exception:
    pass

con.close()

