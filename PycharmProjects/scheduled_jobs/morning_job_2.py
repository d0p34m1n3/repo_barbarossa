
import shared.log as lg

log = lg.get_logger(file_identifier='morning_job_2')

import my_sql_routines.my_sql_utilities as msu
import my_sql_routines.futures_price_loader as fpl
import get_price.presave_price as pp
import opportunity_constructs.spread_carry as sc
import opportunity_constructs.overnight_calendar_spreads as ocs
import formats.futures_strategy_formats as fsf
import contract_utilities.expiration as exp

con = msu.get_my_sql_connection()
report_date = exp.doubledate_shift_bus_days()


try:
    fpl.update_futures_price_database(con=con)
except Exception:
    log.error('update_futures_price_database failed', exc_info=True)
    quit()

try:
    pp.generate_and_update_futures_data_files(ticker_head_list='butterfly')
except Exception:
    log.error('generate_and_update_futures_data_files failed', exc_info=True)
    quit()

try:
    sc.generate_spread_carry_sheet_4date(report_date=report_date)
except Exception:
    log.error('generate_spread_carry_sheet_4date failed', exc_info=True)
    quit()

try:
    ocs.generate_overnight_spreads_sheet_4date(date_to=report_date)
except Exception:
    log.error('generate_overnight_spreads_sheet_4date failed', exc_info=True)
    quit()

try:
    fsf.generate_ocs_formatted_output(report_date=report_date)
except Exception:
    log.error('generate_ocs_formatted_output failed', exc_info=True)
    quit()

con.close()


