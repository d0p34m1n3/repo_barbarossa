
import shared.log as lg

log = lg.get_logger(file_identifier='morning_job_2',log_level='INFO')

import my_sql_routines.my_sql_utilities as msu
import my_sql_routines.futures_price_loader as fpl
import get_price.presave_price as pp
import opportunity_constructs.spread_carry as sc
import opportunity_constructs.overnight_calendar_spreads as ocs
import formats.futures_strategy_formats as fsf
import formats.strategy_followup_formats as sff
import contract_utilities.expiration as exp
import formats.risk_pnl_formats as rpf
import ta.prepare_daily as prep

con = msu.get_my_sql_connection()
report_date = exp.doubledate_shift_bus_days()


try:
    log.info('update_futures_price_database...')
    fpl.update_futures_price_database(con=con)
except Exception:
    log.error('update_futures_price_database failed', exc_info=True)
    quit()

try:
    log.info('generate_and_update_futures_data_files...')
    pp.generate_and_update_futures_data_files(ticker_head_list='butterfly')
except Exception:
    log.error('generate_and_update_futures_data_files failed', exc_info=True)
    quit()

try:
    log.info('generate_portfolio_pnl_report...')
    rpf.generate_portfolio_pnl_report(as_of_date=report_date, broker='ib', con=con)
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='pnl', folder_date=report_date)
except Exception:
    log.error('generate_portfolio_pnl_report', exc_info=True)
    quit()

try:
    log.info('generate_ocs_followup_report...')
    sff.generate_ocs_followup_report(as_of_date=report_date, con=con, broker='ib')
    prep.move_from_dated_folder_2daily_folder(ext='ta', file_name='followup', folder_date=report_date)
except Exception:
    log.error('generate_ocs_followup_report', exc_info=True)
    quit()

try:
    log.info('generate_spread_carry_sheet_4date...')
    sc.generate_spread_carry_sheet_4date(report_date=report_date)
except Exception:
    log.error('generate_spread_carry_sheet_4date failed', exc_info=True)
    quit()

try:
    log.info('generate_overnight_spreads_sheet_4date...')
    ocs.generate_overnight_spreads_sheet_4date(date_to=report_date)
except Exception:
    log.error('generate_overnight_spreads_sheet_4date failed', exc_info=True)
    quit()

try:
    log.info('generate_ocs_formatted_output...')
    fsf.generate_ocs_formatted_output(report_date=report_date)
except Exception:
    log.error('generate_ocs_formatted_output failed', exc_info=True)
    quit()

con.close()


