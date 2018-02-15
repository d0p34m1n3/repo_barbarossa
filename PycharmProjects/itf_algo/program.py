
import ib_api_utils.subscription as subs
import ib_api_utils.ib_contract as ib_contract
import opportunity_constructs.overnight_calendar_spreads as ocs
import my_sql_routines.my_sql_utilities as msu
import copy as cpy
import ta.trade_fill_loader as tfl
import ta.strategy as ts
import ta.position_manager as pm
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import api_utils.portfolio as aup
import shared.utils as su
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import math as mth
import threading as thr
import datetime as dt
from ibapi.contract import *
from ibapi.common import *
from ibapi.ticktype import *
from ibapi.order import *
from ibapi.order_state import *
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.order_condition import *
import itf_algo.algo as algo

import shared.log as lg

def main():
    app = algo.Algo()
    con = msu.get_my_sql_connection()
    date_now = cu.get_doubledate()
    datetime_now = dt.datetime.now()
    report_date = exp.doubledate_shift_bus_days()

    ticker_list = ['HOJ2018','LCJ2018','FCH2018','NQH2018']
    #ticker_list = ['NQH2018']

    theme_name_list = set([x + '_long' for x in ticker_list]).union(set([x + '_short' for x in ticker_list]))

    alias_portfolio = aup.portfolio(ticker_list=theme_name_list)

    latest_trade_entry_datetime = dt.datetime(datetime_now.year, datetime_now.month, datetime_now.day, 12, 0, 0)
    latest_livestock_exit_datetime = dt.datetime(datetime_now.year, datetime_now.month, datetime_now.day, 13, 55, 0)
    latest_macro_exit_datetime = dt.datetime(datetime_now.year, datetime_now.month, datetime_now.day, 15, 55, 0)

    app.ticker_list = ticker_list
    app.tick_size_dictionary = {x:cmi.tick_size[cmi.get_contract_specs(x)['ticker_head']] for x in ticker_list}
    app.contract_multiplier_dictionary = {x:cmi.contract_multiplier[cmi.get_contract_specs(x)['ticker_head']] for x in ticker_list}
    app.ticker_head_dictionary = {x:cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list}
    app.latest_trade_entry_datetime = latest_trade_entry_datetime
    app.latest_trade_exit_datetime_dictionary = {'HO': latest_macro_exit_datetime, 'LC': latest_livestock_exit_datetime, 'FC': latest_livestock_exit_datetime, 'NQ': latest_macro_exit_datetime}
    app.alias_portfolio = alias_portfolio
    app.output_dir = ts.create_strategy_output_dir(strategy_class='ocs', report_date=report_date)
    app.log = lg.get_logger(file_identifier='ib_itf',log_level='INFO')
    app.con = con

    app.connect(client_id=4)
    app.run()

if __name__ == "__main__":
    main()

