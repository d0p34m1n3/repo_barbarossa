

import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import interest_curve.get_rate_from_stir as grfs
import option_models.quantlib_option_models as qom
import get_price.get_options_price as gop
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import math as m


def option_model_wrapper(**kwargs):

    # enter underlying, strike, (option_price or implied_vol)
    # calculation_date, exercise_type, option_type

    con = msu.get_my_sql_connection(**kwargs)
    ticker = kwargs['ticker']
    calculation_date = kwargs['calculation_date']

    if 'interest_rate_date' in kwargs.keys():
        interest_rate_date = kwargs['interest_rate_date']
    else:
        interest_rate_date = calculation_date

    #print(ticker)
    #print(kwargs['exercise_type'])

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    expiration_datetime = exp.get_expiration_from_db(ticker=ticker, instrument='options', con=con)
    expiration_date = int(expiration_datetime.strftime('%Y%m%d'))

    if 'con' not in kwargs.keys():
        con.close()

    interest_rate = grfs.get_simple_rate(as_of_date=interest_rate_date, date_to=expiration_date)['rate_output']

    if np.isnan(interest_rate):
        option_greeks = {'implied_vol': np.NaN, 'delta': np.NaN, 'vega': np.NaN, 'dollar_vega': np.NaN,
                         'theta': np.NaN, 'dollar_theta': np.NaN,
                         'gamma': np.NaN, 'dollar_gamma': np.NaN,
                         'interest_rate': np.NaN, 'cal_dte': np.NaN}
    else:
        option_greeks = qom.get_option_greeks(risk_free_rate=interest_rate, expiration_date=expiration_date, **kwargs)
        option_greeks['implied_vol'] = 100*option_greeks['implied_vol']
        option_greeks['dollar_vega'] = option_greeks['vega']*contract_multiplier/100
        option_greeks['dollar_theta'] = option_greeks['theta']*contract_multiplier
        option_greeks['dollar_gamma'] = option_greeks['gamma']*contract_multiplier
        option_greeks['interest_rate'] = interest_rate

    return option_greeks


def get_option_underlying(**kwargs):

    ticker = kwargs['ticker']

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']
    ticker_month_num = contract_specs_output['ticker_month_num']
    ticker_year = contract_specs_output['ticker_year']

    if ticker_head == 'E0':
        ticker_head = 'ED'
        ticker_year = ticker_year + 1
    elif ticker_head == 'E2':
        ticker_head = 'ED'
        ticker_year = ticker_year + 2
    elif ticker_head == 'E3':
        ticker_head = 'ED'
        ticker_year = ticker_year + 3
    elif ticker_head == 'E4':
        ticker_head = 'ED'
        ticker_year = ticker_year + 4
    elif ticker_head == 'E5':
        ticker_head = 'ED'
        ticker_year = ticker_year + 5

    futures_contract_months = cmi.futures_contract_months[ticker_head]

    futures_contract_month_numbers = [cmi.letter_month_string.find(x)+1 for x in futures_contract_months]

    leading_months = [x for x in futures_contract_month_numbers if x>=ticker_month_num]

    if len(leading_months)>0:
        underlying_month_num = leading_months[0]
        underlying_month_year = ticker_year
    else:
        underlying_month_num = futures_contract_month_numbers[0]
        underlying_month_year = ticker_year+1

    return ticker_head + cmi.letter_month_string[underlying_month_num-1] + str(underlying_month_year)

def get_strike_4current_delta(**kwargs):

    ticker = kwargs['ticker']
    settle_date = kwargs['settle_date']
    underlying_current_price = kwargs['underlying_current_price']

    if m.isnan(underlying_current_price):
        return np.nan

    underlying_ticker = get_option_underlying(ticker=ticker)
    contract_specs_output = cmi.get_contract_specs(underlying_ticker)
    ticker_head = contract_specs_output['ticker_head']

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [ticker_head]}

    if 'call_delta_target' in kwargs.keys():
        call_delta_target = kwargs['call_delta_target']
    else:
        call_delta_target = 0.5

    con = msu.get_my_sql_connection(**kwargs)

    option_data = gop.get_options_price_from_db(ticker=ticker, settle_date=settle_date,
                                                column_names=['id', 'option_type', 'strike', 'cal_dte', 'tr_dte',
                                                              'close_price', 'volume', 'open_interest', 'delta'])

    underlying_settle_price = gfp.get_futures_price_preloaded(ticker=underlying_ticker, settle_date=settle_date,
                                                              futures_data_dictionary=futures_data_dictionary)['close_price'].iloc[0]

    call_data = option_data[option_data['option_type'] == 'C']

    call_data['delta_abs_centered'] = abs(call_data['delta'] - call_delta_target)

    call_data.sort_values('delta_abs_centered', ascending=True, inplace=True)

    strike_at_settle = (call_data['strike'].iloc[0] * call_data['delta_abs_centered'].iloc[1] + call_data['strike'].iloc[1]*call_data['delta_abs_centered'].iloc[0])/\
                       (call_data['delta_abs_centered'].iloc[0] + call_data['delta_abs_centered'].iloc[1])

    strike_offset = strike_at_settle - underlying_settle_price
    strike_current_approximate = underlying_current_price + strike_offset

    call_data['strike_diff'] = abs(call_data['strike'] - strike_current_approximate)
    call_data.sort_values('strike_diff', ascending=True, inplace=True)
    strike_current = call_data['strike'].iloc[0]

    if 'con' not in kwargs.keys():
        con.close()

    return strike_current











