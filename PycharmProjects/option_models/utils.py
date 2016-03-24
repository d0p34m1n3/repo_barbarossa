

import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import interest_curve.get_rate_from_stir as grfs
import option_models.quantlib_option_models as qom
import contract_utilities.contract_meta_info as cmi
import numpy as np


def option_model_wrapper(**kwargs):

    # enter underlying, strike, (option_price or implied_vol)
    # calculation_date, exercise_type, option_type

    con = msu.get_my_sql_connection(**kwargs)
    ticker = kwargs['ticker']
    calculation_date = kwargs['calculation_date']

    #print(ticker)
    #print(kwargs['exercise_type'])

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    expiration_datetime = exp.get_expiration_from_db(ticker=ticker, instrument='options', con=con)
    expiration_date = int(expiration_datetime.strftime('%Y%m%d'))

    if 'con' not in kwargs.keys():
        con.close()

    interest_rate = grfs.get_simple_rate(as_of_date=calculation_date, date_to=expiration_date)['rate_output']

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







