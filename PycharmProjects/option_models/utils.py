

import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import interest_curve.get_rate_from_stir as grfs
import option_models.quantlib_option_models as qom


def option_model_wrapper(**kwargs):

    # enter underlying, strike, (option_price or implied_vol)
    # calculation_date, exercise_type, option_type

    con = msu.get_my_sql_connection(**kwargs)
    ticker = kwargs['ticker']
    calculation_date = kwargs['calculation_date']

    expiration_datetime = exp.get_expiration_from_db(ticker=ticker, instrument='options', con=con)
    expiration_date = int(expiration_datetime.strftime('%Y%m%d'))

    interest_rate = grfs.get_simple_rate(as_of_date=calculation_date, date_to=expiration_date)['rate_output']

    print(interest_rate)

    option_greeks = qom.get_option_greeks(risk_free_rate=interest_rate, expiration_date=expiration_date,**kwargs)

    if 'con' not in kwargs.keys():
        con.close()

    return option_greeks



