
import my_sql_routines.my_sql_utilities as msu
import get_price.get_options_price as gop
import get_price.get_futures_price as gfp
import option_models.quantlib_option_models as qom
import interest_curve.get_rate_from_stir as grfs
import option_models.utils as oput
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi


def cal_greeks_4option_maturity(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    option_prices = gop.get_options_price_from_db(**kwargs)

    contract_specs_out = cmi.get_contract_specs(kwargs['ticker'])
    exercise_type = cmi.get_option_exercise_type(**contract_specs_out)

    underlying_ticker = oput.get_option_underlying(**kwargs)

    underlying_price = gfp.get_futures_price_preloaded(ticker=underlying_ticker,settle_date=kwargs['settle_date'])['close_price'].iloc[0]

    expiration_datetime = exp.get_expiration_from_db(instrument='options', **kwargs)
    expiration_date = int(expiration_datetime.strftime('%Y%m%d'))

    interest_rate = grfs.get_simple_rate(as_of_date=kwargs['settle_date'], date_to=expiration_date)['rate_output']

    option_greeks = [qom.get_option_greeks(underlying=underlying_price,
                                           option_price=float(option_prices['close_price'].iloc[x]),
                                          strike=float(option_prices['strike'].iloc[x]),
                                          risk_free_rate=interest_rate,
                                          expiration_date=expiration_date,
                                          calculation_date=kwargs['settle_date'],
                                          option_type=option_prices['option_type'].iloc[x],
                                          exercise_type=exercise_type) for x in range(len(option_prices.index))]

    if 'con' not in kwargs.keys():
        con.close()

    return  option_greeks








