
import QuantLib as ql
import shared.calendar_utilities as cu
import math as m

day_count_obj = ql.Actual365Fixed()
calendar_obj = ql.UnitedStates()


def get_option_greeks(**kwargs):

    # risk_free_rate and volatility in whole points not percentage points
    # for example enter 0.02 for 2 percent interest rate
    # enter 0.25 for 25 percent annual volatility

    underlying = kwargs['underlying']
    strike = kwargs['strike']
    risk_free_rate = kwargs['risk_free_rate']
    expiration_date = kwargs['expiration_date']
    calculation_date = kwargs['calculation_date']
    option_type = kwargs['option_type'].upper()
    exercise_type = kwargs['exercise_type'].upper()
    dividend_rate = 0

    if 'implied_vol' in kwargs.keys():
        implied_vol = kwargs['implied_vol']
    else:
        implied_vol = 0.15

    if option_type == 'C':
        option_type_obj = ql.Option.Call
    elif option_type == 'P':
        option_type_obj = ql.Option.Put

    expiration_datetime = cu.convert_doubledate_2datetime(expiration_date)
    calculation_datetime = cu.convert_doubledate_2datetime(calculation_date)

    expiration_date_obj = ql.Date(expiration_datetime.day, expiration_datetime.month, expiration_datetime.year)
    calculation_date_obj = ql.Date(calculation_datetime.day, calculation_datetime.month, calculation_datetime.year)

    ql.Settings.instance().evaluationDate = calculation_date_obj

    if exercise_type == 'E':
        exercise_obj = ql.EuropeanExercise(expiration_date_obj)
    elif exercise_type == 'A':
        exercise_obj = ql.AmericanExercise(calculation_date_obj, expiration_date_obj)

    underlying_obj = ql.QuoteHandle(ql.SimpleQuote(underlying/m.exp(day_count_obj.yearFraction(calculation_date_obj,expiration_date_obj)*risk_free_rate)))

    flat_ts_obj = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date_obj, risk_free_rate, day_count_obj))

    dividend_yield_obj = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date_obj, dividend_rate, day_count_obj))
    flat_vol_ts_obj = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date_obj, calendar_obj, implied_vol, day_count_obj))

    bsm_process = ql.BlackScholesMertonProcess(underlying_obj, dividend_yield_obj, flat_ts_obj, flat_vol_ts_obj)
    #bsm_process = ql.BlackProcess(underlying_obj, flat_ts_obj, flat_vol_ts_obj)

    payoff = ql.PlainVanillaPayoff(option_type_obj, strike)
    option_obj = ql.VanillaOption(payoff, exercise_obj)
    option_obj.setPricingEngine(ql.BaroneAdesiWhaleyEngine(bsm_process))

    #time_steps = 100
    #grid_points = 100
    #option_obj.setPricingEngine(ql.FDAmericanEngine(bsm_process,time_steps,grid_points))

    if 'option_price' in kwargs.keys():
        implied_vol = option_obj.impliedVolatility(kwargs['option_price'], bsm_process)
        flat_vol_ts_obj = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date_obj, calendar_obj, implied_vol, day_count_obj))
        bsm_process = ql.BlackScholesMertonProcess(underlying_obj, dividend_yield_obj, flat_ts_obj, flat_vol_ts_obj)
        #bsm_process = ql.BlackProcess(underlying_obj, flat_ts_obj, flat_vol_ts_obj)
        option_obj.setPricingEngine(ql.BaroneAdesiWhaleyEngine(bsm_process))
        #option_obj.setPricingEngine(ql.JuQuadraticApproximationEngine(bsm_process))

        #time_steps = 1000
        #grid_points = 1000
        #option_obj.setPricingEngine(ql.FDAmericanEngine(bsm_process,time_steps,grid_points))


    return {'option_price': option_obj.NPV(),
            'implied_vol': implied_vol,
            'delta': option_obj.delta(),
            'vega': option_obj.vega(),
            'theta': option_obj.thetaPerDay(),
            'gamma': option_obj.gamma()}








