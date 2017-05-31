
import opportunity_constructs.intraday_future_spreads as ifs
import signals.overnight_future_spread_signals as ofss
import ta.strategy as ts
import pandas as pd
import os.path


def generate_overnight_future_spreads_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='ofs', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        overnight_spreads = pd.read_pickle(output_dir + '/summary.pkl')
        return {'overnight_spreads': overnight_spreads,'success': True}

    overnight_spreads = ifs.get_spreads_4date(date_to=date_to)

    signals_output = [ofss.get_ofs_signals(ticker_list=[overnight_spreads['contract1'].iloc[x],
                                       overnight_spreads['contract2'].iloc[x],
                                       overnight_spreads['contract3'].iloc[x]],date_to=date_to) for x in range(len(overnight_spreads.index))]

    overnight_spreads['regress_forecast1Instant1'] = [x['regress_forecast1Instant1'] for x in signals_output]
    overnight_spreads['regress_forecast1Instant2'] = [x['regress_forecast1Instant2'] for x in signals_output]
    overnight_spreads['regress_forecast1Instant3'] = [x['regress_forecast1Instant3'] for x in signals_output]
    overnight_spreads['regress_forecast1Instant4'] = [x['regress_forecast1Instant4'] for x in signals_output]

    overnight_spreads['regress_forecast11'] = [x['regress_forecast11'] for x in signals_output]
    overnight_spreads['regress_forecast12'] = [x['regress_forecast12'] for x in signals_output]
    overnight_spreads['regress_forecast13'] = [x['regress_forecast13'] for x in signals_output]
    overnight_spreads['regress_forecast14'] = [x['regress_forecast14'] for x in signals_output]

    overnight_spreads['regress_forecast51'] = [x['regress_forecast51'] for x in signals_output]
    overnight_spreads['regress_forecast52'] = [x['regress_forecast52'] for x in signals_output]
    overnight_spreads['regress_forecast53'] = [x['regress_forecast53'] for x in signals_output]
    overnight_spreads['regress_forecast54'] = [x['regress_forecast54'] for x in signals_output]

    overnight_spreads['regress_forecast101'] = [x['regress_forecast101'] for x in signals_output]
    overnight_spreads['regress_forecast102'] = [x['regress_forecast102'] for x in signals_output]
    overnight_spreads['regress_forecast103'] = [x['regress_forecast103'] for x in signals_output]

    overnight_spreads['change1_instantNormalized'] = [x['change1_instantNormalized'] for x in signals_output]
    overnight_spreads['change1Normalized'] = [x['change1Normalized'] for x in signals_output]
    overnight_spreads['change5Normalized'] = [x['change5Normalized'] for x in signals_output]
    overnight_spreads['change10Normalized'] = [x['change10Normalized'] for x in signals_output]

    overnight_spreads['change_1Normalized'] = [x['change_1Normalized'] for x in signals_output]
    overnight_spreads['change_2Delta'] = [x['change_2Delta'] for x in signals_output]
    overnight_spreads['change_5Normalized'] = [x['change_5Normalized'] for x in signals_output]
    overnight_spreads['change_10Normalized'] = [x['change_10Normalized'] for x in signals_output]
    overnight_spreads['change_20Normalized'] = [x['change_20Normalized'] for x in signals_output]
    overnight_spreads['change_40Normalized'] = [x['change_40Normalized'] for x in signals_output]

    overnight_spreads.to_pickle(output_dir + '/summary.pkl')

    return {'overnight_spreads': overnight_spreads, 'success': True}







