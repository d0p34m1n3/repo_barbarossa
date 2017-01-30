
import contract_utilities.contract_lists as cl
import my_sql_routines.my_sql_utilities as msu
import signals.ics as ics
import ta.strategy as ts
import pandas as pd
import os.path


def get_spreads_4date(**kwargs):

    spread_frame = cl.get_liquid_spread_frame(settle_date=kwargs['date_to'])
    spread_frame.sort(['ticker_head','Volume'], ascending=[True, False],inplace=True)
    spread_frame = spread_frame[spread_frame['Volume']>1000]
    #spread_frame.drop_duplicates('ticker_head',inplace=True)
    return spread_frame.reset_index(drop=True)


def generate_ics_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='ics', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        intraday_spreads = pd.read_pickle(output_dir + '/summary.pkl')
        return {'intraday_spreads': intraday_spreads,'success': True}

    con = msu.get_my_sql_connection(**kwargs)

    intraday_spreads = get_spreads_4date(**kwargs)

    num_spreads = len(intraday_spreads.index)

    signals_output = [ics.get_ics_signals(ticker=intraday_spreads['ticker'].iloc[x], con=con,
                                    date_to=date_to) for x in range(num_spreads)]

    intraday_spreads['downside'] = [x['downside'] for x in signals_output]
    intraday_spreads['upside'] = [x['upside'] for x in signals_output]
    intraday_spreads['front_tr_dte'] = [x['front_tr_dte'] for x in signals_output]

    intraday_spreads['intraday_mean5'] = [x['intraday_mean5'] for x in signals_output]
    intraday_spreads['intraday_mean2'] = [x['intraday_mean2'] for x in signals_output]
    intraday_spreads['intraday_mean1'] = [x['intraday_mean1'] for x in signals_output]

    intraday_spreads['intraday_std5'] = [x['intraday_std5'] for x in signals_output]
    intraday_spreads['intraday_std2'] = [x['intraday_std2'] for x in signals_output]
    intraday_spreads['intraday_std1'] = [x['intraday_std1'] for x in signals_output]

    intraday_spreads.to_pickle(output_dir + '/summary.pkl')

    if 'con' not in kwargs.keys():
        con.close()

    return {'intraday_spreads': intraday_spreads, 'success': True}





