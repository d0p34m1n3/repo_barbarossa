
import signals.option_signals as ops
import my_sql_routines.my_sql_utilities as msu
import signals.scv_signals as scvs
import contract_utilities.contract_meta_info as cmi
import ta.strategy as ts
import get_price.get_futures_price as gfp
import opportunity_constructs.vcs as vcs
import pandas as pd
import os.path


def get_single_contracts_4date(**kwargs):

    option_frame = ops.get_option_ticker_indicators(**kwargs)

    if 'open_interest_filter' in kwargs.keys():
        open_interest_filter = kwargs['open_interest_filter']
    else:
        open_interest_filter = 100

    option_frame = option_frame[option_frame['open_interest']>=open_interest_filter]
    option_frame['ticker_class'] = [cmi.ticker_class[x] for x in option_frame['ticker_head']]

    selection_indx = (option_frame['ticker_class'] == 'Livestock') | (option_frame['ticker_class'] == 'Ag') | \
                     (option_frame['ticker_class'] == 'Treasury') | (option_frame['ticker_head'] == 'CL') | \
                     (option_frame['ticker_class'] == 'FX') | (option_frame['ticker_class'] == 'Index') | \
                     (option_frame['ticker_class'] == 'Metal')

    #selection_indx = (option_frame['ticker_head'] == 'S')

    option_frame = option_frame[selection_indx]

    option_frame = option_frame[option_frame['tr_dte'] >= 20]
    option_frame.reset_index(drop=True,inplace=True)

    return option_frame[['ticker','ticker_head','ticker_class','tr_dte']]


def generate_scv_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='scv', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        scv_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'scv_frame': scv_frame, 'success': True}

    con = msu.get_my_sql_connection(**kwargs)

    scv_frame = get_single_contracts_4date(settle_date=date_to, con=con)

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in cmi.cme_futures_tickerhead_list}

    num_tickers = len(scv_frame.index)

    q_list = [None]*num_tickers
    downside_list = [None]*num_tickers
    upside_list = [None]*num_tickers
    theta_list = [None]*num_tickers
    realized_vol_forecast_list = [None]*num_tickers
    realized_vol20_list = [None]*num_tickers
    imp_vol_list = [None]*num_tickers
    imp_vol_premium_list = [None]*num_tickers

    for i in range(num_tickers):

        #print(scv_frame['ticker'].iloc[i])

        scv_output = scvs.get_scv_signals(ticker=scv_frame['ticker'].iloc[i], date_to=date_to,
                                          con=con, futures_data_dictionary=futures_data_dictionary)

        q_list[i] = scv_output['q']
        downside_list[i] = scv_output['downside']
        upside_list[i] = scv_output['upside']
        theta_list[i] = scv_output['theta']
        imp_vol_premium_list[i] = scv_output['imp_vol_premium']
        imp_vol_list[i] = scv_output['imp_vol']
        realized_vol_forecast_list[i] = scv_output['realized_vol_forecast']
        realized_vol20_list[i] = scv_output['real_vol20_current']

    scv_frame['Q'] = q_list
    scv_frame['premium'] = imp_vol_premium_list
    scv_frame['impVol'] = imp_vol_list
    scv_frame['forecast'] = realized_vol_forecast_list
    scv_frame['realVol20'] = realized_vol20_list
    scv_frame['downside'] = downside_list
    scv_frame['upside'] = upside_list
    scv_frame['theta'] = theta_list

    scv_frame['downside'] = scv_frame['downside'].round(2)
    scv_frame['upside'] = scv_frame['upside'].round(2)
    scv_frame['theta'] = scv_frame['theta'].round(2)
    scv_frame['premium'] = scv_frame['premium'].round(1)
    scv_frame['forecast'] = scv_frame['forecast'].round(2)
    scv_frame['realVol20'] = scv_frame['realVol20'].round(2)
    scv_frame['impVol'] = scv_frame['impVol'].round(2)

    scv_frame.to_pickle(output_dir + '/summary.pkl')

    return {'scv_frame': scv_frame, 'success': True}





