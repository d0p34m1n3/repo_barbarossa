
import signals.option_signals as ops
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
import os.path
import ta.strategy as ts


def get_vcs_pairs_4date(**kwargs):

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
    option_frame = option_frame[selection_indx]

    option_frame = option_frame[option_frame['tr_dte'] >= 35]
    option_frame.reset_index(drop=True,inplace=True)

    unique_ticker_heads = option_frame['ticker_head'].unique()
    tuples = []

    for ticker_head_i in unique_ticker_heads:

        ticker_head_data = option_frame[option_frame['ticker_head'] == ticker_head_i]
        ticker_head_data.sort(['ticker_year', 'ticker_month'], ascending=[True, True], inplace=True)

        if len(ticker_head_data.index)>=2:
            tuples = tuples + [(ticker_head_data.index[i], ticker_head_data.index[i+1]) for i in range(len(ticker_head_data.index)-1)]


    return pd.DataFrame([(option_frame['ticker'][indx[0]],
                          option_frame['ticker'][indx[1]],
                          option_frame['ticker_head'][indx[0]],
                          option_frame['ticker_class'][indx[0]],
                          option_frame['tr_dte'][indx[0]],
                          option_frame['tr_dte'][indx[1]]) for indx in tuples],columns=['ticker1','ticker2','tickerHead','tickerClass','trDte1','trDte2'])


def generate_vcs_sheet_4date(**kwargs):

    kwargs['settle_date'] = kwargs['date_to']
    num_cal_days_back = 20*365

    output_dir = ts.create_strategy_output_dir(strategy_class='vcs', report_date=kwargs['date_to'])

    if os.path.isfile(output_dir + '/summary.pkl'):
        vcs_pairs = pd.read_pickle(output_dir + '/summary.pkl')
        return {'vcs_pairs': vcs_pairs,'success': True}

    vcs_pairs = get_vcs_pairs_4date(**kwargs)

    num_pairs = len(vcs_pairs.index)

    unique_ticker_heads = list(set(vcs_pairs['tickerHead']))

    con = msu.get_my_sql_connection(**kwargs)
    option_ticker_indicator_dictionary = {x: ops.get_option_ticker_indicators(ticker_head=x,
                                                                              settle_date_to=kwargs['date_to'],
                                                                              num_cal_days_back=num_cal_days_back,
                                                                              con=con) for x in unique_ticker_heads}

    if 'con' not in kwargs.keys():
            con.close()

    q_list = [None]*num_pairs
    qf_list = [None]*num_pairs
    fwd_vol_q_list = [None]*num_pairs
    downside_list = [None]*num_pairs
    upside_list = [None]*num_pairs
    atm_vol_ratio_list = [None]*num_pairs
    real_vol_ratio_list = [None]*num_pairs
    atm_real_vol_ratio_list = [None]*num_pairs
    theta_list = [None]*num_pairs
    fwd_vol = [None]*num_pairs

    for i in range(num_pairs):

        vcs_output = ops.get_vcs_signals(ticker_list=[vcs_pairs['ticker1'].iloc[i], vcs_pairs['ticker2'].iloc[i]],
                            option_ticker_indicator_dictionary=option_ticker_indicator_dictionary,
                            settle_date=kwargs['date_to'])

        q_list[i] = vcs_output['q']
        qf_list[i] = vcs_output['qf']
        fwd_vol_q_list[i] = vcs_output['fwd_vol_q']
        downside_list[i] = vcs_output['downside']
        upside_list[i] = vcs_output['upside']
        atm_vol_ratio_list[i] = vcs_output['atm_vol_ratio']
        fwd_vol[i] = vcs_output['fwd_vol']
        real_vol_ratio_list[i] = vcs_output['real_vol_ratio']
        atm_real_vol_ratio_list[i] = vcs_output['atm_real_vol_ratio']
        theta_list[i] = vcs_output['theta']

    vcs_pairs['Q'] = q_list
    vcs_pairs['QF'] = qf_list
    vcs_pairs['fwdVolQ'] = fwd_vol_q_list
    vcs_pairs['downside'] = downside_list
    vcs_pairs['upside'] = upside_list
    vcs_pairs['atmVolRatio'] = atm_vol_ratio_list
    vcs_pairs['fwdVol'] = fwd_vol
    vcs_pairs['realVolRatio'] = real_vol_ratio_list
    vcs_pairs['atmRealVolRatio'] = atm_real_vol_ratio_list
    vcs_pairs['theta'] = theta_list

    vcs_pairs['downside'] = vcs_pairs['downside'].round(3)
    vcs_pairs['upside'] = vcs_pairs['upside'].round(3)
    vcs_pairs['atmVolRatio'] = vcs_pairs['atmVolRatio'].round(3)
    vcs_pairs['fwdVol'] = vcs_pairs['fwdVol'].round(3)
    vcs_pairs['realVolRatio'] = vcs_pairs['realVolRatio'].round(3)
    vcs_pairs['atmRealVolRatio'] = vcs_pairs['atmRealVolRatio'].round(3)
    vcs_pairs['theta'] = vcs_pairs['theta'].round(3)

    vcs_pairs.to_pickle(output_dir + '/summary.pkl')

    return {'vcs_pairs': vcs_pairs,'success': True}




