
import signals.option_signals as ops
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
import os.path
import ta.strategy as ts
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp


def get_vcs_pairs_4date(**kwargs):

    option_frame = ops.get_option_ticker_indicators(**kwargs)

    if 'open_interest_filter' in kwargs.keys():
        open_interest_filter = kwargs['open_interest_filter']
    else:
        open_interest_filter = 100

    option_frame = option_frame[option_frame['open_interest']>=open_interest_filter]
    option_frame['ticker_class'] = [cmi.ticker_class[x] for x in option_frame['ticker_head']]

    selection_indx = (option_frame['ticker_class'] == 'Livestock') | (option_frame['ticker_class'] == 'Ag') | \
                     (option_frame['ticker_class'] == 'Treasury') | (option_frame['ticker_class'] == 'Energy') | \
                     (option_frame['ticker_class'] == 'FX') | (option_frame['ticker_class'] == 'Index') | \
                     (option_frame['ticker_class'] == 'Metal')
    option_frame = option_frame[selection_indx]

    option_frame = option_frame[option_frame['tr_dte'] >= 35]
    option_frame.reset_index(drop=True,inplace=True)

    unique_ticker_heads = option_frame['ticker_head'].unique()
    tuples = []

    for ticker_head_i in unique_ticker_heads:

        ticker_head_data = option_frame[option_frame['ticker_head'] == ticker_head_i]
        ticker_head_data.sort_values(['ticker_year', 'ticker_month'], ascending=[True, True], inplace=True)

        if len(ticker_head_data.index) >= 2:
            for i in range(len(ticker_head_data.index)-1):
                for j in range(i+1,len(ticker_head_data.index)):
                    tuples = tuples + [(ticker_head_data.index[i], ticker_head_data.index[j])]

    return pd.DataFrame([(option_frame['ticker'][indx[0]],
                          option_frame['ticker'][indx[1]],
                          option_frame['ticker_head'][indx[0]],
                          option_frame['ticker_class'][indx[0]],
                          option_frame['tr_dte'][indx[0]],
                          option_frame['tr_dte'][indx[1]]) for indx in tuples],columns=['ticker1','ticker2','tickerHead','tickerClass','trDte1','trDte2'])


def get_vcs_pairs_4date_legacy(**kwargs):

    settle_date = kwargs['settle_date']
    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    con = msu.get_my_sql_connection(**kwargs)

    liquid_options_frame = cl.generate_liquid_options_list_dataframe(settle_date=settle_date,con=con)
    contract_specs_output = [cmi.get_contract_specs(x) for x in liquid_options_frame['ticker']]
    liquid_options_frame['ticker_head'] = [x['ticker_head'] for x in contract_specs_output]
    liquid_options_frame['ticker_month'] = [x['ticker_month_num'] for x in contract_specs_output]
    liquid_options_frame['ticker_class'] = [x['ticker_class'] for x in contract_specs_output]
    liquid_options_frame['cal_dte'] = [(x-settle_datetime.date()).days for x in liquid_options_frame['expiration_date']]

    liquid_options_frame = liquid_options_frame[((liquid_options_frame['ticker_head'] == 'LN')&(liquid_options_frame['cal_dte'] <= 360))|
                                                ((liquid_options_frame['ticker_head']=='LC')&(liquid_options_frame['cal_dte']<=360)&
                                                 (liquid_options_frame['ticker_month']%2==0))|
                                                ((liquid_options_frame['ticker_head']=='LC')&(liquid_options_frame['cal_dte']<=40)&
                                                 (liquid_options_frame['ticker_month']%2==1))|
                                                ((liquid_options_frame['ticker_head']=='ES')&(liquid_options_frame['cal_dte']<=270))|
                                                ((liquid_options_frame['ticker_class']=='FX')&(liquid_options_frame['cal_dte']<=270)&
                                                 (liquid_options_frame['ticker_month']%3==0))|
                                                ((liquid_options_frame['ticker_class']=='FX')&(liquid_options_frame['cal_dte']<=70)&
                     (liquid_options_frame['ticker_month']%3!=0))|
                     ((liquid_options_frame['ticker_head']=='GC')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month'].isin([6,12])))|
                     ((liquid_options_frame['ticker_head']=='GC')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month'].isin([2,4,8,10])))|
                     ((liquid_options_frame['ticker_head']=='GC')&(liquid_options_frame['cal_dte']<=70))|
                     ((liquid_options_frame['ticker_head']=='SI')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month'].isin([7,12])))|
                     ((liquid_options_frame['ticker_head']=='SI')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month'].isin([1,3,5,9])))|
                     ((liquid_options_frame['ticker_head']=='SI')&(liquid_options_frame['cal_dte']<=70))|
                     ((liquid_options_frame['ticker_class']=='Treasury')&(liquid_options_frame['cal_dte']<=180)&
                     (liquid_options_frame['ticker_month']%3==0))|
                     ((liquid_options_frame['ticker_class']=='Treasury')&(liquid_options_frame['cal_dte']<=70)&
                     (liquid_options_frame['ticker_month']%3!=0))|
                     ((liquid_options_frame['ticker_head']=='C')&(liquid_options_frame['cal_dte']<=540)&
                     (liquid_options_frame['ticker_month']==12))|
                     ((liquid_options_frame['ticker_head']=='C')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month']==7))|
                     ((liquid_options_frame['ticker_head']=='C')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month'].isin([3,5,9])))|
                     ((liquid_options_frame['ticker_head']=='C')&(liquid_options_frame['cal_dte']<=40))|
                     ((liquid_options_frame['ticker_head']=='S')&(liquid_options_frame['cal_dte']<=540)&
                     (liquid_options_frame['ticker_month']==11))|
                     ((liquid_options_frame['ticker_head']=='S')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month']==7))|
                     ((liquid_options_frame['ticker_head']=='S')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month'].isin([1,3,5,8,9])))|
                     ((liquid_options_frame['ticker_head']=='S')&(liquid_options_frame['cal_dte']<=40))|
                     ((liquid_options_frame['ticker_head']=='SM')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month']==12))|
                     ((liquid_options_frame['ticker_head']=='SM')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month']==7))|
                     ((liquid_options_frame['ticker_head']=='SM')&(liquid_options_frame['cal_dte']<=180)&
                     (liquid_options_frame['ticker_month'].isin([1,3,5,8,9,10])))|
                     ((liquid_options_frame['ticker_head']=='SM')&(liquid_options_frame['cal_dte']<=40))|
                     ((liquid_options_frame['ticker_head']=='BO')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month']==12))|
                     ((liquid_options_frame['ticker_head']=='BO')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month']==7))|
                     ((liquid_options_frame['ticker_head']=='BO')&(liquid_options_frame['cal_dte']<=180)&
                     (liquid_options_frame['ticker_month'].isin([1,3,5,8,9,10])))|
                     ((liquid_options_frame['ticker_head']=='BO')&(liquid_options_frame['cal_dte']<=40))|
                     ((liquid_options_frame['ticker_head']=='W')&(liquid_options_frame['cal_dte']<=360)&
                     (liquid_options_frame['ticker_month']==12))|
                     ((liquid_options_frame['ticker_head']=='W')&(liquid_options_frame['cal_dte']<=270)&
                     (liquid_options_frame['ticker_month']==7))|
                     ((liquid_options_frame['ticker_head']=='W')&(liquid_options_frame['cal_dte']<=180)&
                     (liquid_options_frame['ticker_month'].isin([3,5,9])))|
                     ((liquid_options_frame['ticker_head']=='W')&(liquid_options_frame['cal_dte']<=40))|
                     ((liquid_options_frame['ticker_head']=='CL')&(liquid_options_frame['cal_dte']<=720)&
                     (liquid_options_frame['ticker_month']==12))|
                     ((liquid_options_frame['ticker_head']=='CL')&(liquid_options_frame['cal_dte']<=540)&
                     (liquid_options_frame['ticker_month']==6))|
                     ((liquid_options_frame['ticker_head']=='CL')&(liquid_options_frame['cal_dte']<=180))|
                     ((liquid_options_frame['ticker_head']=='NG')&(liquid_options_frame['cal_dte']<=360))]

    liquid_options_frame.sort(['ticker_head','cal_dte'],ascending=[True,True],inplace=True)

    liquid_options_frame['tr_dte'] = [exp.get_days2_expiration(date_to=settle_date,con=con,instrument='options',ticker=x)['tr_dte'] for x in liquid_options_frame['ticker']]

    if 'con' not in kwargs.keys():
            con.close()

    option_frame = liquid_options_frame[liquid_options_frame['tr_dte'] >= 35]

    option_frame.reset_index(drop=True,inplace=True)

    unique_ticker_heads = option_frame['ticker_head'].unique()
    tuples = []

    for ticker_head_i in unique_ticker_heads:

        ticker_head_data = option_frame[option_frame['ticker_head'] == ticker_head_i]
        ticker_head_data.sort('cal_dte', ascending=True, inplace=True)

        if len(ticker_head_data.index) >= 2:
            for i in range(len(ticker_head_data.index)-1):
                for j in range(i+1,len(ticker_head_data.index)):
                    tuples = tuples + [(ticker_head_data.index[i], ticker_head_data.index[j])]

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
    q1_list = [None]*num_pairs
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
        q1_list[i] = vcs_output['q1']
        fwd_vol_q_list[i] = vcs_output['fwd_vol_q']
        downside_list[i] = vcs_output['downside']
        upside_list[i] = vcs_output['upside']
        atm_vol_ratio_list[i] = vcs_output['atm_vol_ratio']
        fwd_vol[i] = vcs_output['fwd_vol']
        real_vol_ratio_list[i] = vcs_output['real_vol_ratio']
        atm_real_vol_ratio_list[i] = vcs_output['atm_real_vol_ratio']
        theta_list[i] = vcs_output['theta']

    vcs_pairs['Q'] = q_list
    vcs_pairs['Q1'] = q1_list
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


def generate_vcs_sheet_4date_legacy(**kwargs):

    kwargs['settle_date'] = kwargs['date_to']
    num_cal_days_back = 20*365

    output_dir = ts.create_strategy_output_dir(strategy_class='vcs', report_date=kwargs['date_to'])

    if os.path.isfile(output_dir + '/summary.pkl'):
        vcs_pairs = pd.read_pickle(output_dir + '/summary.pkl')
        return {'vcs_pairs': vcs_pairs,'success': True}

    vcs_pairs = get_vcs_pairs_4date_legacy(**kwargs)
    num_pairs = len(vcs_pairs.index)

    atm_vol_ratio_list = [None]*num_pairs
    q_list = [None]*num_pairs
    q2_list = [None]*num_pairs
    q1_list = [None]*num_pairs
    q5_list = [None]*num_pairs

    fwd_vol_q_list = [None]*num_pairs
    fwd_vol_q2_list = [None]*num_pairs
    fwd_vol_q1_list = [None]*num_pairs
    fwd_vol_q5_list = [None]*num_pairs

    atm_real_vol_ratio_list = [None]*num_pairs
    q_atm_real_vol_ratio_list = [None]*num_pairs

    atm_real_vol_ratio_ratio_list = [None]*num_pairs
    q_atm_real_vol_ratio_ratio_list = [None]*num_pairs

    tr_dte_diff_percent_list = [None]*num_pairs
    downside_list = [None]*num_pairs
    upside_list = [None]*num_pairs

    theta1_list = [None]*num_pairs
    theta2_list = [None]*num_pairs

    for i in range(num_pairs):

        vcs_output = ops.get_vcs_signals_legacy(ticker_list=[vcs_pairs['ticker1'].iloc[i], vcs_pairs['ticker2'].iloc[i]],
                                                tr_dte_list=[vcs_pairs['trDte1'].iloc[i],vcs_pairs['trDte2'].iloc[i]],
                                                num_cal_days_back=num_cal_days_back,settle_date=kwargs['date_to'])

        atm_vol_ratio_list[i] = vcs_output['atm_vol_ratio']

        q_list[i] = vcs_output['q']
        q2_list[i] = vcs_output['q2']
        q1_list[i] = vcs_output['q1']
        q5_list[i] = vcs_output['q5']

        fwd_vol_q_list[i] = vcs_output['fwd_vol_q']
        fwd_vol_q2_list[i] = vcs_output['fwd_vol_q2']
        fwd_vol_q1_list[i] = vcs_output['fwd_vol_q1']
        fwd_vol_q5_list[i] = vcs_output['fwd_vol_q5']

        atm_real_vol_ratio_list[i] = vcs_output['atm_real_vol_ratio']
        q_atm_real_vol_ratio_list[i] = vcs_output['q_atm_real_vol_ratio']

        atm_real_vol_ratio_ratio_list[i] = vcs_output['atm_real_vol_ratio_ratio']
        q_atm_real_vol_ratio_ratio_list[i] = vcs_output['q_atm_real_vol_ratio_ratio']

        tr_dte_diff_percent_list[i] = vcs_output['tr_dte_diff_percent']
        downside_list[i] = vcs_output['downside']
        upside_list[i] = vcs_output['upside']

        theta1_list[i] = vcs_output['theta1']
        theta2_list[i] = vcs_output['theta2']

    vcs_pairs['Q'] = q_list
    vcs_pairs['Q2'] = q2_list
    vcs_pairs['Q1'] = q1_list
    vcs_pairs['Q5'] = q5_list

    vcs_pairs['fwdVolQ'] = fwd_vol_q_list
    vcs_pairs['fwdVolQ2'] = fwd_vol_q2_list
    vcs_pairs['fwdVolQ1'] = fwd_vol_q1_list
    vcs_pairs['fwdVolQ5'] = fwd_vol_q5_list

    vcs_pairs['atmRealVolRatio'] = atm_real_vol_ratio_list
    vcs_pairs['atmRealVolRatioQ'] = q_atm_real_vol_ratio_list

    vcs_pairs['atmRealVolRatioRatio'] = atm_real_vol_ratio_ratio_list
    vcs_pairs['atmRealVolRatioRatioQ'] = q_atm_real_vol_ratio_ratio_list

    vcs_pairs['trDteDiffPercent'] = tr_dte_diff_percent_list
    vcs_pairs['downside'] = downside_list
    vcs_pairs['upside'] = upside_list

    vcs_pairs['theta1'] = theta1_list
    vcs_pairs['theta2'] = theta2_list

    vcs_pairs.to_pickle(output_dir + '/summary.pkl')

    return {'vcs_pairs': vcs_pairs, 'success': True}



















