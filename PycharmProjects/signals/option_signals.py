
import my_sql_routines.my_sql_utilities as msu
import get_price.get_options_price as gop
from scipy.interpolate import interp1d
import numpy as np
import pandas as pd
import shared.utils as sut
import contract_utilities.contract_meta_info as cmi


def get_vcs_signals(**kwargs):

    ticker1 = kwargs['ticker1']
    ticker2 = kwargs['ticker2']
    settle_date = kwargs['settle_date']

    con = msu.get_my_sql_connection(**kwargs)

    indicator1_output = get_option_ticker_indicators(ticker=ticker1, settle_date=settle_date, con=con)
    indicator2_output = get_option_ticker_indicators(ticker=ticker2, settle_date=settle_date, con=con)

    if 'con' not in kwargs.keys():
        con.close()

    vol1 = float(indicator1_output['atm_vol'][0])
    vol2 = float(indicator2_output['atm_vol'][0])

    tr_dte1 = indicator1_output['tr_dte'][0]
    tr_dte2 = indicator2_output['tr_dte'][0]

    tickerhead1 = indicator1_output['ticker_head'][0]
    tickerhead2 = indicator2_output['ticker_head'][0]

    ticker_class = cmi.ticker_class[tickerhead1]

    if ticker_class in ['Livestock', 'Ag'] or tickerhead1 == 'NG':
        month_specificQ = True
        tickermonth1 = cmi.full_letter_month_list[indicator1_output['ticker_month'][0]-1]
        tickermonth2 = cmi.full_letter_month_list[indicator2_output['ticker_month'][0]-1]
    else:
        month_specificQ = False

    if tickerhead1 in ['ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
        model = 'OU'
    else:
        model = 'BS'

    max_tr_dte = max([tr_dte1, tr_dte2])
    min_tr_dte = min([tr_dte1, tr_dte2])

    max_ref_dte = sut.get_closest(list_input=cmi.aligned_data_tr_dte_list, target_value=max_tr_dte)

    min_ref_dte = sut.get_closest(list_input=cmi.aligned_data_tr_dte_list,
                                  target_value=min_tr_dte+max_ref_dte-max_tr_dte)

    if tr_dte1 >= tr_dte2:
        tr_dte1_ref = max_ref_dte
        tr_dte2_ref = min_ref_dte
    else:
        tr_dte1_ref = min_ref_dte
        tr_dte2_ref = max_ref_dte

    if month_specificQ:

        aligned_data1 = gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[tickerhead1],
                                                    tr_dte_center=tr_dte1_ref,
                                                    contract_month_letter=tickermonth1,
                                                    model=model)
        aligned_data2 = gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[tickerhead2],
                                                    tr_dte_center=tr_dte2_ref,
                                                    contract_month_letter=tickermonth2,
                                                    model=model)
    else:

        aligned_data1 = gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[tickerhead1],
                                                           tr_dte_center=tr_dte1_ref, model=model)
        aligned_data2 = gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[tickerhead2],
                                                           tr_dte_center=tr_dte2_ref, model=model)

    hist = pd.merge(aligned_data1, aligned_data2, how='inner', on='settleDates', suffixes=['1', '2'])

    hist['imp_vol_ratio'] = hist['impVol1']/hist['impVol2']

    current = {'imp_vol_ratio': vol1/vol2}

    return {'hist': hist, 'current': current}





def calc_delta_vol_4ticker(**kwargs):

    delta_target = kwargs['delta_target']
    delta_max_deviation = 0.15

    #print(kwargs['ticker'])

    skew_output = gop.get_options_price_from_db(column_names=['delta', 'imp_vol', 'cal_dte', 'tr_dte'], **kwargs)

    if skew_output.empty:
        tr_dte = np.NaN
        cal_dte = np.NaN
    else:
        tr_dte = skew_output['tr_dte'][0]
        cal_dte = skew_output['cal_dte'][0]

    output_dict = {'delta_vol': np.NaN, 'cal_dte': cal_dte, 'tr_dte': tr_dte}

    skew_output = skew_output[(skew_output['imp_vol'].notnull())]

    delta_band = [delta_target*x for x in [1-delta_max_deviation, 1+delta_max_deviation]]

    skew_output = skew_output[(skew_output['delta'] <= max(delta_band)) & (skew_output['delta'] >= min(delta_band))]

    if skew_output.empty:
        return output_dict

    skew_output['delta'] = skew_output['delta'].astype('float64')
    skew_output['imp_vol'] = skew_output['imp_vol'].astype('float64')

    output_dict['delta_vol'] = skew_output['imp_vol'].iloc[0]

    return output_dict


def get_option_ticker_indicators(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'ticker' in kwargs.keys():
        filter_string = 'WHERE ticker=\'' + kwargs['ticker'] + '\''
    elif 'ticker_head' in kwargs.keys():
        filter_string = 'WHERE ticker_head=\'' + kwargs['ticker_head'] + '\''
    else:
        filter_string = ''

    if 'settle_date' in kwargs.keys():
        filter_string = filter_string + ' and price_date=' + str(kwargs['settle_date'])

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = ['ticker', 'ticker_head', 'ticker_month', 'cal_dte', 'tr_dte', 'atm_vol']

    sql_query = 'SELECT ' + ",".join(column_names) + ' FROM option_ticker_indicators ' + filter_string

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return pd.DataFrame(data, columns=column_names)






