
import my_sql_routines.my_sql_utilities as msu
import get_price.get_options_price as gop
from scipy.interpolate import interp1d
import get_price.get_futures_price as gfp
import option_models.utils as omu
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import shared.utils as sut
import opportunity_constructs.utilities as ocu
import contract_utilities.contract_meta_info as cmi
import copy as cpy
import shared.statistics as stats


def get_vcs_signals(**kwargs):

    aligned_indicators_output = get_aligned_option_indicators(**kwargs)
    hist = aligned_indicators_output['hist']
    current = aligned_indicators_output['current']

    hist['atm_vol_ratio'] = hist['c1']['atm_vol']/hist['c2']['atm_vol']
    atm_vol_ratio = current['atm_vol'][0]/current['atm_vol'][1]

    q = stats.get_quantile_from_number({'x': atm_vol_ratio,
                                        'y': hist['atm_vol_ratio'].values, 'clean_num_obs': max(100, round(3*len(hist.index)/4))})

    return {'hist': hist, 'current': current,
            'atm_vol_ratio': atm_vol_ratio,
            'real_vol_ratio': current['close2close_vol20'][0]/current['close2close_vol20'][1],
            'atm_real_vol_ratio': current['atm_vol'][0]/current['close2close_vol20'][0],
            'q': q}


def get_aligned_option_indicators(**kwargs):

    ticker_list = kwargs['ticker_list']
    settle_datetime = cu.convert_doubledate_2datetime(kwargs['settle_date'])

    if 'num_cal_days_back' in kwargs.keys():
        num_cal_days_back = kwargs['num_cal_days_back']
    else:
        num_cal_days_back = 20*365

    contract_specs_output_list = [cmi.get_contract_specs(x) for x in ticker_list]
    ticker_head_list = [x['ticker_head'] for x in contract_specs_output_list]

    if 'option_ticker_indicator_dictionary' in kwargs.keys():
        option_ticker_indicator_dictionary = kwargs['option_ticker_indicator_dictionary']
    else:
        con = msu.get_my_sql_connection(**kwargs)
        unique_ticker_heads = list(set(ticker_head_list))
        option_ticker_indicator_dictionary = {x: get_option_ticker_indicators(ticker_head=x,
                                                                              settle_date_to=kwargs['settle_date'],
                                                                              num_cal_days_back=num_cal_days_back,
                                                                              con=con) for x in unique_ticker_heads}
        if 'con' not in kwargs.keys():
            con.close()

    option_ticker_indicator_dictionary_final = {ticker_list[x]: option_ticker_indicator_dictionary[ticker_head_list[x]] for x in range(len(ticker_list))}

    max_available_settle_list = []
    tr_dte_list = []
    atm_vol_list = []
    close2close_vol20_list = []
    volume_list = []
    open_interest_list = []

    for x in range(len(ticker_list)):
        ticker_data = option_ticker_indicator_dictionary_final[ticker_list[x]]
        ticker_data = ticker_data[ticker_data['settle_date'] <= settle_datetime]
        option_ticker_indicator_dictionary_final[ticker_list[x]] = ticker_data
        ticker_data = ticker_data[ticker_data['ticker'] == ticker_list[x]]
        max_available_settle_list.append(ticker_data['settle_date'].iloc[-1])

    last_available_settle = min(max_available_settle_list)

    for x in range(len(ticker_list)):
        ticker_data = option_ticker_indicator_dictionary_final[ticker_list[x]]
        ticker_data = ticker_data[(ticker_data['ticker'] == ticker_list[x])&(ticker_data['settle_date'] == last_available_settle)]
        tr_dte_list.append(ticker_data['tr_dte'].iloc[0])
        atm_vol_list.append(ticker_data['atm_vol'].iloc[0])
        close2close_vol20_list.append(ticker_data['close2close_vol20'].iloc[0])
        volume_list.append(ticker_data['volume'].iloc[0])
        open_interest_list.append(ticker_data['open_interest'].iloc[0])

    current_data = pd.DataFrame.from_items([('ticker',ticker_list),
                             ('tr_dte', tr_dte_list),
                             ('atm_vol', atm_vol_list),
                             ('close2close_vol20', close2close_vol20_list),
                             ('volume', volume_list),
                             ('open_interest', open_interest_list)])

    current_data['settle_date'] = last_available_settle
    current_data.set_index('ticker', drop=True, inplace=True)

    current_data = current_data[['settle_date', 'tr_dte', 'atm_vol', 'close2close_vol20', 'volume', 'open_interest']]

    aggregation_method = max([ocu.get_aggregation_method_contracts_back({'ticker_class': x['ticker_class'],
                                                                         'ticker_head': x['ticker_head']})['aggregation_method'] for x in contract_specs_output_list])

    tr_days_half_band_width_selected = ocu.tr_days_half_band_with[aggregation_method]
    data_frame_list = []
    aligned_data_list = []

    max_tr_dte = current_data['tr_dte'].max()
    max_ref_dte = sut.get_closest(list_input=cmi.aligned_data_tr_dte_list, target_value=max_tr_dte)

    #print(max_ref_dte)

    for x in range(len(ticker_list)):
        ticker_data = option_ticker_indicator_dictionary_final[ticker_list[x]]

        if ticker_head_list[x] in ['ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
            model = 'OU'
        else:
            model = 'BS'

        #print(current_data['tr_dte'].loc[ticker_list[x]])


        #ref_dte = sut.get_closest(list_input=cmi.aligned_data_tr_dte_list, target_value=current_data['tr_dte'].loc[ticker_list[x]])

        if aggregation_method == 12:
            ref_dte = sut.get_closest(list_input=cmi.aligned_data_tr_dte_list, target_value=current_data['tr_dte'].loc[ticker_list[x]]+max_ref_dte-max_tr_dte)
            aligned_data = gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[ticker_head_list[x]],
                                                    tr_dte_center=ref_dte,
                                                    contract_month_letter=contract_specs_output_list[x]['ticker_month_str'],
                                                    model=model)
        else: # what about regular quarterly etc?

            tr_dte_upper_band = current_data['tr_dte'].loc[ticker_list[x]]+tr_days_half_band_width_selected
            tr_dte_lower_band = current_data['tr_dte'].loc[ticker_list[x]]-tr_days_half_band_width_selected

            ref_tr_dte_list = [y for y in cmi.aligned_data_tr_dte_list if y <= tr_dte_upper_band and y>=tr_dte_lower_band]

            aligned_data_list = [gop.load_aligend_options_data_file(ticker_head=cmi.aligned_data_tickerhead[ticker_head_list[x]],
                                                    tr_dte_center=y,
                                                    contract_month_letter=contract_specs_output_list[x]['ticker_month_str'],
                                                    model=model) for y in ref_tr_dte_list]

            aligned_data_list = [y[(y['trDTE']>=current_data['tr_dte'].loc[ticker_list[x]]-tr_days_half_band_width_selected)&
              (y['trDTE'] <= current_data['tr_dte'].loc[ticker_list[x]]+tr_days_half_band_width_selected)] for y in aligned_data_list]

            return aligned_data_list







        #print(aligned_data['trDTE'].max())
        #print(aligned_data['trDTE'].min())

        #return aligned_data

        aligned_data['settle_date'] = pd.to_datetime(aligned_data['settleDates'].astype('str'), format='%Y%m%d')

        aligned_data.rename(columns={'TickerYear': 'ticker_year',
                                     'TickerMonth': 'ticker_month',
                                     'trDTE': 'tr_dte',
                                     'calDTE': 'cal_dte',
                                     'impVol': 'atm_vol',
                                     'close2CloseVol20': 'close2close_vol20'}, inplace=True)

        aligned_data = aligned_data[['settle_date','ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'atm_vol', 'close2close_vol20']]

        aligned_data_list.append(aligned_data)

        if aggregation_method == 12:
            ticker_data = ticker_data[ticker_data['ticker_month'] == contract_specs_output_list[x]['ticker_month_num']]

        tr_dte_selection = (ticker_data['tr_dte'] >= current_data['tr_dte'].loc[ticker_list[x]]-tr_days_half_band_width_selected)&\
                           (ticker_data['tr_dte'] <= current_data['tr_dte'].loc[ticker_list[x]]+tr_days_half_band_width_selected)

        ticker_data = ticker_data[tr_dte_selection]

        ticker_data = pd.concat([aligned_data, ticker_data[['settle_date','ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'atm_vol', 'close2close_vol20']]])

        data_frame_list.append(ticker_data)

    for x in range(len(ticker_list)):
        data_frame_list[x].set_index('settle_date', inplace=True)
        data_frame_list[x]['atm_vol'] = data_frame_list[x]['atm_vol'].astype('float64')

    merged_dataframe = pd.concat(data_frame_list, axis=1, join='inner',keys=['c'+ str(x+1) for x in range(len(ticker_list))])

    merged_dataframe

    return {'hist': merged_dataframe, 'current': current_data }


def get_ranked_contract_selection_4vol_strategies(**kwargs):

    ref_tr_dte_list = kwargs['ref_tr_dte_list']
    tr_dte_list = kwargs['tr_dte_list']

    selection_matrix = []

    for i in range(len(tr_dte_list)):

        selection_list = [np.NaN]*len(tr_dte_list)
        abs_diff_list = [abs(x-tr_dte_list[i]) for x in ref_tr_dte_list[i]]
        selection_list[i] = abs_diff_list.index(min(abs_diff_list))

        for j in range(len(tr_dte_list)):

            if j == i:
                continue

            abs_diff_list = [abs(x-(tr_dte_list[j]-tr_dte_list[i]+ref_tr_dte_list[i][selection_list[i]])) for x in ref_tr_dte_list[j]]
            selection_list[j] = abs_diff_list.index(min(abs_diff_list))

        if selection_list not in selection_matrix:
            selection_matrix.append(selection_list)

    sort_index = np.argsort([abs(x-tr_dte_list[0]) for x in ref_tr_dte_list[0]])

    for i in range(1, len(sort_index)):
        selection_list = [np.NaN]*len(tr_dte_list)
        selection_list[0] = sort_index[i]

        for j in range(1, len(tr_dte_list)):

            abs_diff_list = [abs(x-(tr_dte_list[j]-tr_dte_list[0]+ref_tr_dte_list[0][selection_list[0]])) for x in ref_tr_dte_list[j]]
            selection_list[j] = abs_diff_list.index(min(abs_diff_list))

        if selection_list not in selection_matrix:
            selection_matrix.append(selection_list)

    return selection_matrix






























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


def calc_volume_interest_4ticker(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    sql_query = 'SELECT sum(volume),sum(open_interest) FROM futures_master.daily_option_price WHERE ticker=\'' + kwargs['ticker'] + '\'' + \
                ' and price_date=' + str(kwargs['settle_date'])

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    return {'volume': int(data[0][0]) if data[0][0] is not None else np.NaN,
            'open_interest': int(data[0][1]) if data[0][1] is not None else np.NaN}


def calc_realized_vol_4options_ticker(**kwargs):

    ticker = kwargs['ticker']
    contract_specs_output = cmi.get_contract_specs(ticker)

    if contract_specs_output['ticker_class'] in ['Index', 'FX', 'Metal']:
        use_proxy_contract = True
    else:
        use_proxy_contract = False

    if use_proxy_contract:

        if 'futures_data_dictionary' in kwargs.keys():
            futures_data_input = {'ticker_head': contract_specs_output['ticker_head'],'settle_date': kwargs['settle_date']}
            futures_data_input['futures_data_dictionary'] = kwargs['futures_data_dictionary']
            data_out = gfp.get_futures_price_preloaded(**futures_data_input)
            data_out = data_out.reset_index()
            kwargs['ticker'] = data_out['ticker'].loc[data_out['volume'].idxmax()]
    else:
        kwargs['ticker'] = omu.get_option_underlying(**kwargs)

    return calc_realized_vol_4futures_ticker(**kwargs)


def calc_realized_vol_4futures_ticker(**kwargs):

    settle_date = kwargs.pop('settle_date')

    num_obs = kwargs.pop('num_obs', 20)

    futures_price_output = gfp.get_futures_price_preloaded(**kwargs)

    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    futures_price_selected = futures_price_output[futures_price_output['settle_date'] <= settle_datetime]

    logreturns = np.log(futures_price_selected['close_price'][-(num_obs+1):]/
                         futures_price_selected['close_price'][-(num_obs+1):].shift(1))

    return 100*np.sqrt(252*np.mean(np.square(logreturns)))


def get_option_ticker_indicators(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    if 'ticker' in kwargs.keys():
        filter_string = 'WHERE ticker=\'' + kwargs['ticker'] + '\''
    elif 'ticker_head' in kwargs.keys():
        filter_string = 'WHERE ticker_head=\'' + kwargs['ticker_head'] + '\''
    else:
        filter_string = ''

    if 'settle_date' in kwargs.keys():
        if filter_string == '':
            filter_string = filter_string + ' WHERE price_date=' + str(kwargs['settle_date'])
        else:
            filter_string = filter_string + ' and price_date=' + str(kwargs['settle_date'])

    if 'settle_date_to' in kwargs.keys():
        if filter_string == '':
            filter_string = filter_string + ' WHERE price_date<=' + str(kwargs['settle_date_to'])
        else:
            filter_string = filter_string + ' and price_date<=' + str(kwargs['settle_date_to'])

    if 'num_cal_days_back' in kwargs.keys():
        date_from = cu.doubledate_shift(kwargs['settle_date_to'], kwargs['num_cal_days_back'])
        filter_string = filter_string + ' and price_date>=' + str(date_from)

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = ['ticker', 'price_date' , 'ticker_head', 'ticker_month', 'ticker_year', 'cal_dte', 'tr_dte', 'atm_vol','close2close_vol20', 'volume','open_interest']

    sql_query = 'SELECT ' + ",".join(column_names) + ' FROM option_ticker_indicators ' + filter_string

    cur = con.cursor()
    cur.execute(sql_query)
    data = cur.fetchall()

    if 'con' not in kwargs.keys():
        con.close()

    data_frame_output = pd.DataFrame(data, columns=['settle_date' if x == 'price_date' else x for x in column_names])
    data_frame_output['atm_vol'] = data_frame_output['atm_vol'].astype('float64')
    data_frame_output['close2close_vol20'] = data_frame_output['close2close_vol20'].astype('float64')

    return data_frame_output.sort(['ticker_head','settle_date', 'tr_dte'], ascending=[True, True,True], inplace=False)






