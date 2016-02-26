__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import opportunity_constructs.constants as const
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import pandas as pd
import contract_utilities.expiration as exp
import scipy.io

pd.options.mode.chained_assignment = None

tr_days_half_band_with = {1: const.monthly_tr_days_half_band_width,
                          3: const.quarterly_tr_days_half_band_width,
                          12: const.annual_tr_days_half_band_width}

def get_aggregation_method_contracts_back(utilities_input):

    ticker_class = utilities_input['ticker_class']
    ticker_head = utilities_input['ticker_head']

    if ticker_class in ['Ag', 'Livestock', 'Soft'] or ticker_head in ['NG', 'HO', 'RB']:
        aggregation_method = 12
        contracts_back = const.annualContractsBack
    elif ticker_class in ['Energy', 'Index', 'FX', 'Treasury','Metal']:
        aggregation_method = 1
        contracts_back = const.monthlyContractsBack
    elif ticker_class == 'STIR':
        aggregation_method = 3
        contracts_back = const.quarterlyContractsBack

    return {'aggregation_method': aggregation_method, 'contracts_back': contracts_back}

def get_cont_indx_list_history(utilities_input):

    current_year = utilities_input['current_year']
    current_month = utilities_input['current_month']
    aggregation_method = utilities_input['aggregation_method']
    contracts_back = utilities_input['contracts_back']

    cont_indx = 100*current_year+current_month
    month_list_aux = []
    year_list_aux = []

    if aggregation_method==1:
        for x in range(current_year,current_year-(contracts_back//12)-2, -1):
            month_list_aux.extend(list(range(12, 0, -1)))
            year_list_aux.extend([x]*12)

        year_month_list = [100*year_list_aux[x]+month_list_aux[x] for x in range(len(year_list_aux)) ]
        year_month_list = [x for x in year_month_list if x <= cont_indx]
        cont_indx_list = year_month_list[:contracts_back]

    elif aggregation_method==3:
        for x in range(current_year,current_year-(contracts_back//4)-2,-1):
            month_list_aux.extend(list(range(12,0,-3)))
            year_list_aux.extend([x]*4)
        value_to_substract = 0 if current_month%3==0 else 3-current_month%3
        year_month_list = [100*year_list_aux[x]+month_list_aux[x]-value_to_substract for x in range(len(year_list_aux)) ]
        year_month_list = [x for x in year_month_list if x<=cont_indx]
        cont_indx_list = year_month_list[:contracts_back]
    elif aggregation_method==12:
        cont_indx_list =list(range(cont_indx,cont_indx-100*(contracts_back),-100))

    return cont_indx_list


def get_aligned_futures_data(**kwargs):

    contract_list = kwargs['contract_list']
    aggregation_method = kwargs['aggregation_method']
    contracts_back = kwargs['contracts_back']
    date_to = kwargs['date_to']

    if 'use_last_as_current' in kwargs.keys():
        use_last_as_current = kwargs['use_last_as_current']
    else:
        use_last_as_current = False

    if 'tr_dte_list' in kwargs.keys():
        tr_dte_list = kwargs['tr_dte_list']
    else:
        tr_dte_list = [exp.get_futures_days2_expiration({'ticker': x,'date_to': date_to}) for x in contract_list]

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in contract_list]
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in ticker_head_list}

    date_from = cu.doubledate_shift(date_to, 2*3650)

    date_to_datetime = cu.convert_doubledate_2datetime(date_to)
    date_from_datetime = cu.convert_doubledate_2datetime(date_from)

    tr_days_half_band_width_selected = tr_days_half_band_with[aggregation_method]

    num_contracts = len(contract_list)

    contract_specs_list = [cmi.get_contract_specs(x) for x in contract_list]

    data_frame_list = []

    for i in range(num_contracts):

        futures_data_frame = futures_data_dictionary[contract_specs_list[i]['ticker_head']]

        selection_indx = (futures_data_frame['tr_dte'] >= tr_dte_list[i]-tr_days_half_band_width_selected)& \
                             (futures_data_frame['tr_dte'] <= tr_dte_list[i]+tr_days_half_band_width_selected)& \
                             (futures_data_frame['cal_dte'] >= 0)& \
                             (futures_data_frame['settle_date'] >= date_from_datetime) & \
                             (futures_data_frame['settle_date'] <= date_to_datetime)

        futures_data_frame = futures_data_frame[selection_indx]

        data_frame_list.append(futures_data_frame)

    cont_indx_list_rolls = [get_cont_indx_list_history({'current_year': x['ticker_year'],
                                 'current_month': x['ticker_month_num'],
                                 'aggregation_method': aggregation_method,
                                  'contracts_back': contracts_back}) for x in contract_specs_list]

    merged_dataframe_list = [None]*contracts_back

    for i in range(contracts_back):
        if sum([(data_frame_list[j]['cont_indx'] == cont_indx_list_rolls[j][i]).any() for j in range(len(contract_list))])<len(contract_list):
            continue
        contract_data_list = [None]*num_contracts
        for k in range(num_contracts):
            contract_data_list[k] = data_frame_list[k][data_frame_list[k]['cont_indx']==cont_indx_list_rolls[k][i]]
            contract_data_list[k].set_index('settle_date',inplace=True)

        merged_dataframe_list[i] = pd.concat(contract_data_list, axis=1, join='inner',keys=['c'+ str(x+1) for x in range(num_contracts)])

    aligned_dataframe = pd.concat(merged_dataframe_list)
    aligned_dataframe.sort_index(inplace=True)

    aligned_dataframe['tr_dte_match'] = abs(aligned_dataframe['c1']['tr_dte']-tr_dte_list[0])
    aligned_dataframe['settle_date'] = aligned_dataframe.index

    aligned_dataframe.sort(['settle_date','tr_dte_match'],ascending=[True,True],inplace=True)
    aligned_dataframe.drop_duplicates('settle_date',inplace=True)

    if use_last_as_current:
        current_data = aligned_dataframe.iloc[-1]
    else:
        current_data = aligned_dataframe.loc[cu.convert_doubledate_2datetime(date_to)]

    return {'aligned_data': aligned_dataframe, 'current_data': current_data}


def get_column_names_4option_data():

    fwd_looking_window_list = [5, 10, 20, 40, 60]

    optional_cc_skew_columns = ['vegaPnL', 'pastAtmVol1',
            'atmThetaBs','atmVegaBs','atmVegaOu','atmGammaBs',
            'Kvol','LogPrice','SquaredPrice', 'KnsImpBsSkew', 'KnsImpOuSkew',
            'MFIVolPath', 'BkmImpSkewness', 'BkmImpKurtosis',
            'ImpSkewStickinessRatio','SkewStickinessRatio',
            'dImpVolDk', 'BSHistKnsRzVar', 'OUHistKnsRzVar',
            'KnsRzBsSkew', 'DRzOuSkewness', 'DRzSkewness','DRzKurtosis', 'BiPowerVol',
            'BSHistBkmImpBipowerRzVolPremAnn','BSHistSkewPremiumAnn','BSHistScaledKurtPremiumAnn',
            'HistKnsImpVar','HistKnsImpSkew','HistKnsImpKurt' , 'contINDX', 'optBpvs',
            'yzVol20', 'timeValue', 'pastAtmVol20', 'pastAtmVolChange20', 'pastAtmVolChange1', 'expectedDailyMove',
             'optionPnL', 'deltaPnL', 'gammaPnL', 'thetaPnL', 'dSpot', 'Theta30days']

    default_aligned_data_column_names = ['settleDates', 'TickerYear', 'TickerMonth', 'trDTE', 'calDTE',
        'underlying', 'atmVol', 'strike', 'theoValue', 'theoPremium',
        'impVol', 'delta', 'gamma', 'vega', 'theta', 'rate2OptExp',
        'dollarVega', 'dollarTheta', 'normDollarTheta', 'dollarGamma', 'dailyPnL', 'dailyUnhedgedPnL',
        'close2CloseVol10', 'close2CloseVol20', 'close2CloseVol60',
        'dailySdMove', 'success']

    window_fields = sum([['profit'+str(x), 'unhedgedProfit'+str(x), 'onceHedgedProfit'+str(x),
                         'underlyingProfit'+str(x), 'vegaProfit'+str(x), 'gammaProfit'+str(x),
                         'thetaProfit'+str(x), 'impVolFuture'+str(x), 'atmVolFuture'+str(x),
                         'futureAtmVolChange'+str(x), 'spotFuture'+str(x)] for x in fwd_looking_window_list],[])

    return default_aligned_data_column_names + window_fields + optional_cc_skew_columns


def load_aligend_options_data_file(**kwargs):

    ticker_head = kwargs['ticker_head']
    tr_dte_center = kwargs['tr_dte_center']

    option_data_dir = r'C:\Research\data\alignedTimeSeriesOutputTemp/'

    if 'delta_center' in kwargs.keys():
        delta_center = kwargs['delta_center']
    else:
        delta_center = 0.5

    if 'model' in kwargs.keys():
        model = kwargs['model']
    else:
        model = 'BS'

    if 'contract_month_letter' in kwargs.keys():
        contract_month_str = '_' + kwargs['contract_month_letter']
    else:
        contract_month_str = ''

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = get_column_names_4option_data()

    file_dir = ticker_head + '_' + str(delta_center) + '_' + model + '_20_510204060_' + str(tr_dte_center) + contract_month_str + '.mat'

    mat_output = scipy.io.loadmat(option_data_dir+file_dir)

    return pd.DataFrame(mat_output['alignedDataMatrix'],columns=column_names)





