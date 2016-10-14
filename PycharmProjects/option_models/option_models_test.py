
import contract_utilities.contract_meta_info as cmi
import get_price.get_options_price as gop
import pandas as pd
import shared.directory_names as dn
import os.path
pd.options.mode.chained_assignment = None  # default='warn'
import option_models.utils as omu


def generate_test_cases_from_aligned_option_data(**kwargs):

    test_data_dir = dn.get_directory_name(ext='option_model_test_data')

    if os.path.isfile(test_data_dir + '/option_model_test_data.pkl'):
        data_frame_test = pd.read_pickle(test_data_dir + '/option_model_test_data.pkl')
        return data_frame_test

    ticker_head_list = cmi.cme_option_tickerhead_list
    tr_dte_center_list = [10, 15, 20, 25, 30, 35, 40, 45, 50,
                          60, 70, 80, 90, 100, 120, 140, 180,
                          200, 220, 240, 260, 280, 300, 330, 360, 390]

    delta_list = [0.1, 0.15, 0.25, 0.35, 0.5, -0.1, -0.15, -0.25, -0.35, -0.5]

    data_frame_list = []

    aligned_column_names = ['TickerYear','TickerMonth','settleDates','calDTE','rate2OptExp', 'theoValue' , 'impVol', 'atmVol', 'delta',
                            'strike', 'underlying', 'dollarVega', 'dollarTheta','gamma','dollarGamma', 'optionPnL', 'deltaPnL', 'gammaPnL', 'thetaPnL']

    #ticker_head_list = ticker_head_list[3:5]

    for i in range(len(ticker_head_list)):

        if ticker_head_list[i] in ['ED', 'E0', 'E2', 'E3', 'E4', 'E5']:
            model = 'OU'
        else:
            model = 'BS'

        ticker_class = cmi.ticker_class[ticker_head_list[i]]
        contract_multiplier = cmi.contract_multiplier[ticker_head_list[i]]

        if ticker_class in ['Livestock', 'Ag'] or ticker_head_list[i] == 'NG':
            month_specificQ = True
            contract_month_list = cmi.get_option_contract_months(ticker_head=ticker_head_list[i])

        else:
            month_specificQ = False

        for j in range(len(tr_dte_center_list)):
            for k in range(len(delta_list)):

                if month_specificQ:

                    for contract_month in contract_month_list:

                        data_frame = gop.load_aligend_options_data_file(ticker_head=ticker_head_list[i],
                                                    tr_dte_center=tr_dte_center_list[j],
                                                    delta_center=delta_list[k],
                                                    contract_month_letter=contract_month,
                                                    model=model)
                        if data_frame.empty:
                            continue
                        else:

                            x1 = round(len(data_frame.index)/4)
                            x2 = round(2*len(data_frame.index)/4)
                            x3 = round(3*len(data_frame.index)/4)

                            data_frame_select = data_frame[aligned_column_names].iloc[[-x3, -x2, -x1, -1]]
                            data_frame_select['tickerHead'] = ticker_head_list[i]
                            data_frame_select['contractMultiplier'] = contract_multiplier
                            data_frame_select['exercise_type'] = cmi.get_option_exercise_type(ticker_head=ticker_head_list[i])

                            data_frame_list.append(data_frame_select)

                else:
                    data_frame = gop.load_aligend_options_data_file(ticker_head=ticker_head_list[i],
                                                    tr_dte_center=tr_dte_center_list[j],
                                                    delta_center=delta_list[k],
                                                    model=model)
                    if data_frame.empty:
                        continue
                    else:

                        x1 = round(len(data_frame.index)/4)
                        x2 = round(2*len(data_frame.index)/4)
                        x3 = round(3*len(data_frame.index)/4)

                        data_frame_select = data_frame[aligned_column_names].iloc[[-x3, -x2, -x1, -1]]
                        data_frame_select['tickerHead'] = ticker_head_list[i]
                        data_frame_select['contractMultiplier'] = contract_multiplier
                        data_frame_select['exercise_type'] = cmi.get_option_exercise_type(ticker_head=ticker_head_list[i])
                        data_frame_list.append(data_frame_select)

    data_frame_test = pd.concat(data_frame_list)
    data_frame_test['optionPnL'] = data_frame_test['optionPnL']*data_frame_test['contractMultiplier']
    data_frame_test['deltaPnL'] = data_frame_test['deltaPnL']*data_frame_test['contractMultiplier']

    data_frame_test['ticker'] = [data_frame_test['tickerHead'].iloc[x] +
                                 cmi.letter_month_string[int(data_frame_test['TickerMonth'].iloc[x]-1)] +
                                 str(int(data_frame_test['TickerYear'].iloc[x]))
                                 for x in range(len(data_frame_test.index))]

    data_frame_test['option_type'] = 'C'
    data_frame_test['option_type'][data_frame_test['delta'] < 0] = 'P'

    data_frame_test.to_pickle(test_data_dir + '/option_model_test_data.pkl')
    return data_frame_test


def test_option_models(**kwargs):

    test_data_dir = dn.get_directory_name(ext='option_model_test_data')

    engine_name = kwargs['engine_name']

    if os.path.isfile(test_data_dir + '/' + engine_name + '.pkl'):
        data_frame_test = pd.read_pickle(test_data_dir + '/' + engine_name + '.pkl')
        return data_frame_test

    data_frame_test = generate_test_cases_from_aligned_option_data()
    data_frame_test = data_frame_test[data_frame_test['strike'].notnull()]

    if 'num_cases' in kwargs.keys():
        data_frame_test = data_frame_test.iloc[0:kwargs['num_cases']]

    model_wrapper_output = []

    #return data_frame_test['settleDates'].iloc[no]

    for no in range(len(data_frame_test.index)):

        #print('true vol: ' + str(data_frame_test['impVol'].iloc[no]))
        #print('int rate: ' + str(data_frame_test['rate2OptExp'].iloc[no]))
        print(no)

        model_wrapper_output.append(omu.option_model_wrapper(ticker=data_frame_test['ticker'].iloc[no],
                                                        calculation_date=int(data_frame_test['settleDates'].iloc[no]),
                                                        underlying=data_frame_test['underlying'].iloc[no],
                                                        strike = data_frame_test['strike'].iloc[no],
                                                        option_price=data_frame_test['theoValue'].iloc[no],
                                                        exercise_type = data_frame_test['exercise_type'].iloc[no],
                                                        option_type=data_frame_test['option_type'].iloc[no],
                                                        engine_name=engine_name))

    #return model_wrapper_output

    data_frame_test['vol_deviation'] = [100*(model_wrapper_output[no]['implied_vol']-data_frame_test['impVol'].iloc[no])/data_frame_test['impVol'].iloc[no] for no in range(len(data_frame_test.index))]
    data_frame_test['dollar_gamma_deviation'] = [100*(model_wrapper_output[no]['dollar_gamma']-data_frame_test['dollarGamma'].iloc[no])/data_frame_test['dollarGamma'].iloc[no] for no in range(len(data_frame_test.index))]
    data_frame_test['gamma_deviation'] = [100*(model_wrapper_output[no]['gamma']-data_frame_test['gamma'].iloc[no])/data_frame_test['gamma'].iloc[no] for no in range(len(data_frame_test.index))]
    data_frame_test['delta_deviation'] = [100*(model_wrapper_output[no]['delta']-data_frame_test['delta'].iloc[no]) for no in range(len(data_frame_test.index))]
    data_frame_test['dollar_vega_deviation'] = [100*(model_wrapper_output[no]['dollar_vega']-data_frame_test['dollarVega'].iloc[no])/data_frame_test['dollarVega'].iloc[no] for no in range(len(data_frame_test.index))]
    data_frame_test['dollar_theta_deviation'] = [100*(model_wrapper_output[no]['dollar_theta']-data_frame_test['dollarTheta'].iloc[no])/data_frame_test['dollarTheta'].iloc[no] for no in range(len(data_frame_test.index))]
    data_frame_test['interest_rate_deviation'] = [100*(model_wrapper_output[no]['interest_rate']-data_frame_test['rate2OptExp'].iloc[no]) for no in range(len(data_frame_test.index))]
    data_frame_test['cal_dte_deviation'] = [model_wrapper_output[no]['cal_dte']-data_frame_test['calDTE'].iloc[no] for no in range(len(data_frame_test.index))]

    data_frame_test.to_pickle(test_data_dir + '/' + engine_name + '.pkl')
    return data_frame_test

    #return 100*(model_wrapper_output['implied_vol']-data_frame_test['impVol'].iloc[no])/data_frame_test['impVol'].iloc[no]

def generate_csv_file(**kwargs):

    output_dir = dn.get_directory_name(ext='test_data')
    engine_name = kwargs['engine_name']

    data_frame_test = generate_test_cases_from_aligned_option_data()
    data_frame_test = data_frame_test[data_frame_test['strike'].notnull()]

    if 'num_cases' in kwargs.keys():
        data_frame_test = data_frame_test.iloc[0:kwargs['num_cases']]

    model_wrapper_output = []
    for no in range(len(data_frame_test.index)):

        #print('true vol: ' + str(data_frame_test['impVol'].iloc[no]))
        #print('int rate: ' + str(data_frame_test['rate2OptExp'].iloc[no]))
        print(no)

        model_wrapper_output.append(omu.option_model_wrapper(ticker=data_frame_test['ticker'].iloc[no],
                                                        calculation_date=int(data_frame_test['settleDates'].iloc[no]),
                                                        underlying=data_frame_test['underlying'].iloc[no],
                                                        strike = data_frame_test['strike'].iloc[no],
                                                        option_price=data_frame_test['theoValue'].iloc[no],
                                                        exercise_type = data_frame_test['exercise_type'].iloc[no],
                                                        option_type=data_frame_test['option_type'].iloc[no],
                                                        engine_name=engine_name))

    #return model_wrapper_output

    data_frame_test['impVol'] = [model_wrapper_output[no]['implied_vol'] for no in range(len(data_frame_test.index))]
    data_frame_test['dollarGamma'] = [model_wrapper_output[no]['dollar_gamma'] for no in range(len(data_frame_test.index))]
    data_frame_test['gamma'] = [model_wrapper_output[no]['gamma'] for no in range(len(data_frame_test.index))]
    data_frame_test['delta'] = [model_wrapper_output[no]['delta'] for no in range(len(data_frame_test.index))]
    data_frame_test['dollarVega'] = [model_wrapper_output[no]['dollar_vega']for no in range(len(data_frame_test.index))]
    data_frame_test['dollarTheta'] = [model_wrapper_output[no]['dollar_theta'] for no in range(len(data_frame_test.index))]
    data_frame_test['rate2OptExp'] = [model_wrapper_output[no]['interest_rate'] for no in range(len(data_frame_test.index))]
    data_frame_test['calDTE'] = [model_wrapper_output[no]['cal_dte'] for no in range(len(data_frame_test.index))]

    data_frame_test = data_frame_test[data_frame_test['impVol'].notnull()]
    data_frame_test = data_frame_test[~data_frame_test['tickerHead'].isin(['ED','E0','E1','E2','E3','E4','E5'])]

    data_frame_test = data_frame_test[['settleDates','ticker','option_type','strike','underlying','theoValue',
                                       'impVol','delta','dollarVega','dollarTheta','dollarGamma','rate2OptExp']]

    data_frame_test.reset_index(drop=True, inplace=True)

    writer = pd.ExcelWriter(output_dir + '/' + 'option_model_test' + '.xlsx', engine='xlsxwriter')
    data_frame_test.to_excel(writer, sheet_name='all')

