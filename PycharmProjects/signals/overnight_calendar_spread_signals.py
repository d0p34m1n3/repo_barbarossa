
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import opportunity_constructs.spread_carry as sc
import opportunity_constructs.utilities as opUtil
import shared.statistics as stats
import get_price.get_futures_price as gfp
import signals.technical_indicators as ti
import numpy as np


def get_overnight_calendar_signals(**kwargs):

    ticker_list = kwargs['ticker_list']
    date_to = kwargs['date_to']

    #print(ticker_list)

    if 'tr_dte_list' in kwargs.keys():
        tr_dte_list = kwargs['tr_dte_list']
    else:
        tr_dte_list = [exp.get_futures_days2_expiration({'ticker': x,'date_to': date_to}) for x in ticker_list]

    if 'aggregation_method' in kwargs.keys() and 'contracts_back' in kwargs.keys():
        aggregation_method = kwargs['aggregation_method']
        contracts_back = kwargs['contracts_back']
    else:
        amcb_output = opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(ticker_list[0]))
        aggregation_method = amcb_output['aggregation_method']
        contracts_back = amcb_output['contracts_back']

    if 'use_last_as_current' in kwargs.keys():
        use_last_as_current = kwargs['use_last_as_current']
    else:
        use_last_as_current = False

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [cmi.get_contract_specs(ticker_list[0])['ticker_head']]}

    if 'contract_multiplier' in kwargs.keys():
        contract_multiplier = kwargs['contract_multiplier']
    else:
        contract_multiplier = cmi.contract_multiplier[cmi.get_contract_specs(ticker_list[0])['ticker_head']]

    aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_list,
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=use_last_as_current)

    if not aligned_output['success']:
        return {'success': False,'ts_slope5': np.nan,
                'ts_slope10': np.nan,
                'linear_deviation5': np.nan,
                'linear_deviation10': np.nan,
                'momentum5': np.nan, 'momentum10': np.nan,
                'underlying_zscore': np.nan,
                'q_carry': np.nan,
                'q_carry_average': np.nan,
                'reward_risk': np.nan,
                'spread_price': np.nan,
                'normalized_target': np.nan,
                'pnl_per_contract': np.nan,
                'noise_100': np.nan,
                'contract_multiplier': contract_multiplier}

    aligned_data = aligned_output['aligned_data']
    current_data = aligned_output['current_data']
    aligned_data['spread_change_1'] = aligned_data['c1']['change_1']-aligned_data['c2']['change_1']
    aligned_data['spread_price'] = aligned_data['c1']['close_price']-aligned_data['c2']['close_price']
    spread_price_current = current_data['c1']['close_price']-current_data['c2']['close_price']

    noise_100 = np.std(aligned_data['spread_change_1'].iloc[-100:])

    if noise_100 == 0:
        noise_100 = np.nan

    current_spread_data = aligned_data[aligned_data['c1']['cont_indx'] == current_data['c1']['cont_indx']]
    current_spread_data_last10 = current_spread_data.iloc[-10:]

    ts_regression_output5 = ti.time_series_regression(data_frame_input=current_spread_data, num_obs=5, y_var_name='spread_price')
    ts_regression_output10 = ti.time_series_regression(data_frame_input=current_spread_data, num_obs=10, y_var_name='spread_price')

    normalized_target = (current_data['c1']['change1_instant']-current_data['c2']['change1_instant'])/noise_100
    pnl_per_contract = (current_data['c1']['change1_instant']-current_data['c2']['change1_instant'])*contract_multiplier

    ts_slope5 = ts_regression_output5['beta']/noise_100
    ts_slope10 = ts_regression_output10['beta']/noise_100

    linear_deviation5 = ts_regression_output5['zscore']
    linear_deviation10 = ts_regression_output10['zscore']

    momentum5 = (spread_price_current-current_spread_data['spread_price'].iloc[-6])/noise_100
    momentum10 = (spread_price_current-current_spread_data['spread_price'].iloc[-11])/noise_100

    underlying_regression_output = stats.get_regression_results({'y': current_spread_data_last10['spread_price'],
                                  'x': current_spread_data_last10['c1']['close_price'],
                                  'y_current': spread_price_current,
                                  'x_current': current_data['c1']['close_price'],
                                  'clean_num_obs': 10})

    underlying_zscore = underlying_regression_output['zscore']

    q_carry = np.nan
    q_carry_average = np.nan
    reward_risk = np.nan

    spread_carry_output = sc.generate_spread_carry_sheet_4date(report_date=date_to)

    if spread_carry_output['success']:
        spread_report = spread_carry_output['spread_report']
        selected_line = spread_report[(spread_report['ticker1']==ticker_list[0])&(spread_report['ticker2']==ticker_list[1])]
        if not selected_line.empty:
            q_carry = selected_line['q_carry'].iloc[0]
            q_carry_average = selected_line['q_carry_average'].iloc[0]
            reward_risk = selected_line['reward_risk'].iloc[0]

    return {'success': True, 'ts_slope5': ts_slope5,
            'ts_slope10': ts_slope10,
            'linear_deviation5': linear_deviation5,
            'linear_deviation10': linear_deviation10,
            'momentum5': momentum5, 'momentum10': momentum10,
            'underlying_zscore': underlying_zscore,
            'q_carry': q_carry,
            'q_carry_average': q_carry_average,
            'reward_risk': reward_risk,
            'spread_price': spread_price_current,
            'normalized_target': normalized_target,
            'pnl_per_contract': pnl_per_contract,
            'noise_100': noise_100,
            'contract_multiplier': contract_multiplier}
