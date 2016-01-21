
import my_sql_routines.my_sql_utilities as msu
import ta.strategy as ts
import shared.converters as sc
import signals.futures_signals as fs
import contract_utilities.expiration as exp

def get_results_4strategy(**kwargs):

    alias = kwargs['alias']
    con = msu.get_my_sql_connection(**kwargs)

    signal_input = dict()

    if 'futures_data_dictionary' in kwargs.keys():
        signal_input['futures_data_dictionary'] = kwargs['futures_data_dictionary']

    if 'date_to' in kwargs.keys():
        date_to = kwargs['date_to']
    else:
        date_to = exp.doubledate_shift_bus_days()

    if 'datetime5_years_ago' in kwargs.keys():
        signal_input['datetime5_years_ago'] = kwargs['datetime5_years_ago']

    if 'strategy_info_output' in kwargs.keys():
        strategy_info_output = kwargs['strategy_info_output']
    else:
        strategy_info_output =  ts.get_strategy_info_from_alias(**kwargs)


    strategy_info_dict = sc.convert_from_string_to_dictionary(string_input=strategy_info_output['description_string'])

    if 'con' not in kwargs.keys():
        con.close()

    strategy_class = strategy_info_dict['strategy_class']

    if strategy_class=='futures_butterfly':
        QF_initial = strategy_info_dict['QF']
        z1_initial = strategy_info_dict['z1']

        bf_signals_output = fs.get_futures_butterfly_signals(ticker_list=[strategy_info_dict['ticker1'],
                                                                          strategy_info_dict['ticker2'],
                                                                          strategy_info_dict['ticker3']],
                                          aggregation_method=int(strategy_info_dict['agg']),
                                          contracts_back=int(strategy_info_dict['cBack']),
                                          date_to=date_to,**signal_input)

        aligned_output = bf_signals_output['aligned_output']
        current_data = aligned_output['current_data']

        result_output = {'success': True,
                        'QF_initial':float(QF_initial),'z1_initial': float(z1_initial),
                        'QF': bf_signals_output['qf'],'z1': bf_signals_output['zscore1'],
                        'short_tr_dte': current_data['c1']['tr_dte'],
                        'holding_tr_dte': int(strategy_info_dict['trDte1'])-current_data['c1']['tr_dte'],
                        'second_spread_weight': bf_signals_output['second_spread_weight_1']}

    else:
        result_output = {'success': False}

    return result_output



