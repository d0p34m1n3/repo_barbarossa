
import my_sql_routines.my_sql_utilities as msu
import ta.strategy as ts
import ta.pnl as tpnl
import shared.converters as sc
import signals.futures_signals as fs
import contract_utilities.expiration as exp
import pandas as pd
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import opportunity_constructs.spread_carry as osc


def get_results_4strategy(**kwargs):

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
        strategy_info_output = ts.get_strategy_info_from_alias(**kwargs)

    strategy_info_dict = sc.convert_from_string_to_dictionary(string_input=strategy_info_output['description_string'])

    strategy_class = strategy_info_dict['strategy_class']

    if strategy_class == 'futures_butterfly':
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

    elif strategy_class == 'spread_carry':
        trades4_strategy = ts.get_trades_4strategy_alias(**kwargs)
        grouped = trades4_strategy.groupby('ticker')
        net_position = pd.DataFrame()
        net_position['ticker'] = (grouped['ticker'].first()).values
        net_position['qty'] = (grouped['trade_quantity'].sum()).values
        net_position = net_position[net_position['qty'] != 0]

        net_position['ticker_head'] = [cmi.get_contract_specs(x)['ticker_head'] for x in net_position['ticker']]
        price_output = [gfp.get_futures_price_preloaded(ticker=x, settle_date=date_to) for x in net_position['ticker']]
        net_position['tr_dte'] = [x['tr_dte'].values[0] for x in price_output]

        results_frame = pd.DataFrame()
        unique_tickerhead_list = net_position['ticker_head'].unique()
        results_frame['tickerHead'] = unique_tickerhead_list
        results_frame['ticker1'] = [None]*len(unique_tickerhead_list)
        results_frame['ticker2'] = [None]*len(unique_tickerhead_list)
        results_frame['qty'] = [None]*len(unique_tickerhead_list)
        results_frame['pnl'] = [None]*len(unique_tickerhead_list)
        results_frame['downside'] = [None]*len(unique_tickerhead_list)
        results_frame['indicator'] = [None]*len(unique_tickerhead_list)
        results_frame['timeHeld'] = [None]*len(unique_tickerhead_list)
        results_frame['recommendation'] = [None]*len(unique_tickerhead_list)

        spread_carry_output = osc.generate_spread_carry_sheet_4date(report_date=date_to)
        spread_report = spread_carry_output['spread_report']

        pnl_output = tpnl.get_strategy_pnl(**kwargs)
        pnl_per_tickerhead = pnl_output['pnl_per_tickerhead']

        for i in range(len(unique_tickerhead_list)):

            net_position_per_tickerhead = net_position[net_position['ticker_head'] == unique_tickerhead_list[i]]
            net_position_per_tickerhead.sort('tr_dte',ascending=True,inplace=True)

            selected_spread = spread_report[(spread_report['ticker1'] == net_position_per_tickerhead['ticker'].values[0]) &
                             (spread_report['ticker2'] == net_position_per_tickerhead['ticker'].values[1])]

            results_frame['ticker1'][i] = selected_spread['ticker1'].values[0]
            results_frame['ticker2'][i] = selected_spread['ticker2'].values[0]
            results_frame['qty'][i] = net_position_per_tickerhead['qty'].values[0]

            selected_trades = trades4_strategy[trades4_strategy['ticker'] == results_frame['ticker1'].values[i]]

            price_output = gfp.get_futures_price_preloaded(ticker=results_frame['ticker1'].values[i],
                                                           settle_date=pd.to_datetime(selected_trades['trade_date'].values[0]))

            results_frame['timeHeld'][i] = price_output['tr_dte'].values[0]-net_position_per_tickerhead['tr_dte'].values[0]
            results_frame['pnl'][i] = pnl_per_tickerhead[unique_tickerhead_list[i]].sum()

            if unique_tickerhead_list[i] in ['CL', 'B', 'ED']:
                results_frame['indicator'][i] = selected_spread['reward_risk'].values[0]

                if results_frame['qty'][i] > 0:
                    results_frame['recommendation'][i] = 'STOP'
                elif results_frame['qty'][i] < 0:
                    if results_frame['indicator'][i] > -0.06:
                        results_frame['recommendation'][i] = 'STOP'
                    else:
                        results_frame['recommendation'][i] = 'HOLD'
            else:

                results_frame['indicator'][i] = selected_spread['q_carry'].values[0]

                if results_frame['qty'][i] > 0:
                    if results_frame['indicator'][i] < 19:
                        results_frame['recommendation'][i] = 'STOP'
                    else:
                        results_frame['recommendation'][i] = 'HOLD'

                elif results_frame['qty'][i] < 0:
                    if results_frame['indicator'][i] > -9:
                        results_frame['recommendation'][i] = 'STOP'
                    else:
                        results_frame['recommendation'][i] = 'HOLD'

            if results_frame['qty'][i] > 0:
                results_frame['downside'][i] = selected_spread['downside'].values[0]*results_frame['qty'][i]
            else:
                results_frame['downside'][i] = selected_spread['upside'].values[0]*results_frame['qty'][i]

        return {'success': True, 'results_frame': results_frame}
    else:
        result_output = {'success': False}

    return result_output



