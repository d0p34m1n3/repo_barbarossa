
import my_sql_routines.my_sql_utilities as msu
import ta.strategy as ts
import ta.strategy_followup as sf
import shared.converters as sc
import pandas as pd

def generate_futures_butterfly_followup_report(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    strategy_frame = ts.get_open_strategies(**kwargs)

    strategy_class_list = [sc.convert_from_string_to_dictionary(string_input=strategy_frame['description_string'][x])['strategy_class']
                           for x in range(len(strategy_frame.index))]

    futures_butterfly_indx = [x=='futures_butterfly' for x in strategy_class_list]

    futures_butterfly_frame = strategy_frame[futures_butterfly_indx]

    results = [sf.get_results_4strategy(alias=futures_butterfly_frame['alias'].iloc[x],
                                        strategy_info_output=futures_butterfly_frame.iloc[x])
               for x in range(len(futures_butterfly_frame.index))]

    butterfly_followup_frame = pd.DataFrame(results)
    butterfly_followup_frame['alias'] = futures_butterfly_frame['alias'].values
    butterfly_followup_frame = butterfly_followup_frame[['alias','holding_tr_dte','short_tr_dte','z1_initial','z1','QF_initial','QF']]
    butterfly_followup_frame.sort('QF',ascending=False,inplace=True)

    butterfly_followup_frame['z1'] = butterfly_followup_frame['z1'].round(2)

    if 'con' not in kwargs.keys():
        con.close()

    return butterfly_followup_frame

