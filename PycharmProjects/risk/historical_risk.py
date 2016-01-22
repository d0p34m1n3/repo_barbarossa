
import contract_utilities.expiration as exp
import pandas as pd
import ta.strategy as ts
import my_sql_routines.my_sql_utilities as msu
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import opportunity_constructs.utilities as opUtil
import opportunity_constructs.constants as const
import shared.statistics as stats

def get_historical_risk_4strategy(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)

    alias = kwargs['alias']

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    if 'datetime5_years_ago' in kwargs.keys():
        datetime5_years_ago = kwargs['datetime5_years_ago']
    else:
        date5_years_ago = cu.doubledate_shift(as_of_date,5*365)
        datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    net_position = ts.get_net_position_4strategy_alias(alias=alias,con=con)

    amcb_output = [opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(x)) for x in net_position['ticker']]

    aggregation_method = pd.DataFrame(amcb_output)['aggregation_method'].min()

    if aggregation_method==12:
        contracts_back = const.annualContractsBack
    elif aggregation_method==3:
        contracts_back = const.quarterlyContractsBack
    elif aggregation_method==1:
        contracts_back = const.monthlyContractsBack

    #feeding futures_data_dict might speed things up
    aligned_output = opUtil.get_aligned_futures_data(contract_list = net_position['ticker'].values,
                                    aggregation_method=aggregation_method,
                                    contracts_back=contracts_back,date_to=as_of_date)

    aligned_data = aligned_output['aligned_data']

    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago
    data_last5_years = aligned_data[last5_years_indx]

    contract_multiplier_list = [cmi.contract_multiplier[cmi.get_contract_specs(x)['ticker_head']] for x in net_position['ticker']]

    pnl_5_change = sum([contract_multiplier_list[x]*
           net_position['qty'][x]*
           data_last5_years['c' + str(x+1)]['change_5'] for x in range(len(net_position.index))])

    percentile_vector = stats.get_number_from_quantile(y=pnl_5_change.values,
                                                       quantile_list=[1, 15],
                                                       clean_num_obs=max(100, round(3*len(pnl_5_change.values)/4)))

    downside = (percentile_vector[0]+percentile_vector[1])/2

    return downside