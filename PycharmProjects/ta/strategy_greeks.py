
import my_sql_routines.my_sql_utilities as msu
import ta.strategy as tas
import contract_utilities.expiration as exp
import contract_utilities.contract_meta_info as cmi
import get_price.get_options_price as gop
import pandas as pd


def get_greeks_4strategy_4date(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    alias = kwargs['alias']

    if 'as_of_date' in kwargs.keys():
        as_of_date = kwargs['as_of_date']
    else:
        as_of_date = exp.doubledate_shift_bus_days()

    position_frame = tas.get_net_position_4strategy_alias(alias=alias,as_of_date=as_of_date,con=con)
    options_frame = position_frame[position_frame['instrument'] == 'O']

    unique_ticker_list = options_frame['ticker'].unique()
    result_list = []

    contract_specs_output_list = [cmi.get_contract_specs(x) for x in unique_ticker_list]
    contract_multiplier_list = [cmi.contract_multiplier[x['ticker_head']] for x in contract_specs_output_list]

    for i in range(len(unique_ticker_list)):

        skew_output = gop.get_options_price_from_db(ticker=unique_ticker_list[i],
                                                    settle_date=as_of_date,
                                                    column_names=['option_type', 'strike', 'theta', 'vega', 'delta'])

        skew_output.reset_index(drop=True,inplace=True)
        skew_output['delta_diff'] = abs(skew_output['delta']-0.5)
        atm_point = skew_output.loc[skew_output['delta_diff'].idxmin()]
        skew_output.rename(columns={'strike': 'strike_price'}, inplace=True)

        merged_data = pd.merge(options_frame[options_frame['ticker'] == unique_ticker_list[i]], skew_output, how='left', on=['option_type','strike_price'])

        merged_data['oev'] = merged_data['vega']/atm_point['vega']
        merged_data['total_oev'] = merged_data['oev']*merged_data['qty']
        merged_data['dollar_theta'] = merged_data['theta']*merged_data['qty']*contract_multiplier_list[i]

        result_list.append(merged_data)

    strike_portfolio = pd.concat(result_list)

    grouped = strike_portfolio.groupby('ticker')

    ticker_portfolio = pd.DataFrame()
    ticker_portfolio['ticker'] = (grouped['ticker'].first()).values
    ticker_portfolio['total_oev'] = (grouped['total_oev'].sum()).values
    ticker_portfolio['theta'] = (grouped['dollar_theta'].sum()).values

    if 'con' not in kwargs.keys():
        con.close()

    return {'ticker_portfolio': ticker_portfolio, 'strike_portfolio': strike_portfolio }