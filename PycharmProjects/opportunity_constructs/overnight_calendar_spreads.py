
import contract_utilities.contract_lists as cl
import contract_utilities.contract_meta_info as cmi
import signals.overnight_calendar_spread_signals as ocss
import get_price.get_futures_price as gfp
import pandas as pd
import os.path
import ta.strategy as ts


def get_overnight_spreads_4date(**kwargs):

    futures_dataframe = cl.generate_futures_list_dataframe(**kwargs)

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
        futures_dataframe = futures_dataframe[futures_dataframe['volume']>volume_filter]

    futures_dataframe.reset_index(drop=True, inplace=True)

    unique_ticker_heads = cmi.futures_butterfly_strategy_tickerhead_list
    tuples = []

    for ticker_head_i in unique_ticker_heads:
        ticker_head_data = futures_dataframe[futures_dataframe['ticker_head'] == ticker_head_i]

        ticker_head_data.sort(['ticker_year','ticker_month'], ascending=[True, True], inplace=True)

        if len(ticker_head_data.index) >= 2:
            tuples = tuples + [(ticker_head_data.index[0], ticker_head_data.index[0], ticker_head_data.index[1])]

        if len(ticker_head_data.index) >= 3:
            tuples = tuples + [(ticker_head_data.index[i-1], ticker_head_data.index[i],ticker_head_data.index[i+1]) for i in range(1, len(ticker_head_data.index)-1)]

    return pd.DataFrame([(futures_dataframe['ticker'][indx[0]],
    futures_dataframe['ticker'][indx[1]],
    futures_dataframe['ticker'][indx[2]],
    futures_dataframe['ticker_head'][indx[0]],
    futures_dataframe['ticker_class'][indx[0]],
    futures_dataframe['tr_dte'][indx[0]],
    futures_dataframe['tr_dte'][indx[1]],
    futures_dataframe['tr_dte'][indx[2]],
    futures_dataframe['multiplier'][indx[0]],
    futures_dataframe['aggregation_method'][indx[0]],
    futures_dataframe['contracts_back'][indx[0]]) for indx in tuples],
                        columns=['ticker1','ticker2','ticker3','tickerHead','tickerClass','trDte1','trDte2','trDte3','multiplier','agg','cBack'])


def generate_overnight_spreads_sheet_4date(**kwargs):

    date_to = kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='ocs', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        overnight_calendars = pd.read_pickle(output_dir + '/summary.pkl')
        return {'overnight_calendars': overnight_calendars,'success': True}

    if 'volume_filter' not in kwargs.keys():
        kwargs['volume_filter'] = 100

    overnight_calendars = get_overnight_spreads_4date(**kwargs)

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in overnight_calendars['tickerHead']}

    num_spreads = len(overnight_calendars.index)
    signals_output = [ocss.get_overnight_calendar_signals(ticker_list=[overnight_calendars.iloc[x]['ticker1'], overnight_calendars.iloc[x]['ticker2']],
                                                          futures_data_dictionary=futures_data_dictionary, date_to=date_to) for x in range(num_spreads)]

    overnight_calendars['ts_slope5'] = [x['ts_slope5'] for x in signals_output]
    overnight_calendars['ts_slope10'] = [x['ts_slope10'] for x in signals_output]
    overnight_calendars['linear_deviation5'] = [x['linear_deviation5'] for x in signals_output]
    overnight_calendars['linear_deviation10'] = [x['linear_deviation10'] for x in signals_output]
    overnight_calendars['momentum5'] = [x['momentum5'] for x in signals_output]
    overnight_calendars['momentum10'] = [x['momentum10'] for x in signals_output]
    overnight_calendars['underlying_zscore'] = [x['underlying_zscore'] for x in signals_output]
    overnight_calendars['q_carry'] = [x['q_carry'] for x in signals_output]
    overnight_calendars['q_carry_average'] = [x['q_carry_average'] for x in signals_output]
    overnight_calendars['reward_risk'] = [x['reward_risk'] for x in signals_output]
    overnight_calendars['spread_price'] = [x['spread_price'] for x in signals_output]
    overnight_calendars['normalized_target'] = [x['normalized_target'] for x in signals_output]
    overnight_calendars['pnl_per_contract'] = [x['pnl_per_contract'] for x in signals_output]
    overnight_calendars['noise_100'] = [x['noise_100'] for x in signals_output]
    overnight_calendars['contract_multiplier'] = [x['contract_multiplier'] for x in signals_output]

    overnight_calendars.to_pickle(output_dir + '/summary.pkl')

    return {'overnight_calendars': overnight_calendars, 'success': True}



