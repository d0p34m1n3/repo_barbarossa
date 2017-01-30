
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import my_sql_routines.my_sql_utilities as msu
import contract_utilities.expiration as exp
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import shared.statistics as stats
import datetime as dt
import pandas as pd
import numpy as np


def get_ics_signals(**kwargs):

    ticker = kwargs['ticker']
    #print(ticker)
    date_to = kwargs['date_to']
    con = msu.get_my_sql_connection(**kwargs)

    ticker_list = ticker.split('-')
    #print(ticker_list)
    ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
    ticker_class = cmi.ticker_class[ticker_head_list[0]]

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in list(set(ticker_head_list))}

    if 'datetime5_years_ago' in kwargs.keys():
        datetime5_years_ago = kwargs['datetime5_years_ago']
    else:
        date5_years_ago = cu.doubledate_shift(date_to,5*365)
        datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    if 'num_days_back_4intraday' in kwargs.keys():
        num_days_back_4intraday = kwargs['num_days_back_4intraday']
    else:
        num_days_back_4intraday = 5

    tr_dte_list = [exp.get_days2_expiration(ticker=x,date_to=date_to, instrument='futures',con=con)['tr_dte'] for x in ticker_list]

    amcb_output = [opUtil.get_aggregation_method_contracts_back(cmi.get_contract_specs(x)) for x in ticker_list]
    aggregation_method = max([x['aggregation_method'] for x in amcb_output])
    contracts_back = min([x['contracts_back'] for x in amcb_output])
    contract_multiplier = cmi.contract_multiplier[ticker_head_list[0]]

    aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_list,
                                                          tr_dte_list=tr_dte_list,
                                                          aggregation_method=aggregation_method,
                                                          contracts_back=contracts_back,
                                                          date_to=date_to,
                                                          futures_data_dictionary=futures_data_dictionary,
                                                          use_last_as_current=True)

    aligned_data = aligned_output['aligned_data']
    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago
    data_last5_years = aligned_data[last5_years_indx]

    data_last5_years['spread_pnl_1'] = aligned_data['c1']['change_1']-aligned_data['c2']['change_1']

    percentile_vector = stats.get_number_from_quantile(y=data_last5_years['spread_pnl_1'].values,
                                                       quantile_list=[1, 15, 85, 99],
                                                       clean_num_obs=max(100, round(3*len(data_last5_years.index)/4)))

    downside = contract_multiplier*(percentile_vector[0]+percentile_vector[1])/2
    upside = contract_multiplier*(percentile_vector[2]+percentile_vector[3])/2

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to,shift_in_days=x) for x in reversed(range(1,num_days_back_4intraday))]
    date_list.append(date_to)

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=[ticker],
                                       date_list=date_list)

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())

    end_hour = cmi.last_trade_hour_minute[ticker_head_list[0]]
    start_hour = cmi.first_trade_hour_minute[ticker_head_list[0]]

    if ticker_class == 'Ag':
        start_hour1 = dt.time(0, 45, 0, 0)
        end_hour1 = dt.time(7, 45, 0, 0)
        selection_indx = [x for x in range(len(intraday_data.index)) if
                          ((intraday_data['time_stamp'].iloc[x].time() < end_hour1)
                           and(intraday_data['time_stamp'].iloc[x].time() >= start_hour1)) or
                          ((intraday_data['time_stamp'].iloc[x].time() < end_hour)
                           and(intraday_data['time_stamp'].iloc[x].time() >= start_hour))]

    else:
        selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

    intraday_data = intraday_data.iloc[selection_indx]

    intraday_mean5 = np.nan
    intraday_std5 = np.nan

    intraday_mean2 = np.nan
    intraday_std2 = np.nan

    intraday_mean1 = np.nan
    intraday_std1 = np.nan

    if len(intraday_data.index) > 0:

        intraday_data['mid_p'] = (intraday_data['c1']['best_bid_p']+intraday_data['c1']['best_ask_p'])/2

        intraday_mean5 = intraday_data['mid_p'].mean()
        intraday_std5 = intraday_data['mid_p'].std()

        intraday_data_last2days = intraday_data[intraday_data['settle_date'] >= cu.convert_doubledate_2datetime(date_list[-2]).date()]
        intraday_data_yesterday = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-1]).date()]

        intraday_mean2 = intraday_data_last2days['mid_p'].mean()
        intraday_std2 = intraday_data_last2days['mid_p'].std()

        intraday_mean1 = intraday_data_yesterday['mid_p'].mean()
        intraday_std1 = intraday_data_yesterday['mid_p'].std()

    if 'con' not in kwargs.keys():
        con.close()

    return {'downside': downside, 'upside': upside,'front_tr_dte': tr_dte_list[0],
            'intraday_mean5': intraday_mean5,
            'intraday_std5': intraday_std5,
            'intraday_mean2': intraday_mean2,
            'intraday_std2': intraday_std2,
            'intraday_mean1': intraday_mean1,
            'intraday_std1':intraday_std1}