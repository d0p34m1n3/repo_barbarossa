
import contract_utilities.expiration as exp
import my_sql_routines.my_sql_utilities as msu
import signals.utils as sutil
import shared.calendar_utilities as cu
import pandas as pd
import numpy as np
import contract_utilities.contract_meta_info as cmi
pd.options.mode.chained_assignment = None  # default='warn'
import get_price.get_futures_price as gfp


def get_past_realized_vol_until_expiration(**kwargs):

    ticker = kwargs['ticker']
    contracts_back = kwargs['contracts_back']
    aggregation_method = kwargs['aggregation_method']
    date_to = kwargs['date_to']

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_class = contract_specs_output['ticker_class']
    ticker_head = contract_specs_output['ticker_head']

    con = msu.get_my_sql_connection(**kwargs)

    datetime_to = cu.convert_doubledate_2datetime(date_to)

    exp_output = exp.get_days2_expiration(ticker=ticker, con=con,
                                          instrument='options',
                                          date_to=date_to)

    cal_dte = exp_output['cal_dte']
    expiration_datetime = exp_output['expiration_datetime']

    min_num_obs_4realized_vol_2expiration = np.floor(cal_dte*0.6*250/365)

    expiration_date = int(expiration_datetime.strftime('%Y%m%d'))

    if aggregation_method == 1:
        additional_contracts_2request = round(cal_dte/30) + 5
    else:
        additional_contracts_2request = 10

    report_date_list = sutil.get_bus_dates_from_agg_method_and_contracts_back(ref_date=date_to,
                                                           aggregation_method=aggregation_method,
                                                           contracts_back=contracts_back + additional_contracts_2request,
                                                           shift_bus_days=2)

    exp_date_list = sutil.get_bus_dates_from_agg_method_and_contracts_back(ref_date=expiration_date,
                                                           aggregation_method=aggregation_method,
                                                           contracts_back=contracts_back+ additional_contracts_2request)

    observed_real_vol_date_from = sutil.get_bus_dates_from_agg_method_and_contracts_back(ref_date=date_to,
                                                           aggregation_method=aggregation_method,
                                                           contracts_back=contracts_back + additional_contracts_2request,
                                                           shift_bus_days=-30)

    observed_real_vol_date_to = sutil.get_bus_dates_from_agg_method_and_contracts_back(ref_date=date_to,
                                                           aggregation_method=aggregation_method,
                                                           contracts_back=contracts_back + additional_contracts_2request)

    if 'con' not in kwargs.keys():
        con.close()

    data_frame_out = pd.DataFrame.from_items([('date_from', report_date_list), ('date_to', exp_date_list),
                                              ('observed_real_vol_date_from', observed_real_vol_date_from),
                                              ('observed_real_vol_date_to', observed_real_vol_date_to)])

    selection_indx = data_frame_out['date_to'] < datetime_to
    data_frame_out = data_frame_out[selection_indx]
    data_frame_out = data_frame_out[:contracts_back]
    data_frame_out.reset_index(drop=True, inplace=True)

    data_frame_out['real_vol_till_expiration'] = np.NaN
    data_frame_out['real_vol20'] = np.NaN
    data_frame_out['ticker'] = None

    get_futures_price_input = {}

    if 'futures_data_dictionary' in kwargs.keys():
            get_futures_price_input['futures_data_dictionary'] = kwargs['futures_data_dictionary']

    if ticker_class in ['Index', 'FX', 'Metal', 'Treasury']:

        rolling_price = sutil.get_rolling_futures_price(ticker_head=ticker_head, **get_futures_price_input)
        for i in data_frame_out.index:
            selected_price = rolling_price[(rolling_price['settle_date'] >= data_frame_out['date_from'].loc[i])&
            (rolling_price['settle_date'] <= data_frame_out['date_to'].loc[i])]

            if len(selected_price.index) >= min_num_obs_4realized_vol_2expiration:

                data_frame_out['real_vol_till_expiration'].loc[i] = 100*np.sqrt(252*np.mean(np.square(selected_price['log_return'])))

            selected_price = rolling_price[(rolling_price['settle_date'] >= data_frame_out['observed_real_vol_date_from'].loc[i])&
            (rolling_price['settle_date'] <= data_frame_out['observed_real_vol_date_to'].loc[i])]
            return selected_price['log_return']

            data_frame_out['real_vol20'].loc[i] = 100*np.sqrt(252*np.mean(np.square(selected_price['log_return'][-20:])))


    elif ticker_class in ['Ag', 'Livestock', 'Energy']:

        ticker_list = sutil.get_tickers_from_agg_method_and_contracts_back(ticker=ticker,
                                                             aggregation_method=aggregation_method,
                                                             contracts_back=contracts_back+additional_contracts_2request)

        ticker_list = [ticker_list[x] for x in range(len(ticker_list)) if selection_indx[x]]
        ticker_list = ticker_list[:contracts_back]
        data_frame_out['ticker'] = ticker_list

        for i in data_frame_out.index:
            ticker_data = gfp.get_futures_price_preloaded(ticker=data_frame_out['ticker'].loc[i], **get_futures_price_input)
            ticker_data.sort('settle_date', ascending=True, inplace=True)
            shifted = ticker_data.shift(1)

            ticker_data['log_return'] = np.log(ticker_data['close_price']/shifted['close_price'])

            selected_price = ticker_data[(ticker_data['settle_date'] >= data_frame_out['date_from'].loc[i])&
            (ticker_data['settle_date'] <= data_frame_out['date_to'].loc[i])]

            if len(selected_price.index) >= min_num_obs_4realized_vol_2expiration:

                data_frame_out['real_vol_till_expiration'].loc[i] = 100*np.sqrt(252*np.mean(np.square(selected_price['log_return'])))

    return data_frame_out






