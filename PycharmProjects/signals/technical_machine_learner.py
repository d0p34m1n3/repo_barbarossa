
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import statsmodels.api

import get_price.get_futures_price as gfp
import get_price.get_stock_price as gsp
import signals.technical_indicators as ti
import shared.calendar_utilities as cu
import pandas as pd


def get_tms(**kwargs):

    date_to = kwargs['date_to']

    feature_list = ['obv', 'rsi_6', 'rsi_12', 'atr_14', 'mfi_14',
                    'adx_14', 'adx_20', 'rocr_1', 'rocr_3', 'rocr_12', 'cci_12', 'cci_20',
                    'macd_12_26', 'macd_signal_12_26_9', 'macd_hist_12_26_9',
                    'williams_r_10', 'tsf_10_3', 'tsf_20_3', 'trix_15']

    if 'ticker_head' in kwargs.keys():
        ticker_head = kwargs['ticker_head']

        panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head, settle_date_to=date_to,
                                                 settle_date_from=cu.doubledate_shift(date_to, 11 * 365))

        panel_data.rename(columns={'open_price': 'open', 'high_price': 'high', 'low_price': 'low', 'close_price': 'close'},inplace=True)
        panel_data.sort_values(['cont_indx', 'settle_date'], ascending=[True, True], inplace=True)
        unique_cont_indx_list = panel_data['cont_indx'].unique()
        contract_data_list = []

        for i in range(len(unique_cont_indx_list)):

            contract_data = panel_data[panel_data['cont_indx'] == unique_cont_indx_list[i]]

            if len(contract_data.index) < 30:
                continue

            contract_data = calc_tms(contract_data=contract_data)
            contract_data_list.append(contract_data)

        merged_frame = pd.concat(contract_data_list)

        merged_frame.dropna(subset=feature_list, inplace=True)
        merged_frame = merged_frame[merged_frame['cal_dte'] > 30]
        merged_frame.sort_values(['settle_date', 'cont_indx'], ascending=[True, True], inplace=True)
        data_out = merged_frame.drop_duplicates('settle_date', inplace=False, keep='first')

    elif 'stock_ticker' in kwargs.keys():

        contract_data = gsp.get_stock_price_preloaded(ticker='AAPL', settle_date_to=date_to,
                                                      settle_date_from=cu.doubledate_shift(date_to, 11 * 365))

        contract_data = contract_data[['close', 'settle_datetime', 'volume', 'high', 'low', 'split_coefficient']]

        contract_data.reset_index(drop=True,inplace=True)

        split_envents = contract_data[contract_data['split_coefficient'] != 1]
        split_index_list = split_envents.index

        for i in range(len(split_index_list)):
            contract_data['close'].iloc[:split_index_list[i]] = \
                contract_data['close'].iloc[:split_index_list[i]] / contract_data['split_coefficient'].iloc[split_index_list[i]]

            contract_data['high'].iloc[:split_index_list[i]] = \
                contract_data['high'].iloc[:split_index_list[i]] / contract_data['split_coefficient'].iloc[
                    split_index_list[i]]

            contract_data['low'].iloc[:split_index_list[i]] = \
                contract_data['low'].iloc[:split_index_list[i]] / contract_data['split_coefficient'].iloc[
                    split_index_list[i]]

        contract_data.rename(columns={'settle_datetime': 'settle_date'}, inplace=True)
        contract_data['change_1'] = contract_data.close - contract_data.close.shift(1)
        data_out = calc_tms(contract_data=contract_data)
        data_out.dropna(subset=feature_list, inplace=True)

    return data_out


def calc_tms(**kwargs):

    contract_data = kwargs['contract_data']

    contract_data = ti.get_obv(data_frame_input=contract_data, period=10)
    contract_data = ti.rsi(data_frame_input=contract_data, change_field='change_1', period=6)
    contract_data = ti.rsi(data_frame_input=contract_data, change_field='change_1', period=12)
    contract_data = ti.get_atr(data_frame_input=contract_data, period=14)
    contract_data = ti.get_money_flow_index(data_frame_input=contract_data, period=14)
    contract_data = ti.get_adx(data_frame_input=contract_data, period=14)
    contract_data = ti.get_adx(data_frame_input=contract_data, period=20)
    contract_data.drop(['atr_20'], 1, inplace=True)
    contract_data = ti.get_rocr(data_frame_input=contract_data, period=1)
    contract_data = ti.get_rocr(data_frame_input=contract_data, period=3)
    contract_data = ti.get_rocr(data_frame_input=contract_data, period=12)
    contract_data = ti.get_cci(data_frame_input=contract_data, period=12)
    contract_data = ti.get_cci(data_frame_input=contract_data, period=20)
    contract_data = ti.get_macd(data_frame_input=contract_data, period1=12, period2=26, period3=9)
    contract_data = ti.get_williams_r(data_frame_input=contract_data, period=10)
    contract_data = ti.get_time_series_based_return_forecast(data_frame_input=contract_data,
                                                             regression_window=10, forecast_window1=3,
                                                             forecast_window2=5)

    contract_data = ti.get_time_series_based_return_forecast(data_frame_input=contract_data,
                                                             regression_window=20, forecast_window1=3,
                                                             forecast_window2=5)
    contract_data = ti.get_trix(data_frame_input=contract_data, period=15)

    contract_data['target1'] = 0
    contract_data.target1.loc[contract_data.close.shift(-1) > contract_data.close] = 1
    contract_data.target1.loc[contract_data.close.shift(-1) < contract_data.close] = -1

    contract_data['target3'] = 0
    contract_data.target3.loc[contract_data.close.shift(-3) > contract_data.close] = 1
    contract_data.target3.loc[contract_data.close.shift(-3) < contract_data.close] = -1

    contract_data['target5'] = 0
    contract_data.target5.loc[contract_data.close.shift(-5) > contract_data.close] = 1
    contract_data.target5.loc[contract_data.close.shift(-5) < contract_data.close] = -1

    contract_data['change3'] = contract_data.close.shift(-3) - contract_data.close

    return contract_data







