

import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import contract_utilities.contract_meta_info as cmi


def get_results_tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    data_out = gfp.get_futures_price_preloaded(ticker_head=ticker_head)

    unique_settle_dates = data_out['settle_date'].unique()
    unique_settle_dates = unique_settle_dates[-2500:]

    data_out = data_out[data_out['tr_dte']>30]
    data_out.sort(['settle_date','tr_dte'],ascending=[True,True],inplace=True)
    data_out.drop_duplicates(subset='settle_date',take_last=False,inplace=True)

    sharp20_list = []
    sharp40_list = []
    sharp80_list = []
    sharp160_list = []

    change1_list = []
    change2_list = []
    change5_list = []
    change10_list = []
    change20_list = []

    for i in range(len(unique_settle_dates)):

        historical_data = data_out[data_out['settle_date']<=unique_settle_dates[i]]

        sharp20_list.append(16*np.mean(historical_data['change_1'].iloc[-20:])/np.std(historical_data['change_1'].iloc[-20:]))
        sharp40_list.append(16*np.mean(historical_data['change_1'].iloc[-40:])/np.std(historical_data['change_1'].iloc[-40:]))
        sharp80_list.append(16*np.mean(historical_data['change_1'].iloc[-80:])/np.std(historical_data['change_1'].iloc[-80:]))
        sharp160_list.append(16*np.mean(historical_data['change_1'].iloc[160:])/np.std(historical_data['change_1'].iloc[-160:]))

        change1_list.append(historical_data['change1'].iloc[-1]/np.std(historical_data['change_1'].iloc[-20:]))
        change2_list.append(historical_data['change2'].iloc[-1]/np.std(historical_data['change_1'].iloc[-20:]))
        change5_list.append(historical_data['change5'].iloc[-1]/np.std(historical_data['change_1'].iloc[-20:]))
        change10_list.append(historical_data['change10'].iloc[-1]/np.std(historical_data['change_1'].iloc[-20:]))
        change20_list.append(historical_data['change20'].iloc[-1]/np.std(historical_data['change_1'].iloc[-20:]))

    output_frame = pd.DataFrame.from_items([('settle_date', unique_settle_dates),
                                            ('sharp20', sharp20_list),('sharp40', sharp40_list),
                                            ('sharp80', sharp80_list),('sharp160', sharp160_list),
                                            ('change1', change1_list),
                                            ('change2', change2_list),
                                            ('change5', change5_list),
                                            ('change10', change10_list),
                                            ('change20', change20_list)])

    return output_frame


def get_portfolio_results(**kwargs):

    tickerhead_list = list(set(cmi.futures_butterfly_strategy_tickerhead_list + cmi.cme_futures_tickerhead_list))

    result_dictionary = {x:get_results_tickerhead(ticker_head=x) for x in tickerhead_list[:3]}
    return result_dictionary


