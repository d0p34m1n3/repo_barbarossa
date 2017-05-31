
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import statsmodels.api as sm
import fundamental_data.cot_data as cot
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import datetime as dt
pd.options.mode.chained_assignment = None


def get_cot_strategy_signals(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']
    target_noise = 5000

    #print(ticker)

    contract_specs_out = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_out['ticker_head']
    ticker_class = contract_specs_out['ticker_class']

    data_out = gfp.get_futures_price_preloaded(ticker=ticker)

    data_out['change_5'] = data_out['close_price']-data_out['close_price'].shift(5)
    data_out['change_10'] = data_out['close_price']-data_out['close_price'].shift(10)
    data_out['change_20'] = data_out['close_price']-data_out['close_price'].shift(20)
    data_out['change_40'] = data_out['close_price']-data_out['close_price'].shift(40)
    data_out['change_60'] = data_out['close_price']-data_out['close_price'].shift(60)

    datetime_to = cu.convert_doubledate_2datetime(date_to)

    data_out = data_out[data_out['settle_date']<=datetime_to]

    data_out_current = data_out[data_out['settle_date']==datetime_to]

    daily_noise = np.std(data_out['change_1'].iloc[-60:])
    average_volume = np.mean(data_out['volume'].iloc[-60:])

    change5 = data_out_current['change5'].iloc[0]
    change10 = data_out_current['change10'].iloc[0]
    change20 = data_out_current['change20'].iloc[0]

    normalized_pnl_5 = change5*target_noise/daily_noise
    normalized_pnl_10 = change10*target_noise/daily_noise
    normalized_pnl_20 = change20*target_noise/daily_noise

    change_5_normalized = data_out_current['change_5'].iloc[0]/daily_noise
    change_10_normalized = data_out_current['change_10'].iloc[0]/daily_noise
    change_20_normalized = data_out_current['change_20'].iloc[0]/daily_noise
    change_40_normalized = data_out_current['change_40'].iloc[0]/daily_noise
    change_60_normalized = data_out_current['change_60'].iloc[0]/daily_noise

    volume_5_normalized = np.mean(data_out['volume'].iloc[-5:])/average_volume
    volume_10_normalized = np.mean(data_out['volume'].iloc[-10:])/average_volume
    volume_20_normalized = np.mean(data_out['volume'].iloc[-20:])/average_volume

    cot_output = cot.get_cot_data(ticker_head=ticker_head,date_to=date_to)

    if ticker_class in ['FX','STIR','Index','Treasury']:

        cot_output['comm_net'] = cot_output['Dealer Longs']-cot_output['Dealer Shorts']
        cot_output['spec_net'] = cot_output['Asset Manager Longs']-cot_output['Asset Manager Shorts']+cot_output['Leveraged Funds Longs']-cot_output['Leveraged Funds Shorts']
    else:
        cot_output['comm_net'] = cot_output['Producer/Merchant/Processor/User Longs']-cot_output['Producer/Merchant/Processor/User Shorts']
        cot_output['spec_net'] = cot_output['Money Manager Longs']-cot_output['Money Manager Shorts']

    cot_output['comm_net_change_1'] = cot_output['comm_net']-cot_output['comm_net'].shift(1)
    cot_output['comm_net_change_2'] = cot_output['comm_net']-cot_output['comm_net'].shift(2)
    cot_output['comm_net_change_4'] = cot_output['comm_net']-cot_output['comm_net'].shift(4)

    cot_output['spec_net_change_1'] = cot_output['spec_net']-cot_output['spec_net'].shift(1)
    cot_output['spec_net_change_2'] = cot_output['spec_net']-cot_output['spec_net'].shift(2)
    cot_output['spec_net_change_4'] = cot_output['spec_net']-cot_output['spec_net'].shift(4)

    comm_net_change_1_avg = np.std(cot_output['comm_net_change_1'].iloc[-150:])
    comm_net_change_2_avg = np.std(cot_output['comm_net_change_2'].iloc[-150:])
    comm_net_change_4_avg = np.std(cot_output['comm_net_change_4'].iloc[-150:])
    spec_net_change_1_avg = np.std(cot_output['spec_net_change_1'].iloc[-150:])
    spec_net_change_2_avg = np.std(cot_output['spec_net_change_2'].iloc[-150:])
    spec_net_change_4_avg = np.std(cot_output['spec_net_change_4'].iloc[-150:])

    comm_net_change_1_normalized = cot_output['comm_net_change_1'].iloc[-1]/comm_net_change_1_avg
    comm_net_change_2_normalized = cot_output['comm_net_change_2'].iloc[-1]/comm_net_change_2_avg
    comm_net_change_4_normalized = cot_output['comm_net_change_4'].iloc[-1]/comm_net_change_4_avg

    spec_net_change_1_normalized = cot_output['spec_net_change_1'].iloc[-1]/spec_net_change_1_avg
    spec_net_change_2_normalized = cot_output['spec_net_change_2'].iloc[-1]/spec_net_change_2_avg
    spec_net_change_4_normalized = cot_output['spec_net_change_4'].iloc[-1]/spec_net_change_4_avg

    comm_z = (cot_output['comm_net'].iloc[-1]-np.mean(cot_output['comm_net'].iloc[-150:]))/np.std(cot_output['comm_net'].iloc[-150:])
    spec_z = (cot_output['spec_net'].iloc[-1]-np.mean(cot_output['spec_net'].iloc[-150:]))/np.std(cot_output['spec_net'].iloc[-150:])

    return {'success': True,
            'change_5_normalized': change_5_normalized,
            'change_10_normalized': change_10_normalized,
            'change_20_normalized': change_20_normalized,
            'change_40_normalized': change_40_normalized,
            'change_60_normalized': change_60_normalized,
            'volume_5_normalized': volume_5_normalized,
            'volume_10_normalized': volume_10_normalized,
            'volume_20_normalized': volume_20_normalized,
            'comm_net_change_1_normalized': comm_net_change_1_normalized,
            'comm_net_change_2_normalized': comm_net_change_2_normalized,
            'comm_net_change_4_normalized': comm_net_change_4_normalized,
            'spec_net_change_1_normalized': spec_net_change_1_normalized,
            'spec_net_change_2_normalized': spec_net_change_2_normalized,
            'spec_net_change_4_normalized': spec_net_change_4_normalized,
            'comm_z': comm_z,'spec_z':spec_z,
            'normalized_pnl_5': normalized_pnl_5,
            'normalized_pnl_10': normalized_pnl_10,
            'normalized_pnl_20': normalized_pnl_20}


def get_arma_signals(**kwargs):

    ticker_head = kwargs['ticker_head']
    date_to = kwargs['date_to']

    date2_years_ago = cu.doubledate_shift(date_to, 2*365)

    panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head,settle_date_from=date2_years_ago,settle_date_to=date_to)
    panel_data = panel_data[panel_data['tr_dte']>=40]
    panel_data.sort(['settle_date','tr_dte'],ascending=[True,True],inplace=True)
    rolling_data = panel_data.drop_duplicates(subset=['settle_date'], take_last=False)
    rolling_data['percent_change'] = 100*rolling_data['change_1']/rolling_data['close_price']
    rolling_data = rolling_data[rolling_data['percent_change'].notnull()]

    data_input = np.array(rolling_data['percent_change'])

    daily_noise = np.std(data_input)

    param1_list = []
    param2_list = []
    akaike_list = []
    forecast_list = []

    for i in range(0, 3):
        for j in range(0, 3):
            try:
                model_output = sm.tsa.ARMA(data_input, (i,j)).fit()
            except:
                continue

            param1_list.append(i)
            param2_list.append(j)
            akaike_list.append(model_output.aic)
            forecast_list.append(model_output.predict(len(data_input),len(data_input))[0])

    result_frame = pd.DataFrame.from_items([('param1', param1_list),
                             ('param2', param2_list),
                             ('akaike', akaike_list),
                             ('forecast', forecast_list)])

    result_frame.sort('akaike',ascending=True,inplace=True)

    param1 = result_frame['param1'].iloc[0]
    param2 = result_frame['param2'].iloc[0]

    if (param1 == 0)&(param2 == 0):
        forecast = np.nan
    else:
        forecast = result_frame['forecast'].iloc[0]

    return {'success': True, 'forecast':forecast,'normalized_forecast': forecast/daily_noise,
            'param1':param1,'param2':param2,
            'normalized_target': 100*(rolling_data['change1_instant'].iloc[-1]/rolling_data['close_price'].iloc[-1])/daily_noise}







