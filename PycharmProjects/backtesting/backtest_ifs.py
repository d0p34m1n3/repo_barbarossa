
import opportunity_constructs.intraday_future_spreads as ifs
import opportunity_constructs.utilities as opUtil
import contract_utilities.contract_meta_info as cmi
import contract_utilities.expiration as exp
import signals.utils as sutil
import datetime as dt
import numpy as np
import pandas as pd
import ta.strategy as ts
import os.path


def backtest_ifs_4date(**kwargs):

    report_date = kwargs['report_date']

    output_dir = ts.create_strategy_output_dir(strategy_class='ifs', report_date=report_date)

    #if os.path.isfile(output_dir + '/backtest_results.pkl'):
    #    intraday_spreads = pd.read_pickle(output_dir + '/backtest_results.pkl')
    #    return intraday_spreads

    start_hour = dt.time(9, 0, 0, 0)
    end_hour = dt.time(12, 55, 0, 0)

    sheet_output = ifs.generate_ifs_sheet_4date(date_to=report_date)
    intraday_spreads = sheet_output['intraday_spreads']

    intraday_spreads['pnl1'] = 0
    intraday_spreads['pnl2'] = 0
    intraday_spreads['pnl5'] = 0
    intraday_spreads['pnl6'] = 0

    intraday_spreads['pnl1_wc'] = 0
    intraday_spreads['pnl2_wc'] = 0
    intraday_spreads['pnl5_wc'] = 0
    intraday_spreads['pnl6_wc'] = 0

    intraday_spreads['report_date'] = report_date

    intraday_spreads['spread_description'] = intraday_spreads.apply(lambda x: x['ticker_head1']+ '_' +x['ticker_head2'] if x['ticker_head3'] is None else x['ticker_head1']+ '_' +x['ticker_head2'] + '_' + x['ticker_head3'] ,axis=1)
    intraday_spreads['min_volume'] = intraday_spreads.apply(lambda x: min(x['volume1'],x['volume2']) if x['ticker_head3'] is None else min(x['volume1'],x['volume2'],x['volume3']) ,axis=1)

    intraday_spreads.sort(['spread_description','min_volume'],ascending=[True, False],inplace=True)
    intraday_spreads.drop_duplicates('spread_description',inplace=True)
    intraday_spreads.reset_index(drop=True,inplace=True)

    date_list = [exp.doubledate_shift_bus_days(double_date=report_date, shift_in_days=x) for x in [-1,-2]]

    for i in range(len(intraday_spreads.index)):

        #print(i)

        ticker_list = [intraday_spreads.iloc[i]['contract1'],intraday_spreads.iloc[i]['contract2'],intraday_spreads.iloc[i]['contract3']]
        ticker_list = [x for x in ticker_list if x is not None]
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
        num_contracts = len(ticker_list)
        weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
        contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]
        spread_weights = weights_output['spread_weights']

        intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=ticker_list,
                                       date_list=date_list)
        intraday_data['spread'] = 0

        for j in range(num_contracts):

            intraday_data['c' + str(j+1), 'mid_p'] = (intraday_data['c' + str(j+1)]['best_bid_p'] +
                                         intraday_data['c' + str(j+1)]['best_ask_p'])/2

            intraday_data['spread'] = intraday_data['spread']+intraday_data['c' + str(j+1)]['mid_p']*spread_weights[j]

        selection_indx = [x for x in range(len(intraday_data.index)) if
                          (intraday_data.index[x].to_datetime().time() < end_hour)
                          and(intraday_data.index[x].to_datetime().time() >= start_hour)]

        intraday_data = intraday_data.iloc[selection_indx]

        intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
        intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())
        unique_settle_dates = intraday_data['settle_date'].unique()
        intraday_data['spread1'] = np.nan

        if len(unique_settle_dates)<2:
            continue

        if (intraday_data['settle_date'] == unique_settle_dates[0]).sum() == (intraday_data['settle_date'] == unique_settle_dates[1]).sum():
            intraday_data.loc[intraday_data['settle_date'] == unique_settle_dates[0],'spread1'] = intraday_data['spread'][intraday_data['settle_date'] == unique_settle_dates[1]].values
        else:
            continue

        intraday_data = intraday_data[intraday_data['spread1'].notnull()]
        intraday_data['spread_diff'] = contract_multiplier_list[0]*(intraday_data['spread1']-intraday_data['spread'])/spread_weights[0]

        mean5 = intraday_spreads.iloc[i]['mean']
        std5 = intraday_spreads.iloc[i]['std']

        mean1 = intraday_spreads.iloc[i]['mean1']
        std1 = intraday_spreads.iloc[i]['std1']

        mean2 = intraday_spreads.iloc[i]['mean2']
        std2 = intraday_spreads.iloc[i]['std2']

        long_qty = -5000/intraday_spreads.iloc[i]['downside']
        short_qty = -5000/intraday_spreads.iloc[i]['upside']

        intraday_data['z5'] = (intraday_data['spread']-mean5)/std5
        intraday_data['z1'] = (intraday_data['spread']-mean1)/std1
        intraday_data['z2'] = (intraday_data['spread']-mean2)/std2
        intraday_data['z6'] = (intraday_data['spread']-mean1)/std5

        intraday_data11 = intraday_data[intraday_data['z1']>1]
        intraday_data1_1 = intraday_data[intraday_data['z1']<-1]

        if intraday_data1_1.empty:
            pnl1_1 = 0
            pnl1_1_wc = 0
        else:
            pnl1_1 = long_qty*intraday_data1_1['spread_diff'].mean()
            pnl1_1_wc = pnl1_1 - 2*2*long_qty*num_contracts

        if intraday_data11.empty:
            pnl11 = 0
            pnl11_wc = 0
        else:
            pnl11 = short_qty*intraday_data11['spread_diff'].mean()
            pnl11_wc = pnl11 + 2*2*short_qty*num_contracts

        intraday_data21 = intraday_data[intraday_data['z2']>1]
        intraday_data2_1 = intraday_data[intraday_data['z2']<-1]

        if intraday_data2_1.empty:
            pnl2_1 = 0
            pnl2_1_wc = 0
        else:
            pnl2_1 = long_qty*intraday_data2_1['spread_diff'].mean()
            pnl2_1_wc = pnl2_1 - 2*2*long_qty*num_contracts

        if intraday_data21.empty:
            pnl21 = 0
            pnl21_wc = 0
        else:
            pnl21 = short_qty*intraday_data21['spread_diff'].mean()
            pnl21_wc = pnl21 + 2*2*short_qty*num_contracts

        intraday_data51 = intraday_data[intraday_data['z5']>1]
        intraday_data5_1 = intraday_data[intraday_data['z5']<-1]

        if intraday_data5_1.empty:
            pnl5_1 = 0
            pnl5_1_wc = 0
        else:
            pnl5_1 = long_qty*intraday_data5_1['spread_diff'].mean()
            pnl5_1_wc = pnl5_1 - 2*2*long_qty*num_contracts

        if intraday_data51.empty:
            pnl51 = 0
            pnl51_wc = 0
        else:
            pnl51 = short_qty*intraday_data51['spread_diff'].mean()
            pnl51_wc = pnl51 + 2*2*short_qty*num_contracts

        intraday_data61 = intraday_data[intraday_data['z6']>0.25]
        intraday_data6_1 = intraday_data[intraday_data['z6']<-0.25]

        if intraday_data6_1.empty:
            pnl6_1 = 0
            pnl6_1_wc = 0
        else:
            pnl6_1 = long_qty*intraday_data6_1['spread_diff'].mean()
            pnl6_1_wc = pnl6_1 - 2*2*long_qty*num_contracts

        if intraday_data61.empty:
            pnl61 = 0
            pnl61_wc = 0
        else:
            pnl61 = short_qty*intraday_data61['spread_diff'].mean()
            pnl61_wc = pnl61 + 2*2*short_qty*num_contracts

        intraday_spreads['pnl1'].iloc[i] = pnl1_1 + pnl11
        intraday_spreads['pnl2'].iloc[i] = pnl2_1 + pnl21
        intraday_spreads['pnl5'].iloc[i] = pnl5_1 + pnl51
        intraday_spreads['pnl6'].iloc[i] = pnl6_1 + pnl61

        intraday_spreads['pnl1_wc'].iloc[i] = pnl1_1_wc + pnl11_wc
        intraday_spreads['pnl2_wc'].iloc[i] = pnl2_1_wc + pnl21_wc
        intraday_spreads['pnl5_wc'].iloc[i] = pnl5_1_wc + pnl51_wc
        intraday_spreads['pnl6_wc'].iloc[i] = pnl6_1_wc + pnl61_wc

    intraday_spreads.to_pickle(output_dir + '/backtest_results.pkl')
    return intraday_spreads






