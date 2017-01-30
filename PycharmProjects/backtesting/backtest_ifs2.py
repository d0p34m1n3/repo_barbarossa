
import ta.strategy as ts
import numpy as np
import opportunity_constructs.intraday_future_spreads as ifs
import contract_utilities.contract_meta_info as cmi
import signals.utils as sutil
import signals.intraday_machine_learner as iml
import contract_utilities.expiration as exp


def backtest_ifs2_4date(**kwargs):

    report_date = kwargs['report_date']
    backtest_date = exp.doubledate_shift_bus_days(double_date=report_date,shift_in_days=-1)

    output_dir = ts.create_strategy_output_dir(strategy_class='ifs', report_date=report_date)

    sheet_output = ifs.generate_ifs_sheet_4date(date_to=report_date)
    intraday_spreads = sheet_output['intraday_spreads']

    intraday_spreads.sort(['spread_description','min_volume'],ascending=[True, False],inplace=True)
    intraday_spreads.drop_duplicates('spread_description',inplace=True)
    intraday_spreads.reset_index(drop=True,inplace=True)

    intraday_spreads['pnl'] = np.nan
    intraday_spreads['num_trades'] = np.nan
    intraday_spreads['mean_holding_period'] = np.nan

    signal_name = 'ma40_spread'

    for i in range(len(intraday_spreads.index)):

        ticker_list = [intraday_spreads.iloc[i]['contract1'],intraday_spreads.iloc[i]['contract2'],intraday_spreads.iloc[i]['contract3']]
        ticker_list = [x for x in ticker_list if x is not None]
        ticker_head_list = [cmi.get_contract_specs(x)['ticker_head'] for x in ticker_list]
        num_contracts = len(ticker_list)
        weights_output = sutil.get_spread_weights_4contract_list(ticker_head_list=ticker_head_list)
        contract_multiplier_list = [cmi.contract_multiplier[x] for x in ticker_head_list]
        spread_weights = weights_output['spread_weights']

        intraday_data = iml.get_intraday_data_4spread(ticker_list=ticker_list,date_to=backtest_date,num_days_back=0,spread_weights=spread_weights)

        intraday_data = intraday_data[intraday_data['hour_minute'] > 930]
        pnl_list = []
        holding_period_list = []
        current_position = 0

        for j in range(len(intraday_data.index)):

            if (current_position == 0) & (intraday_data[signal_name].iloc[j]<intraday_spreads['maSpreadLowL'].iloc[i]):
                current_position = 1
                entry_point = j
                entry_price = intraday_data['spread'].iloc[j]
            elif (current_position == 0) & (intraday_data[signal_name].iloc[j]>intraday_spreads['maSpreadHighL'].iloc[i]):
                current_position = -1
                entry_point = j
                entry_price = intraday_data['spread'].iloc[j]
            elif (current_position == 1) & ((intraday_data[signal_name].iloc[j]>0)|(j==len(intraday_data.index)-1)):
                current_position = 0
                exit_price = intraday_data['spread'].iloc[j]
                pnl_list.append(contract_multiplier_list[0]*(exit_price-entry_price)/spread_weights[0]-2*num_contracts)
                holding_period_list.append(j-entry_point)
            elif (current_position == -1) & ((intraday_data[signal_name].iloc[j]<0)|(j==len(intraday_data.index)-1)):
                current_position = 0
                exit_price = intraday_data['spread'].iloc[j]
                pnl_list.append(contract_multiplier_list[0]*(entry_price-exit_price)/spread_weights[0]-2*num_contracts)
                holding_period_list.append(j-entry_point)

        intraday_spreads['pnl'].iloc[i] = sum(pnl_list)
        intraday_spreads['num_trades'].iloc[i] = len(pnl_list)
        intraday_spreads['mean_holding_period'].iloc[i] = np.mean(holding_period_list)

    return intraday_spreads



