
import opportunity_constructs.intraday_breakouts as ibo
import opportunity_constructs.utilities as opUtil
import signals.intraday_futures_signals as ifs
import contract_utilities.expiration as exp
import shared.calendar_utilities as cu
import contract_utilities.contract_meta_info as cmi
import ta.strategy as ts
import datetime as dt
import math as m
import pandas as pd
import os.path


def construct_ibo_portfolio_4date(**kwargs):
    # date_to

    ibo_output = ibo.generate_ibo_sheet_4date(**kwargs)
    sheet_4date = ibo_output['sheet_4date']
    cov_output = ibo_output['cov_output']
    cov_data_integrity = cov_output['cov_data_integrity']

    if cov_data_integrity < 80:
        return {'success': False}

    cov_matrix = cov_output['cov_matrix']
    sheet_4date['qty'] = sheet_4date['ticker_head'].apply(lambda x: round(1000/m.sqrt(cov_matrix[x][x])))
    sheet_4date['pnl_scaled'] = sheet_4date['pnl']*sheet_4date['qty']

    return {'success': True, 'sheet_4date': sheet_4date}


def accumulated_ibo_performance(**kwargs):

    date_list = exp.get_bus_day_list(date_from=20160915, date_to=kwargs['date_to']) #date_from = 20160701
    result_list = []

    for i in range(len(date_list)):
        print(date_list[i])
        out_frame = construct_ibo_portfolio_4date(date_to=date_list[i])

        if out_frame['success']:
            sheet_4date = out_frame['sheet_4date']
            sheet_4date['trade_date'] = date_list[i]
            result_list.append(sheet_4date)

    merged_frame = pd.concat(result_list)
    return merged_frame


def backtest_continuous_ibo_4ticker(**kwargs):
    # ticker, # date_to

    date_to = kwargs['date_to']
    ticker = kwargs['ticker']

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    ticker_class = cmi.ticker_class[ticker_head]

    signals_output = ifs.get_intraday_trend_signals(**kwargs)
    daily_noise = signals_output['daily_noise']

    date_list = [exp.doubledate_shift_bus_days(double_date=date_to, shift_in_days=x) for x in [-1,-2]]

    intraday_data = opUtil.get_aligned_futures_data_intraday(contract_list=[ticker],
                                       date_list=date_list)

    intraday_data['mid_p'] = (intraday_data['c1']['best_bid_p']+intraday_data['c1']['best_ask_p'])/2

    intraday_data['time_stamp'] = [x.to_datetime() for x in intraday_data.index]
    intraday_data['settle_date'] = intraday_data['time_stamp'].apply(lambda x: x.date())
    intraday_data['hour_minute'] = [100*x.hour+x.minute for x in intraday_data['time_stamp']]

    entry_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-2]).date()]
    exit_data = intraday_data[intraday_data['settle_date'] == cu.convert_doubledate_2datetime(date_list[-1]).date()]

    exit_morning_data = exit_data[(exit_data['hour_minute'] >= 930)&(exit_data['hour_minute'] <= 1000)]
    exit_afternoon_data = exit_data[(exit_data['hour_minute'] >= 1230)&(exit_data['hour_minute'] <= 1300)]

    end_hour = cmi.last_trade_hour_minute[ticker_head]
    start_hour = cmi.first_trade_hour_minute[ticker_head]

    if ticker_class in ['Ag']:
        start_hour1 = dt.time(0, 45, 0, 0)
        end_hour1 = dt.time(7, 45, 0, 0)
        selection_indx = [x for x in range(len(entry_data.index)) if
                          ((entry_data['time_stamp'].iloc[x].time() < end_hour1)
                           and(entry_data['time_stamp'].iloc[x].time() >= start_hour1)) or
                          ((entry_data['time_stamp'].iloc[x].time() < end_hour)
                           and(entry_data['time_stamp'].iloc[x].time() >= start_hour))]

    else:
        selection_indx = [x for x in range(len(entry_data.index)) if
                          (entry_data.index[x].to_datetime().time() < end_hour)
                          and(entry_data.index[x].to_datetime().time() >= start_hour)]

    entry_data = entry_data.iloc[selection_indx]

    entry_data.reset_index(inplace=True,drop=True)

    mean5 = signals_output['intraday_mean5']
    std5 = signals_output['intraday_std5']

    mean2 = signals_output['intraday_mean2']
    std2 = signals_output['intraday_std2']

    mean1 = signals_output['intraday_mean1']
    std1 = signals_output['intraday_std1']

    entry_data['z5'] = (entry_data['mid_p']-mean5)/std5
    entry_data['z1'] = (entry_data['mid_p']-mean1)/std1
    entry_data['z2'] = (entry_data['mid_p']-mean2)/std2
    entry_data['z6'] = (entry_data['mid_p']-mean1)/std5

    entry_data_shifted60 = entry_data.shift(-60)
    entry_data['mid_p_shifted'] = entry_data_shifted60['mid_p']
    entry_data['delta60'] = (entry_data['mid_p_shifted']-entry_data['mid_p'])/daily_noise

    entry_data_shifted15 = entry_data.shift(-15)
    entry_data['mid_p_shifted'] = entry_data_shifted15['mid_p']
    entry_data['delta15'] = (entry_data['mid_p_shifted']-entry_data['mid_p'])/daily_noise

    entry_data_shifted_10 = entry_data.shift(10)
    entry_data['mid_p_shifted'] = entry_data_shifted_10['mid_p']
    entry_data['delta_10'] = (entry_data['mid_p']-entry_data['mid_p_shifted'])/daily_noise

    entry_data_shifted_60 = entry_data.shift(60)
    entry_data['mid_p_shifted'] = entry_data_shifted_60['mid_p']
    entry_data['delta_60'] = (entry_data['mid_p']-entry_data['mid_p_shifted'])/daily_noise

    entry_data_shifted_120 = entry_data.shift(120)
    entry_data['mid_p_shifted'] = entry_data_shifted_120['mid_p']
    entry_data['delta_120'] = (entry_data['mid_p']-entry_data['mid_p_shifted'])/daily_noise

    entry_data_shifted_180 = entry_data.shift(180)
    entry_data['mid_p_shifted'] = entry_data_shifted_180['mid_p']
    entry_data['delta_180'] = (entry_data['mid_p']-entry_data['mid_p_shifted'])/daily_noise

    entry_data['delta_morning'] = (exit_morning_data['mid_p'].mean() - entry_data['mid_p'])/daily_noise
    entry_data['delta_afternoon'] = (exit_afternoon_data['mid_p'].mean() - entry_data['mid_p'])/daily_noise

    entry_data['ewma10_50_spread'] = signals_output['ewma10_50_spread']
    entry_data['ewma20_100_spread'] = signals_output['ewma20_100_spread']

    return entry_data[entry_data['hour_minute']>=930]


def backtest_continuous_ibo_4date(**kwargs):

    date_to=kwargs['date_to']

    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=date_to)

    if os.path.isfile(output_dir + '/backtest_results_cont.pkl'):
        backtest_results = pd.read_pickle(output_dir + '/backtest_results_cont.pkl')
        return backtest_results

    ibo_output = ibo.generate_ibo_sheet_4date(**kwargs)
    sheet_4date = ibo_output['sheet_4date']

    backtest_results_list = []

    for i in range(len(sheet_4date.index)):

        backtest_resul4_4ticker = backtest_continuous_ibo_4ticker(ticker=sheet_4date['ticker'].iloc[i],date_to=date_to)
        backtest_resul4_4ticker['ticker_head'] = sheet_4date['ticker_head'].iloc[i]
        backtest_resul4_4ticker['ticker'] = sheet_4date['ticker'].iloc[i]

        backtest_results_list.append(backtest_resul4_4ticker)

    backtest_results = pd.concat(backtest_results_list)
    backtest_results['report_date'] = date_to

    backtest_results.to_pickle(output_dir + '/backtest_results_cont.pkl')
    return backtest_results







