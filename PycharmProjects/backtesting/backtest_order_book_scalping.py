
import contract_utilities.expiration as exp
import signals.order_book_forecasting as obf
import shared.statistics as stats
import contract_utilities.contract_meta_info as cmi
import reformat_intraday_data.reformat_ttapi_intraday_data as rid
import ta.trade_fill_loader as tfl
import statsmodels.api as sm
import pandas as pd


def get_order_book_signals_4date(**kwargs):

    trade_date = kwargs['trade_date']
    ticker = kwargs['ticker']

    calibration_date = exp.doubledate_shift_bus_days(double_date=trade_date, shift_in_days=1)

    trading_data = obf.get_orderbook_signals(ticker=ticker, date_to=trade_date)
    calibration_data = obf.get_orderbook_signals(ticker=ticker, date_to=calibration_date)

    y = calibration_data.loc[:,'target']  # response
    X = calibration_data.loc[:,['voi','voi1','voi2','voi3','voi4','voi5',
                         'oir','oir1','oir2','oir3','oir4','oir5']]  # predictor
    X = sm.add_constant(X)

    olsmod = sm.OLS(y, X)
    olsres = olsmod.fit()
    calibration_data['predict'] = olsres.predict(X)

    pred_levels = stats.get_number_from_quantile(y=calibration_data['predict'],quantile_list=[10, 90])

    X = trading_data.loc[:,['voi','voi1','voi2','voi3','voi4','voi5',
                         'oir','oir1','oir2','oir3','oir4','oir5']]  # predictor
    X = sm.add_constant(X)

    trading_data['predict'] = olsres.predict(X)
    trading_data['bias'] = 0

    trading_data.loc[trading_data['predict']<=pred_levels[0],'bias'] = -1
    trading_data.loc[trading_data['predict']>=pred_levels[1],'bias'] = 1

    return trading_data


def backtest_scalping_4date(**kwargs):

    ticker = kwargs['ticker']
    trade_date = kwargs['trade_date']
    profit_target = kwargs['profit_target']
    stop_loss = kwargs['stop_loss']

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    tick_size = cmi.tick_size[ticker_head]

    data_frame_out = rid.load_csv_file_4ticker(ticker=ticker,folder_date=trade_date)

    best_bid_p = data_frame_out[data_frame_out['field'] == 'BestBidPrice']
    best_ask_p = data_frame_out[data_frame_out['field'] == 'BestAskPrice']
    best_bid_p['value'] = best_bid_p['value'].astype('float64')
    best_ask_p['value'] = best_ask_p['value'].astype('float64')

    best_bid_p['value'] = [tfl.convert_trade_price_from_tt(price=x,ticker_head=ticker_head) for x in best_bid_p['value']]
    best_ask_p['value'] = [tfl.convert_trade_price_from_tt(price=x,ticker_head=ticker_head) for x in best_ask_p['value']]

    snapshot_data = get_order_book_signals_4date(ticker = ticker,trade_date=trade_date)

    position = 0
    entry_list  = []
    exit_list  = []
    entry_time = []
    exit_time = []

    for i in range(len(snapshot_data.index)-1):
    #print(i)
        ask_updates = best_ask_p[(best_ask_p['time']>=snapshot_data.index[i])&(best_ask_p['time']<snapshot_data.index[i+1])]
        bid_updates = best_bid_p[(best_bid_p['time']>=snapshot_data.index[i])&(best_bid_p['time']<snapshot_data.index[i+1])]

        if (position ==0) and snapshot_data['bias'].iloc[i]:

            if ask_updates.empty:
                continue

            ask_market_cross = ask_updates[ask_updates['value']<=snapshot_data['best_bid_p'].iloc[i]]

            if ask_market_cross.empty:
                continue
            entry_list.append(ask_market_cross['value'].iloc[0])
            entry_time.append(ask_market_cross['time'].iloc[0])
            stop_point = ask_market_cross['value'].iloc[0]-stop_loss*tick_size
            exit_point = ask_market_cross['value'].iloc[0]+profit_target*tick_size
            position = 1

        elif (i==len(snapshot_data.index)-2) and (position==1):

            exit_list.append(snapshot_data['best_bid_p'].iloc[i+1])
            exit_time.append(snapshot_data.index[i+1])
            position = 0

        elif position==1:

            if bid_updates.empty:
                continue

            long_stop =  bid_updates[bid_updates['value']<=stop_point]

            if not long_stop.empty:
                exit_list.append(long_stop['value'].iloc[0])
                exit_time.append(long_stop['time'].iloc[0])
                position = 0
                continue

            long_exit =  bid_updates[bid_updates['value']>=exit_point]

            if not long_exit.empty:
                exit_list.append(long_exit['value'].iloc[0])
                exit_time.append(long_exit['time'].iloc[0])
                position = 0

    data_frame = pd.DataFrame.from_items([('entry_p', entry_list),
                                      ('exit_p', exit_list),
                                      ('entry_time', entry_time),
                                      ('exit_time', exit_time)])

    data_frame['pnl'] = data_frame['exit_p']-data_frame['entry_p']

    return data_frame














