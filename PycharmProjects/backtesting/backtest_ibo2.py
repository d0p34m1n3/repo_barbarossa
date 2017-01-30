
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import contract_utilities.contract_lists as cl
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
import pandas as pd

pd.options.mode.chained_assignment = None  # default='warn'


def backtest_ibo_4ticker(**kwargs):

    data_input = {}

    if 'futures_data_dictionary' in kwargs.keys():
        data_input['futures_data_dictionary'] = kwargs['futures_data_dictionary']

    ticker = kwargs['ticker']
    trade_date = kwargs['trade_date']

    ticker_head = cmi.get_contract_specs(ticker)['ticker_head']
    ticker_class = cmi.ticker_class[ticker_head]
    contract_multiplier = cmi.contract_multiplier[ticker_head]

    data_input['ticker'] = ticker

    data_out = gfp.get_futures_price_preloaded(**data_input)

    trade_datetime = cu.convert_doubledate_2datetime(trade_date)

    trade_data = data_out[data_out['settle_date'] == trade_datetime]

    signal_data = data_out[data_out['settle_date'] < trade_datetime]

    signal_data['ewma20'] = pd.ewma(signal_data['close_price'], span=20)
    signal_data['ewma50'] = pd.ewma(signal_data['close_price'], span=50)

    signal_data = signal_data.iloc[-20:]
    price_std = signal_data['close_price'].std()

    if signal_data['ewma20'].iloc[-1]>signal_data['ewma50'].iloc[-1]:
        long_term_trend = 1
    else:
        long_term_trend = -1

    lower_limit1 = signal_data['close_price'].iloc[-1]-price_std
    lower_limit2 = signal_data['close_price'].iloc[-1]-1.5*price_std
    lower_limit3 = signal_data['close_price'].iloc[-1]-2*price_std

    upper_limit1 = signal_data['close_price'].iloc[-1]+price_std
    upper_limit2 = signal_data['close_price'].iloc[-1]+1.5*price_std
    upper_limit3 = signal_data['close_price'].iloc[-1]+2*price_std

    if trade_data['high_price'].iloc[0] > upper_limit1:
        long_pnl1 = trade_data['close_price'].iloc[0]-upper_limit1
    else:
        long_pnl1 = 0

    if trade_data['high_price'].iloc[0] > upper_limit2:
        long_pnl2 = trade_data['close_price'].iloc[0]-upper_limit2
    else:
        long_pnl2 = 0

    if trade_data['high_price'].iloc[0] > upper_limit3:
        long_pnl3 = trade_data['close_price'].iloc[0]-upper_limit3
    else:
        long_pnl3 = 0

    if trade_data['low_price'].iloc[0] < lower_limit1:
        short_pnl1 = lower_limit1-trade_data['close_price'].iloc[0]
    else:
        short_pnl1 = 0

    if trade_data['low_price'].iloc[0] < lower_limit2:
        short_pnl2 = lower_limit2-trade_data['close_price'].iloc[0]
    else:
        short_pnl2 = 0

    if trade_data['low_price'].iloc[0] < lower_limit3:
        short_pnl3 = lower_limit3-trade_data['close_price'].iloc[0]
    else:
        short_pnl3 = 0

    return {'ticker': ticker,
            'ticker_head': ticker_head,
            'ticker_class': ticker_class,
            'qty': 5000/(contract_multiplier*price_std),
            'long_term_trend': long_term_trend,
            'long_pnl1': 5000*long_pnl1/price_std,
            'long_pnl2': 5000*long_pnl2/price_std,
            'long_pnl3': 5000*long_pnl3/price_std,
            'short_pnl1': 5000*short_pnl1/price_std,
            'short_pnl2': 5000*short_pnl2/price_std,
            'short_pnl3': 5000*short_pnl3/price_std}


def backtest_ibo_4date(**kwargs):

    trade_date = kwargs['trade_date']

    signal_date = exp.doubledate_shift_bus_days(double_date=trade_date)

    futures_frame = cl.generate_futures_list_dataframe(date_to=signal_date)

    futures_frame.sort(['ticker_head','volume'],ascending=[True, False], inplace=True)
    futures_frame = futures_frame.drop_duplicates('ticker_head')

    futures_frame.reset_index(drop=True, inplace=True)

    backtest_ibo_result_list = [backtest_ibo_4ticker(ticker=x,trade_date=trade_date) for x in futures_frame['ticker']]

    result_frame = pd.DataFrame()

    result_frame['ticker'] = [x['ticker'] for x in backtest_ibo_result_list]
    result_frame['ticker_head'] = [x['ticker_head'] for x in backtest_ibo_result_list]
    result_frame['ticker_class'] = [x['ticker_class'] for x in backtest_ibo_result_list]
    result_frame['qty'] = [x['qty'] for x in backtest_ibo_result_list]
    result_frame['long_term_trend'] = [x['long_term_trend'] for x in backtest_ibo_result_list]
    result_frame['long_pnl1'] = [x['long_pnl1'] for x in backtest_ibo_result_list]
    result_frame['long_pnl2'] = [x['long_pnl2'] for x in backtest_ibo_result_list]
    result_frame['long_pnl3'] = [x['long_pnl3'] for x in backtest_ibo_result_list]
    result_frame['short_pnl1'] = [x['short_pnl1'] for x in backtest_ibo_result_list]
    result_frame['short_pnl2'] = [x['short_pnl2'] for x in backtest_ibo_result_list]
    result_frame['short_pnl3'] = [x['short_pnl3'] for x in backtest_ibo_result_list]

    return result_frame











