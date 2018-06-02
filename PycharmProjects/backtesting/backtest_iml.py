
import get_price.quantgo_data as qd
import get_price.get_futures_price as gfp

def get_results_4ticker(**kwargs):

    ticker = kwargs['ticker']
    date_to = kwargs['date_to']

    candle_frame = qd.get_continuous_bar_data(ticker=ticker, date_to=date_to, num_days_back=0)

    history_frame = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=date_to)
    history_frame = history_frame.iloc[:-1]
    history_frame['ma9'] = history_frame['close_price'].rolling(window=9, center=False).mean()

    high_1 = history_frame['high_price'].iloc[-1]
    low_1 = history_frame['low_price'].iloc[-1]

    trend_direction = 0

    if history_frame['ma9'].iloc[-1] > history_frame['ma9'].iloc[-2]:
        trend_direction = 1
    elif history_frame['ma9'].iloc[-1] < history_frame['ma9'].iloc[-2]:
        trend_direction = -1








