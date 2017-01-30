
import signals.intraday_futures_signals as ifs
import signals.intraday_technical_scalper as its
import signals.ifs as ifs2
import matplotlib.pyplot as plt
import datetime as dt
import shared.calendar_utilities as cu


def get_intraday_breakout_chart(**kwargs):

    intraday_results = its.get_technical_scalper_4ticker(ticker=kwargs['ticker'],date_to=kwargs['trade_date'])
    intraday_data = intraday_results['hloc_data']

    if 'hour_minute_from' in kwargs.keys():
        hour_minute_from = kwargs['hour_minute_from']
    else:
        hour_minute_from = 930

    intraday_data = intraday_data[intraday_data['hour_minute'] >= hour_minute_from]

    plt.figure(figsize=(16, 7))
    plt.subplot(211)
    plt.plot(intraday_data.index,intraday_data['close'])
    plt.grid()
    plt.subplot(212)
    plt.plot(intraday_data.index,intraday_data['D1'],
             intraday_data.index,intraday_data['D2'])

    plt.grid()
    plt.show()


def get_intraday_futures_spread_chart(**kwargs):
    """inputs: ticker_list, date_to, num_days_back_4intraday =5 """

    signals_output = ifs.get_intraday_spread_signals(**kwargs)
    intraday_data = signals_output['intraday_data']
    plt.figure(figsize=(16, 7))
    plt.plot(range(len(intraday_data.index)),intraday_data['spread'],color='k')
    plt.plot(range(len(intraday_data.index)),intraday_data['ma40'],color='b')
    plt.grid()
    plt.show()


def get_intraday_spread_chart2(**kwargs):
    """inputs: ticker, date_to, num_days_back_4intraday =5 """

    signals_output = ifs.get_intraday_spread_signals(**kwargs)
    intraday_data = signals_output['trading_data']
    plt.figure(figsize=(16, 7))
    plt.subplot(311)
    plt.plot(range(len(intraday_data.index)),intraday_data['spread'],color='k')
    plt.plot(range(len(intraday_data.index)),intraday_data['ma40'],color='b')
    plt.grid()
    plt.title('ma spread low: ' + str(signals_output['ma_spread_low']) + ' ma spread high: ' + str(signals_output['ma_spread_high']))
    plt.subplot(312)
    plt.plot(range(len(intraday_data.index)),intraday_data['ma40_spread'],color='k')
    plt.grid()
    plt.subplot(313)
    plt.plot(range(len(intraday_data.index)),intraday_data['proxy_pnl'].cumsum(),color='k')
    plt.grid()
    plt.show()




