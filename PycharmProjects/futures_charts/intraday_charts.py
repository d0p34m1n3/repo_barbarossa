
import signals.intraday_futures_signals as ifs
import matplotlib.pyplot as plt
import datetime as dt
import shared.calendar_utilities as cu


def get_intraday_breakout_chart(**kwargs):

    signal_out = ifs.get_intraday_trend_signals(ticker=kwargs['ticker'],date_to=kwargs['trade_date'])
    intraday_data = signal_out['intraday_data']
    trade_datetime = cu.convert_doubledate_2datetime(kwargs['trade_date'])

    intraday_data = intraday_data[intraday_data['time_stamp']>=trade_datetime]

    plt.figure(figsize=(16, 7))
    plt.plot(intraday_data['time_stamp'],intraday_data['mid_p'],color='k')
    plt.plot(intraday_data['time_stamp'],intraday_data['ewma25'],color='b')
    plt.plot(intraday_data['time_stamp'],intraday_data['ewma100'],color='g')


    plt.axvline(dt.datetime.combine(trade_datetime,dt.time(8,30,0,0)),color='r')
    plt.axvline(dt.datetime.combine(trade_datetime,dt.time(9,0,0,0)),color='r')
    #plt.axvline([x for x in intraday_data['time_stamp'] if 100*x.hour+x.minute == 830][0],color='r')
    #plt.axvline([x for x in intraday_data.index if 100*x.hour+x.minute == 900][0],color='r')
    plt.grid()
    plt.show()

