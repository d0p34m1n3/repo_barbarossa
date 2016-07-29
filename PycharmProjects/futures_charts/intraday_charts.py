
import reformat_intraday_data.reformat_ttapi_intraday_data as rid
import matplotlib.pyplot as plt


def get_intraday_breakout_chart(**kwargs):

    intraday_data = rid.get_book_snapshot_4ticker(ticker=kwargs['ticker'], folder_date=kwargs['trade_date'])

    intraday_data['mid_p'] = (intraday_data['best_bid_p']+intraday_data['best_ask_p'])/2

    plt.figure(figsize=(16, 7))
    plt.plot(intraday_data.index,intraday_data['mid_p'])
    plt.axvline([x for x in intraday_data.index if 100*x.hour+x.minute == 830][0],color='r')
    plt.axvline([x for x in intraday_data.index if 100*x.hour+x.minute == 900][0],color='r')
    plt.grid()
    plt.show()

