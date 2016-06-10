
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import shared.directory_names as dn
import pandas as pd


def get_ticker_list_4tickerhead(**kwargs):

    if 'volume_filter' in kwargs.keys():
        volume_filter = kwargs['volume_filter']
    else:
        volume_filter = 1000

    ticker_head = kwargs['ticker_head']

    futures_frame = gfp.get_futures_price_preloaded(**kwargs)

    futures_frame = futures_frame[futures_frame['volume']>=volume_filter]

    outright_frame = pd.DataFrame()
    spread_frame = pd.DataFrame()
    wide_spread_frame1 = pd.DataFrame()
    wide_spread_frame2 = pd.DataFrame()

    outright_frame['ticker1'] = futures_frame['ticker']
    outright_frame['ticker2'] = None

    spread_frame['ticker1'] = futures_frame['ticker']
    spread_frame['ticker2'] = futures_frame['ticker'].shift(-1)

    if ticker_head in ['CL', 'HO', 'RB', 'NG', 'ED']:

        if ticker_head == 'ED':
            diff = 3
        else:
            diff = 1

        spread_frame['ticker_month1'] = futures_frame['ticker_month']
        spread_frame['ticker_month2'] = futures_frame['ticker_month'].shift(-1)

        spread_frame['ticker_year1'] = futures_frame['ticker_year']
        spread_frame['ticker_year2'] = futures_frame['ticker_year'].shift(-1)

        spread_frame['diff'] = 12*(spread_frame['ticker_year2']-spread_frame['ticker_year1'])+\
                           (spread_frame['ticker_month2']-spread_frame['ticker_month1'])

        spread_frame = spread_frame[spread_frame['diff'] == diff]

        spread_frame.drop(['ticker_month1','ticker_month2','ticker_year1','ticker_year2','diff'],axis=1,inplace=True)

    else:

        spread_frame.drop(spread_frame.index[-1],inplace=True)

    if ticker_head == 'CL':

        wide_futures_frame1 = futures_frame[futures_frame['ticker_month'] % 3 == 0]
        wide_spread_frame1['ticker1'] = wide_futures_frame1['ticker']
        wide_spread_frame1['ticker2'] = wide_futures_frame1['ticker'].shift(-1)
        wide_spread_frame1.drop(wide_spread_frame1.index[-1],inplace=True)

        wide_futures_frame2 = futures_frame[futures_frame['ticker_month'] % 6 == 0]
        wide_spread_frame2['ticker1'] = wide_futures_frame2['ticker']
        wide_spread_frame2['ticker2'] = wide_futures_frame2['ticker'].shift(-1)
        wide_spread_frame2.drop(wide_spread_frame2.index[-1],inplace=True)

    output_frame = pd.concat([outright_frame,spread_frame,wide_spread_frame1,wide_spread_frame2])
    output_frame['tickerHead'] = ticker_head

    return output_frame.drop_duplicates(['ticker1','ticker2'],inplace=False)


def get_ticker_list(**kwargs):

    output_frame_list = [get_ticker_list_4tickerhead(ticker_head=x, settle_date=20160602,volume_filter=100)
                    for x in cmi.cme_futures_tickerhead_list]

    output_frame = pd.concat(output_frame_list)

    output_dir = dn.get_directory_name(ext='daily')
    writer = pd.ExcelWriter(output_dir + '/intraday_tickers.xlsx', engine='xlsxwriter')
    output_frame.to_excel(writer, sheet_name='tickers')
    writer.save()





