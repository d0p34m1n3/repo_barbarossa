
import shared.directory_names as dn
import pandas as pd
import contract_utilities.contract_meta_info as cmi
import read_exchange_files.read_cme_files as rcf
import datetime as dt


def process_cme_futures_4tickerhead(**kwargs):

    ticker_head = kwargs['ticker_head']
    report_date = kwargs['report_date']

    ticker_class = cmi.ticker_class[ticker_head]

    if ticker_class == 'STIR':
        file_name = 'interest_rate'

    data_read_out = rcf.read_cme_settle_txt_files(file_name=file_name, report_date=report_date)

    title_frame = data_read_out['title_frame']
    settle_list = data_read_out['settle_list']
    open_list = data_read_out['open_list']
    high_list = data_read_out['high_list']
    low_list = data_read_out['low_list']

    volume_filtered_list = data_read_out['volume_filtered_list']
    interest_filtered_list = data_read_out['interest_filtered_list']

    month_strike_list = data_read_out['month_strike_list']

    selected_frame = title_frame[(title_frame['asset_type'] == 'futures') & (title_frame['ticker_head'] == ticker_head)]

    contact_month_strings = month_strike_list[selected_frame.index[0]]

    datetime_conversion = [dt.datetime.strptime(x.replace('JLY', 'JUL'),'%b%y') for x in contact_month_strings]

    settle_frame = pd.DataFrame()
    settle_frame['ticker_year'] = [x.year for x in datetime_conversion]
    settle_frame['ticker_month'] = [x.month for x in datetime_conversion]

    settle_frame['ticker'] = [ticker_head +
                            cmi.full_letter_month_list[settle_frame.loc[x, 'ticker_month']-1] +
                            str(settle_frame.loc[x, 'ticker_year']) for x in settle_frame.index]

    settle_frame['settle'] = settle_list[selected_frame.index[0]]
    settle_frame['settle'] = settle_frame['settle'].astype('float64')

    settle_frame['open'] = open_list[selected_frame.index[0]]
    settle_frame['open'] = settle_frame['open'].replace('----', 0)
    settle_frame['open'] = settle_frame['open'].astype('float64')

    settle_frame['high'] = high_list[selected_frame.index[0]]
    settle_frame['high'] = settle_frame['high'].replace('----', 0)
    settle_frame['high'] = settle_frame['high'].str.replace('B', '')
    settle_frame['high'] = settle_frame['high'].astype('float64')

    settle_frame['low'] = low_list[selected_frame.index[0]]
    settle_frame['low'] = settle_frame['low'].replace('----', 0)
    settle_frame['low'] = settle_frame['low'].str.replace('A', '')
    settle_frame['low'] = settle_frame['low'].astype('float64')

    settle_frame['volume'] = volume_filtered_list[selected_frame.index[0]]
    settle_frame['volume'] = settle_frame['volume'].replace('',0)
    settle_frame['volume'] = settle_frame['volume'].astype('int')

    settle_frame['interest'] = interest_filtered_list[selected_frame.index[0]]
    settle_frame['interest'] = settle_frame['interest'].replace('',0)
    settle_frame['interest'] = settle_frame['interest'].astype('int')

    return {'settle_frame': settle_frame,
           'volume_filtered_list': volume_filtered_list[selected_frame.index[0]],
            'contract_months': month_strike_list[selected_frame.index[0]]}


