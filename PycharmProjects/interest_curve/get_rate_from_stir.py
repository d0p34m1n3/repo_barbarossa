
import get_price.get_futures_price as gfp
import contract_utilities.expiration as exp
import shared.directory_names as dn
import os.path
import shared.calendar_utilities as cu
import numpy as np
import pandas as pd
import os.path


def get_simple_rate(**kwargs):

    as_of_date = kwargs['as_of_date']
    date_to = kwargs['date_to']

    if 'date_from' in kwargs.keys():
        date_from = kwargs['date_from']
    else:
        date_from = as_of_date

    if 'ticker_head' in kwargs.keys():
        ticker_head = kwargs['ticker_head']
    else:
        ticker_head = 'ED'

    ta_output_dir = dn.get_dated_directory_extension(folder_date=as_of_date,ext='ta')

    file_name = ta_output_dir + '/' + ticker_head + '_interest_curve.pkl'

    #print('as_of_date: ' + str(as_of_date) + ', date_to: ' + str(date_to))

    if os.path.isfile(file_name):
        price_frame = pd.read_pickle(file_name)

    if (not os.path.isfile(file_name)) or price_frame.empty:
        price_frame = gfp.get_futures_price_preloaded(ticker_head=ticker_head, settle_date=as_of_date)
        price_frame = price_frame[price_frame['close_price'].notnull()]

        price_frame.sort_values('tr_dte', ascending=True, inplace=True)
        price_frame['exp_date'] = [exp.get_futures_expiration(x) for x in price_frame['ticker']]
        price_frame['implied_rate'] = 100-price_frame['close_price']
        price_frame.to_pickle(file_name)

    if price_frame.empty:
        return {'rate_output': np.NaN, 'price_frame': pd.DataFrame(columns=['ticker', 'cal_dte','exp_date','implied_rate'])}

    datetime_to = cu.convert_doubledate_2datetime(date_to)
    datetime_from = cu.convert_doubledate_2datetime(date_from)

    price_frame_first = price_frame[price_frame['exp_date'] <= datetime_from]
    price_frame_middle = price_frame[(price_frame['exp_date'] > datetime_from) & (price_frame['exp_date'] < datetime_to)]

    if price_frame_middle.empty:
        if not price_frame_first.empty:
            rate_output = price_frame_first['implied_rate'].iloc[-1]/100
        else:
            rate_output = price_frame['implied_rate'].iloc[0]/100
        return {'rate_output': rate_output, 'price_frame': price_frame[['ticker', 'cal_dte', 'exp_date','implied_rate']]}

    if price_frame_first.empty:
        first_rate = price_frame_middle['implied_rate'].iloc[0]
        first_period = (price_frame_middle['exp_date'].iloc[0].to_pydatetime()-datetime_from).days
    else:
        first_rate = price_frame_first['implied_rate'].iloc[-1]
        first_period = (price_frame_middle['exp_date'].iloc[0].to_pydatetime()-datetime_from).days

    last_rate = price_frame_middle['implied_rate'].iloc[-1]
    last_period = (datetime_to-price_frame_middle['exp_date'].iloc[-1].to_pydatetime()).days

    middle_discount = [1+(price_frame_middle['implied_rate'].iloc[x]*
        (price_frame_middle['cal_dte'].iloc[x+1]-price_frame_middle['cal_dte'].iloc[x])/36500) for x in range(len(price_frame_middle.index)-1)]

    total_discount = np.prod(np.array(middle_discount))*(1+(first_rate*first_period/36500))*(1+(last_rate*last_period/36500))

    total_period = (price_frame_middle['cal_dte'].iloc[-1]-price_frame_middle['cal_dte'].iloc[0])+first_period+last_period

    rate_output = (total_discount-1)*365/total_period

    return {'rate_output': rate_output, 'price_frame': price_frame[['ticker', 'cal_dte', 'exp_date','implied_rate']]}
