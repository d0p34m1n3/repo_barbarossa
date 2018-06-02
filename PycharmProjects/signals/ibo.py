
import numpy as np

def get_poi(**kwargs):

    candle_frame = kwargs['candle_frame']
    poi_type = kwargs['poi_type']

    if poi_type=='open':
        open_candle = candle_frame[candle_frame['hour_minute'] == 830]
        if len(open_candle.index)>0:
            poi_out = open_candle['open'].iloc[0]
            success = True
        else:
            poi_out = np.nan
            success = False

    return {'success': success, 'poi': poi_out}

def get_eod_price(**kwargs):

    candle_frame = kwargs['candle_frame']
    ticker_head = kwargs['ticker_head']

    if ticker_head in ['CL']:
        eod_candles = candle_frame[candle_frame['hour_minute'] <= 1555]
        eod_price = eod_candles['open'].iloc[-1]

    return eod_price


def get_distance(**kwargs):

    settle_frame = kwargs['settle_frame']
    distance_type = kwargs['distance_type']
    distance_multiplier = kwargs['distance_multiplier']

    if distance_type=='daily_sd':
        settle_frame['close_diff'] = settle_frame['close_price'].diff()
        distance_out = distance_multiplier*np.std(settle_frame['close_diff'].iloc[-40:])

    return distance_out

def get_time_filter(**kwargs):

    ticker_head = kwargs['ticker_head']
    section_no = kwargs['section_no']

    if ticker_head in ['CL']:
        if section_no==1:
            start_hour_minute = 830
            end_hour_minute = 1045
        elif section_no==2:
            start_hour_minute = 1045
            end_hour_minute = 1300
        elif section_no==3:
            start_hour_minute = 1300
            end_hour_minute = 1515

    return {'start_hour_minute': start_hour_minute, 'end_hour_minute': end_hour_minute}












