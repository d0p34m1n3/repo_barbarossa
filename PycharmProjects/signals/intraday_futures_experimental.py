
import boto3 as bt3
import get_price.quantgo_data as qgd
import numpy as np

def get_crossover_duration(**kwargs):

    if 'boto_client' in kwargs.keys():
        boto_client = kwargs['boto_client']
    else:
        boto_client = bt3.client('s3',aws_access_key_id=qgd.aws_access_key_id,aws_secret_access_key=qgd.aws_secret_access_key)

    data_out = qgd.get_continuous_bar_data(ticker=kwargs['ticker'], date_to=kwargs['date_to'], num_days_back=20, boto_client=boto_client)

    data_out['ema10'] = data_out['close'].ewm(span=10, min_periods=6, adjust=False).mean()
    data_out['ema30'] = data_out['close'].ewm(span=30, min_periods=18, adjust=False).mean()

    data_out['ema10_30'] = data_out['ema10'] - data_out['ema30']
    data_out['ema10_30_1'] = data_out['ema10_30'].shift(1)

    clean_data = data_out[data_out['ema10_30'].notnull() & data_out['ema10_30_1'].notnull()]
    crossover_index = np.sign(clean_data['ema10_30_1']) != np.sign(clean_data['ema10_30'])
    clean_data['num_obs'] = range(len(clean_data.index))

    crossover_data = clean_data[crossover_index]
    crossover_data['crossover_duration'] = crossover_data['num_obs'].diff()
    quantile_out = crossover_data['crossover_duration'].quantile([0.25, 0.75])

    return {'mean_duration': crossover_data['crossover_duration'].mean(),
            'duration_25': quantile_out[0.25],
            'duration_75': quantile_out[0.75],
            'num_crossovers': len(crossover_data.index)}

