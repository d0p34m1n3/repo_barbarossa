
import numpy as np
import shared.statistics as stats
from itertools import chain


def list_and(*args):
    return [all(tuple) for tuple in zip(*args)]


def list_or(*args):
    return [any(tuple) for tuple in zip(*args)]


def flatten_list(input_list):
    return list(chain.from_iterable(input_list))


def get_closest(**kwargs):
    list_input = kwargs['list_input']
    target_value = kwargs['target_value']

    return min(list_input, key=lambda x: abs(x-target_value))


def get_equal_length_partition(**kwargs):

    min_value = kwargs['min_value']
    max_value = kwargs['max_value']
    num_parts = kwargs['num_parts']

    interval_step = (max_value-min_value)/num_parts

    return np.arange(min_value+interval_step,max_value,interval_step)


def get_key_in_dictionary(**kwargs):

    dictionary_input = kwargs['dictionary_input']
    value_input = kwargs['value']

    key = (key for key,value in dictionary_input.items() if value == value_input).__next__()
    return key


def bucket_data(**kwargs):

    data_input = kwargs['data_input']
    bucket_var = kwargs['bucket_var']

    if 'ascending_q' in kwargs.keys():
        ascending_q = kwargs['ascending_q']
    else:
        ascending_q = True

    if 'num_buckets' in kwargs.keys():
        num_buckets = kwargs['num_buckets']
    else:
        num_buckets = 10

    quantile_limits = get_equal_length_partition(min_value=0,
                                                  max_value=100,
                                                  num_parts=num_buckets)

    bucket_limits = stats.get_number_from_quantile(y=data_input[bucket_var].values.astype(np.float64),
                                                   quantile_list=quantile_limits)

    bucket_data_list = []

    for i in range(num_buckets):

        if i == 0:
            bucket_data = data_input.loc[data_input[bucket_var] <= bucket_limits[i]]
        elif i < num_buckets-1:
            bucket_data = data_input.loc[(data_input[bucket_var] > bucket_limits[i-1])&
                                         (data_input[bucket_var] <= bucket_limits[i])]
        else:
            bucket_data = data_input.loc[data_input[bucket_var] > bucket_limits[i-1]]

        bucket_data_list.append(bucket_data)

    if not ascending_q:
        bucket_data_list.reverse()
        bucket_limits = np.flipud(bucket_limits)

    return {'bucket_data_list' : bucket_data_list, 'bucket_limits' : bucket_limits}