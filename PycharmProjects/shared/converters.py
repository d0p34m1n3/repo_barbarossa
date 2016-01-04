__author__ = 'kocat_000'

def convert_from_dictionary_to_string(**kwargs):

    dictionary_input = kwargs['dictionary_input']
    return '&'.join([str(k) + '=' + str(v) for k, v in dictionary_input.items()])

