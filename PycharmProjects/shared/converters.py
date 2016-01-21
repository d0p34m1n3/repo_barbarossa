__author__ = 'kocat_000'

def convert_from_dictionary_to_string(**kwargs):

    dictionary_input = kwargs['dictionary_input']
    return '&'.join([str(k) + '=' + str(v) for k, v in dictionary_input.items()])

def convert_from_string_to_dictionary(**kwargs):

    string_input = kwargs['string_input']
    return dict(item.split("=") for item in string_input.split('&'))


