
def get_file_name_type_from_tickerclass(ticker_class):

    if ticker_class in ['STIR', 'Treasury']:
        file_name = 'interest_rate'
        file_type = 'txt'
    elif ticker_class in ['Ag', 'Livestock']:
        file_name = 'commodity'
        file_type = 'txt'
    elif ticker_class == 'Index':
        file_name = 'equity'
        file_type = 'txt'
    elif ticker_class == 'FX':
        file_name = 'fx'
        file_type = 'txt'
    elif ticker_class == 'Metal':
        file_name = 'comex_futures'
        file_type = 'csv'
    elif ticker_class == 'Energy':
        file_name = 'nymex_futures'
        file_type = 'csv'

    return {'file_name': file_name, 'file_type': file_type}