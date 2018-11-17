
import shared.calendar_utilities as cu
import get_price.get_stock_price as gsp
import get_price.save_stock_data as ssd

symbol_list = ['IVV','VEU', 'BIL']

# sharp ratio 0.87
# max drawdown -%18

def get_signals_4date(**kwargs):

    report_date = kwargs['report_date']
    date_from = cu.doubledate_shift(report_date, 365)

    report_datetime = cu.convert_doubledate_2datetime(report_date)

    fund_price_dictionary = {x: gsp.get_stock_price_preloaded(ticker=x, settle_date_from=date_from, settle_date_to=report_date) for x in symbol_list}

    data_current_dictionary = {x: fund_price_dictionary[x]['settle_datetime'].iloc[-1] == report_datetime for x in symbol_list}

    symbols2update = []

    for key, value in data_current_dictionary.items():
        if not value:
            symbols2update.append(key)

    if symbols2update:
        ssd.save_stock_data(symbol_list=symbols2update)
        fund_price_dictionary = {x: gsp.get_stock_price_preloaded(ticker=x, settle_date_from=date_from, settle_date_to=report_date) for x in symbol_list}

    performance_dictionary = {}

    for j in range((len(symbol_list))):

        price_data = fund_price_dictionary[symbol_list[j]]

        if price_data['settle_datetime'].iloc[-1] != report_datetime:
            return {'success': False, 'performance_dictionary': {}}

        price_data.reset_index(drop=True, inplace=True)
        #split_envents = price_data[price_data['split_coefficient'] != 1]
        #split_index_list = split_envents.index
        #for i in range(len(split_index_list)):
        #    price_data['close'].iloc[:split_index_list[i]] = \
        #        price_data['close'].iloc[:split_index_list[i]] / price_data['split_coefficient'].iloc[split_index_list[i]]

        #   price_data['dividend_amount'].iloc[:split_index_list[i]] = \
        #        price_data['dividend_amount'].iloc[:split_index_list[i]] / price_data['split_coefficient'].iloc[split_index_list[i]]

        performance_dictionary[symbol_list[j]] = 100*(price_data['close'].iloc[-1]-price_data['close'].iloc[0])/price_data['close'].iloc[0]

    return {'success': True, 'performance_dictionary': performance_dictionary}

