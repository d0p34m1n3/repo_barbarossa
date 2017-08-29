
import contract_utilities.contract_meta_info as cmi
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import pandas as pd
pd.options.mode.chained_assignment = None

def get_underlying_proxy_ticker(**kwargs):

    ticker = kwargs['ticker']
    settle_date = kwargs['settle_date']
    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']
    ticker_class = contract_specs_output['ticker_class']

    if ticker_class in ['Livestock', 'Ag', 'Soft', 'Energy', 'STIR']:
        return {'ticker': ticker, 'add_2_proxy': 0}

    if 'futures_data_dictionary' in kwargs.keys():
        futures_data_dictionary = kwargs['futures_data_dictionary']
    else:
        futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [ticker_head]}

    settle_date_from = cu.doubledate_shift(settle_date, 15)
    settle_datetime = cu.convert_doubledate_2datetime(settle_date)

    panel_data = gfp.get_futures_price_preloaded(ticker_head=ticker_head,settle_date_from=settle_date_from,settle_date_to=settle_date,futures_data_dictionary=futures_data_dictionary)

    last_day_data = panel_data[panel_data['settle_date'] == settle_datetime]
    last_day_data.sort('volume', ascending=False, inplace=True)
    proxy_ticker = last_day_data['ticker'].iloc[0]

    ticker_data = panel_data.loc[panel_data['ticker'] == ticker, ['settle_date', 'close_price']]
    proxy_data = panel_data.loc[panel_data['ticker'] == proxy_ticker, ['settle_date', 'close_price']]

    merged_data = pd.merge(ticker_data, proxy_data, on=['settle_date'], how='inner')
    merged_data['add_to_proxy'] = merged_data['close_price_x'] - merged_data['close_price_y']
    add_2_proxy = merged_data['add_to_proxy'].mean()

    return {'ticker': proxy_ticker, 'add_2_proxy':add_2_proxy}
