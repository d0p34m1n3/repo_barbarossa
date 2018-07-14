
import contract_utilities.contract_meta_info as cmi
import contract_utilities.contract_lists as cl
import contract_utilities.expiration as exp
import get_price.get_futures_price as gfp
import shared.calendar_utilities as cu
import contract_utilities.expiration as exp
import shared.directory_names as dn
import my_sql_routines.my_sql_utilities as msu
import pandas as pd
pd.options.mode.chained_assignment = None


def get_underlying_proxy_ticker(**kwargs):

    ticker = kwargs['ticker']
    settle_date = kwargs['settle_date']
    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_head = contract_specs_output['ticker_head']
    ticker_class = contract_specs_output['ticker_class']

    con = msu.get_my_sql_connection(**kwargs)

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

    last_day_data['tr_days_2roll'] = last_day_data.apply(lambda x: exp.get_days2_roll(ticker=x['ticker'],
                                                                                   instrument='Futures',
                                                                                   date_to=settle_date,con=con)['tr_days_2roll'],axis=1)

    last_day_data = last_day_data[last_day_data['tr_days_2roll'] >= 10]
    last_day_data.reset_index(drop=True, inplace=True)

    last_day_data.sort_values('volume', ascending=False, inplace=True)

    proxy_ticker = last_day_data['ticker'].iloc[0]

    ticker_data = panel_data.loc[panel_data['ticker'] == ticker, ['settle_date', 'close_price']]
    proxy_data = panel_data.loc[panel_data['ticker'] == proxy_ticker, ['settle_date', 'close_price']]

    merged_data = pd.merge(ticker_data, proxy_data, on=['settle_date'], how='inner')
    merged_data['add_to_proxy'] = merged_data['close_price_x'] - merged_data['close_price_y']
    add_2_proxy = merged_data['add_to_proxy'].mean()

    if 'con' not in kwargs.keys():
        con.close()

    return {'ticker': proxy_ticker, 'add_2_proxy': add_2_proxy}


def generate_underlying_proxy_report(**kwargs):

    con = msu.get_my_sql_connection(**kwargs)
    report_date = kwargs['report_date']
    futures_dataframe = cl.generate_futures_list_dataframe(date_to=report_date)
    futures_flat_curve = futures_dataframe[futures_dataframe['ticker_class'].isin(['FX','Metal','Treasury','Index'])]
    futures_flat_curve.reset_index(drop=True, inplace=True)

    futures_flat_curve['tr_days_2roll'] = futures_flat_curve.apply(lambda x: exp.get_days2_roll(ticker=x['ticker'],
                                                                                   instrument='Futures',
                                                                                   date_to=report_date,con=con)['tr_days_2roll'],axis=1)

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in futures_flat_curve['ticker_head'].unique()}

    proxy_output_list = [get_underlying_proxy_ticker(ticker=futures_flat_curve['ticker'].iloc[x],
                                                    settle_date=report_date,
                                                    con=con,futures_data_dictionary=futures_data_dictionary) for x in range(len(futures_flat_curve.index))]

    futures_flat_curve['proxy_ticker'] = [x['ticker'] for x in proxy_output_list]
    futures_flat_curve['add_2_proxy'] = [x['add_2_proxy'] for x in proxy_output_list]

    ta_output_dir = dn.get_dated_directory_extension(folder_date=report_date, ext='ta')
    writer = pd.ExcelWriter(ta_output_dir + '/proxy_report.xlsx', engine='xlsxwriter')
    futures_flat_curve = futures_flat_curve[['ticker','ticker_head','ticker_class','volume','tr_days_2roll','proxy_ticker','add_2_proxy']]

    futures_flat_curve.to_excel(writer, sheet_name='proxy_report')
    writer.save()

    if 'con' not in kwargs.keys():
        con.close()




