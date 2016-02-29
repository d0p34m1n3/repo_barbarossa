
import contract_utilities.contract_meta_info as cmi
import read_exchange_files.read_cme_files as rcf
import read_exchange_files.cme_utilities as cmeu
import datetime as dt
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'


def process_cme_options_4ticker(**kwargs):

    ticker = kwargs['ticker']
    report_date = kwargs['report_date']

    contract_specs_output = cmi.get_contract_specs(ticker)
    ticker_class = contract_specs_output['ticker_class']
    ticker_head = contract_specs_output['ticker_head']

    name_type_output = cmeu.get_file_name_type_from_tickerclass(ticker_class)
    file_name = name_type_output['file_name']
    file_type = name_type_output['file_type']

    if file_type == 'txt':
        data_read_out = rcf.read_cme_settle_txt_files(file_name=file_name, report_date=report_date)
        return data_read_out
        title_frame = data_read_out['title_frame']
        settle_list = data_read_out['settle_list']
        month_strike_list = data_read_out['month_strike_list']
        volume_filtered_list = data_read_out['volume_filtered_list']
        interest_filtered_list = data_read_out['interest_filtered_list']

        selected_frame = title_frame[(title_frame['asset_type'] == 'options') & (title_frame['ticker_head'] == ticker_head)]

        datetime_conversion = [dt.datetime.strptime(x.replace('JLY', 'JUL'), '%b%y') for x in selected_frame['maturity_string']]
        selected_frame['ticker_year'] = [x.year for x in datetime_conversion]
        selected_frame['ticker_month'] = [x.month for x in datetime_conversion]

        selected_frame['ticker'] = [ticker_head +
                            cmi.full_letter_month_list[selected_frame.loc[x, 'ticker_month']-1] +
                            str(selected_frame.loc[x, 'ticker_year']) for x in selected_frame.index]

        selected_frame_call = selected_frame[(selected_frame['ticker'] == ticker)&(selected_frame['option_type'] == 'C')]
        selected_frame_put = selected_frame[(selected_frame['ticker'] == ticker)&(selected_frame['option_type'] == 'P')]
        selected_call_indx = selected_frame_call.index[0]
        selected_put_indx = selected_frame_put.index[0]

        call_dataframe = pd.DataFrame.from_items([('strike', month_strike_list[selected_call_indx]),
                                                  ('settle', settle_list[selected_call_indx]),
                                                  ('volume', volume_filtered_list[selected_call_indx]),
                                                  ('interest', interest_filtered_list[selected_call_indx])])

        put_dataframe = pd.DataFrame.from_items([('strike', month_strike_list[selected_put_indx]),
                                                  ('settle', settle_list[selected_put_indx]),
                                                  ('volume', volume_filtered_list[selected_put_indx]),
                                                  ('interest', interest_filtered_list[selected_put_indx])])

        call_dataframe['option_type'] = 'C'
        put_dataframe['option_type'] = 'P'

        settle_frame = pd.concat([call_dataframe, put_dataframe])
        settle_frame['strike'] = settle_frame['strike'].astype('float64')
        settle_frame['settle'] = settle_frame['settle'].astype('float64')

        settle_frame['volume'] = settle_frame['volume'].replace('', 0)
        settle_frame['volume'] = settle_frame['volume'].astype('int')

        settle_frame['interest'] = settle_frame['interest'].replace('', 0)
        settle_frame['interest'] = settle_frame['interest'].astype('int')


        return {'settle_list': settle_list,
                'month_strike_list': month_strike_list,
                'selected_call_indx': selected_call_indx,
                'selected_put_indx': selected_put_indx,
                'settle_frame': settle_frame}


