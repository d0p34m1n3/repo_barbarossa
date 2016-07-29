
import pandas as pd
import contract_utilities.contract_lists as cl
import ta.strategy as ts
import signals.intraday_futures_signals as ifs
import os.path


def get_tickers_4date(**kwargs):

     futures_dataframe = cl.generate_futures_list_dataframe(**kwargs)
     futures_dataframe.sort(['ticker_head','volume'],ascending=[False,False],inplace=True)

     grouped = futures_dataframe.groupby(['ticker_head'])
     output_frame = pd.DataFrame()

     output_frame['ticker'] = grouped['ticker'].first()

     return output_frame.reset_index(drop=True,inplace=False)


def generate_ibo_sheet_4date(**kwargs):

    date_to = kwargs['date_to']
    output_dir = ts.create_strategy_output_dir(strategy_class='ibo', report_date=date_to)

    if os.path.isfile(output_dir + '/summary.pkl'):
        sheet_4date = pd.read_pickle(output_dir + '/summary.pkl')
        return {'sheet_4date': sheet_4date,'success': True}

    sheet_4date = get_tickers_4date(**kwargs)

    num_tickers = len(sheet_4date.index)

    signals_output = [ifs.get_intraday_trend_signals(ticker=sheet_4date.iloc[x]['ticker'],date_to=date_to)
                      for x in range(num_tickers)]

    sheet_4date['daily_pnl'] = [x['daily_pnl'] for x in signals_output]

    sheet_4date.to_pickle(output_dir + '/summary.pkl')

    return {'sheet_4date': sheet_4date, 'success': True}



