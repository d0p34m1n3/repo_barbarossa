
import opportunity_constructs.intraday_breakouts as ibo
import contract_utilities.expiration as exp
import math as m
import pandas as pd


def construct_ibo_portfolio_4date(**kwargs):
    # date_to

    ibo_output = ibo.generate_ibo_sheet_4date(**kwargs)
    sheet_4date = ibo_output['sheet_4date']
    cov_output = ibo_output['cov_output']
    cov_data_integrity = cov_output['cov_data_integrity']

    if cov_data_integrity < 80:
        return {'success': False}

    cov_matrix = cov_output['cov_matrix']
    sheet_4date['qty'] = sheet_4date['ticker_head'].apply(lambda x: round(1000/m.sqrt(cov_matrix[x][x])))
    sheet_4date['pnl_scaled'] = sheet_4date['pnl']*sheet_4date['qty']

    return {'success': True, 'sheet_4date': sheet_4date}


def accumulated_ibo_performance(**kwargs):

    date_list = exp.get_bus_day_list(date_from=20160701, date_to=kwargs['date_to']) #date_from = 20160701
    result_list = []

    for i in range(len(date_list)):
        print(date_list[i])
        out_frame = construct_ibo_portfolio_4date(date_to=date_list[i])

        if out_frame['success']:
            sheet_4date = out_frame['sheet_4date']
            sheet_4date['trade_date'] = date_list[i]
            result_list.append(sheet_4date)

    merged_frame = pd.concat(result_list)
    return merged_frame



