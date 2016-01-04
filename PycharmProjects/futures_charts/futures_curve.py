__author__ = 'kocat_000'

import sys
sys.path.append(r'C:\Users\kocat_000\quantFinance\PycharmProjects')
import get_price.get_futures_price as gfp
import contract_utilities.contract_meta_info as cmi
import matplotlib.pyplot as plt
import opportunity_constructs.futures_butterfly as fb
import contract_utilities.expiration as exp
import opportunity_constructs.utilities as opUtil
import shared.calendar_utilities as cu


def get_futures_curve_chart_4date(**kwargs):

    ticker_head = kwargs['ticker_head']
    settle_date = kwargs['settle_date']

    data2_plot = gfp.get_futures_price_preloaded(ticker_head=ticker_head, settle_date=settle_date)

    ticker_year_short = data2_plot['ticker_year'] % 10
    month_letters = [cmi.letter_month_string[x-1] for x in data2_plot['ticker_month'].values]

    tick_labels = [month_letters[x]+str(ticker_year_short.values[x]) for x in range(len(month_letters))]

    plt.plot(data2_plot['close_price'])
    plt.xticks(range(len(data2_plot.index)),tick_labels)
    plt.grid()
    plt.show()

def get_butterfly_panel_plot(**kwargs):

    report_date = kwargs['report_date']
    id = kwargs['id']

    bf_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = bf_output['butterflies']

    contract_list = [butterflies['ticker1'][id],butterflies['ticker2'][id],butterflies['ticker3'][id]]
    tr_dte_list = [butterflies['trDte1'][id],butterflies['trDte2'][id],butterflies['trDte3'][id]]
    aggregation_method = butterflies['agg'][id]
    contracts_back = butterflies['cBack'][id]

    post_report_date = exp.doubledate_shift_bus_days(double_date=report_date,shift_in_days=-20)

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [butterflies['tickerHead'][id]]}

    aligned_data_output = opUtil.get_aligned_futures_data(contract_list=contract_list,
                                 tr_dte_list=tr_dte_list,
                                aggregation_method = aggregation_method,
                                contracts_back = contracts_back,
                                futures_data_dictionary = futures_data_dictionary,date_to = post_report_date)
    aligned_data = aligned_data_output['aligned_data']

    yield1 = 100*(aligned_data['c1']['close_price']-aligned_data['c2']['close_price'])/aligned_data['c2']['close_price']
    yield2 = 100*(aligned_data['c2']['close_price']-aligned_data['c3']['close_price'])/aligned_data['c3']['close_price']

    new_index = list(range(len(aligned_data.index)))
    contract_change_indx = (aligned_data['c1']['ticker_year']-aligned_data['c1']['ticker_year'].shift(1)!=0).values
    front_contract_year = aligned_data['c1']['ticker_year'] % 10
    contract_change_indx[0] = False

    report_datetime = cu.convert_doubledate_2datetime(report_date)

    x_index = [x for x in new_index if aligned_data['settle_date'][x] == report_datetime][0]

    x_tick_locations = [x for x in new_index if contract_change_indx[x]]
    x_tick_locations.append(x_index)

    x_tick_values = [cmi.letter_month_string[aligned_data['c1']['ticker_month'].values[x]-1]+
                     str(front_contract_year.values[x]) for x in new_index if contract_change_indx[x]]
    x_tick_values.append('X')

    plt.plot(yield1-yield2*butterflies['second_spread_weight_1'][id])
    plt.xticks(x_tick_locations,x_tick_values)
    plt.grid()
    plt.show()

    return aligned_data_output

def get_butterfly_scatter_plot(**kwargs):

    report_date = kwargs['report_date']
    id = kwargs['id']

    bf_output = fb.generate_futures_butterfly_sheet_4date(date_to=report_date)
    butterflies = bf_output['butterflies']

    contract_list = [butterflies['ticker1'][id],butterflies['ticker2'][id],butterflies['ticker3'][id]]
    tr_dte_list = [butterflies['trDte1'][id],butterflies['trDte2'][id],butterflies['trDte3'][id]]
    aggregation_method = butterflies['agg'][id]
    contracts_back = butterflies['cBack'][id]

    futures_data_dictionary = {x: gfp.get_futures_price_preloaded(ticker_head=x) for x in [butterflies['tickerHead'][id]]}

    aligned_data_output = opUtil.get_aligned_futures_data(contract_list=contract_list,
                                 tr_dte_list=tr_dte_list,
                                aggregation_method = aggregation_method,
                                contracts_back = contracts_back,
                                futures_data_dictionary = futures_data_dictionary,date_to = report_date)
    aligned_data = aligned_data_output['aligned_data']
    current_data = aligned_data_output['current_data']

    yield1 = 100*(aligned_data['c1']['close_price']-aligned_data['c2']['close_price'])/aligned_data['c2']['close_price']
    yield2 = 100*(aligned_data['c2']['close_price']-aligned_data['c3']['close_price'])/aligned_data['c3']['close_price']

    date5_years_ago = cu.doubledate_shift(report_date,5*365)
    datetime5_years_ago = cu.convert_doubledate_2datetime(date5_years_ago)

    last5_years_indx = aligned_data['settle_date']>=datetime5_years_ago
    data_last5_years = aligned_data[last5_years_indx]

    yield1_last5_years = yield1[last5_years_indx]
    yield2_last5_years = yield2[last5_years_indx]

    yield1_current = 100*(current_data['c1']['close_price'][0]-current_data['c2']['close_price'][0])/current_data['c2']['close_price'][0]
    yield2_current = 100*(current_data['c2']['close_price'][0]-current_data['c3']['close_price'][0])/current_data['c3']['close_price'][0]

    plt.scatter(yield2, yield1, color='b')
    plt.scatter(yield2_last5_years,yield1_last5_years, color='k')
    plt.scatter(yield2_current, yield1_current, color='r')
    plt.legend(['old', 'recent', 'last'], frameon=False)
    plt.grid()
    plt.show()

    return aligned_data_output

