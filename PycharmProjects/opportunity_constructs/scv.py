
import signals.option_signals as ops


def get_single_contracts_4date(**kwargs):

    option_frame = ops.get_option_ticker_indicators(**kwargs)

    if 'open_interest_filter' in kwargs.keys():
        open_interest_filter = kwargs['open_interest_filter']
    else:
        open_interest_filter = 100

    option_frame = option_frame[option_frame['open_interest']>=open_interest_filter]
    option_frame['ticker_class'] = [cmi.ticker_class[x] for x in option_frame['ticker_head']]

    selection_indx = (option_frame['ticker_class'] == 'Livestock') | (option_frame['ticker_class'] == 'Ag') | \
                     (option_frame['ticker_class'] == 'Treasury') | (option_frame['ticker_head'] == 'CL') | \
                     (option_frame['ticker_class'] == 'FX') | (option_frame['ticker_class'] == 'Index') | \
                     (option_frame['ticker_class'] == 'Metal')
    option_frame = option_frame[selection_indx]

    option_frame = option_frame[option_frame['tr_dte'] >= 20]
    option_frame.reset_index(drop=True,inplace=True)

