
ib_worst_case_fees = {'C': 2.8,
                      'S': 2.8,
                      'BO': 2.8,
                      'SM': 2.8,
                      'W': 2.8,
                      'KW': 2.8,
                      'CL': 2.35,
                      'HO': 2.35,
                      'NG': 2.35,
                      'RB': 2.35,
                      'LN': 2.88,
                      'LC': 2.88,
                      'FC': 2.88}


def ib_fee_calculator(**kwargs):

    ticker_head = kwargs['ticker_head']

    return ib_worst_case_fees[ticker_head]

