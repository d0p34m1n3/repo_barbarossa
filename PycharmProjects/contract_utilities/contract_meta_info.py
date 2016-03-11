__author__ = 'kocat_000'

import math as m

full_letter_month_list = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
quarterly_month_list = ['H', 'M', 'U', 'Z']
letter_month_string = 'FGHJKMNQUVXZ'

futures_contract_months = {'LN': ['G', 'J', 'K', 'M', 'N', 'Q', 'V', 'Z'],
                           'LC': ['G', 'J', 'M', 'Q', 'V', 'Z'],
                           'FC': ['F', 'H', 'J', 'K', 'Q', 'U', 'V', 'X'],
                           'C': ['H', 'K', 'N', 'U', 'Z'],
                           'S': ['F', 'H', 'K', 'N', 'Q', 'U', 'X'],
                           'SM': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
                           'BO': ['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'],
                           'W': ['H', 'K', 'N', 'U', 'Z'],
                           'KW': ['H', 'K', 'N', 'U', 'Z'],
                           'SB': ['H', 'K', 'N', 'V'],
                           'KC': ['H', 'K', 'N', 'U', 'Z'],
                           'CC': ['H', 'K', 'N', 'U', 'Z'],
                           'CT': ['H', 'K', 'N', 'V', 'Z'],
                           'OJ': ['F', 'H', 'K', 'N', 'U', 'X'],
                           'ED': full_letter_month_list,
                           'CL': full_letter_month_list,
                           'B' : full_letter_month_list,
                           'HO': full_letter_month_list,
                           'RB': full_letter_month_list,
                           'NG': full_letter_month_list,
                           'ES': quarterly_month_list,
                           'NQ': quarterly_month_list,
                           'EC': quarterly_month_list,
                           'JY': quarterly_month_list,
                           'AD': quarterly_month_list,
                           'CD': quarterly_month_list,
                           'BP': quarterly_month_list,
                           'TY': quarterly_month_list,
                           'US': quarterly_month_list,
                           'FV': quarterly_month_list,
                           'TU': quarterly_month_list,
                           'GC': full_letter_month_list,
                           'SI': full_letter_month_list}

option_cabinet_values = {'ED': 0.0025,
                         'E0': 0.0025,
                         'E2': 0.0025,
                         'E3': 0.0025,
                         'E4': 0.0025,
                         'E5': 0.0025,
                         'LC': 0.0125,
                         'LN': 0.0125,
                         'FC': 0.0125,
                         'ES': 0.05,
                         'NQ': 0.05,
                         'TU': 1/128,
                         'FV': 1/128,
                         'TY': 1/64,
                         'US': 1/64,
                         'EC': 0.00005,
                         'JY': 5,
                         'AD': 0.00005,
                         'CD': 0.00005,
                         'BP': 0.0001}


def get_option_contract_months(**kwargs):

    ticker_head = kwargs['ticker_head']
    contract_months = []

    if ticker_head == 'LN':
        contract_months = ['G', 'J', 'K', 'M', 'N', 'Q', 'V', 'Z']
    elif ticker_head in option_tickerhead_list:
        contract_months = full_letter_month_list

    return contract_months

futures_butterfly_strategy_tickerhead_list = ['LN', 'LC', 'FC',
                                              'C', 'S', 'SM', 'BO', 'W', 'KW',
                                              'SB', 'KC', 'CC', 'CT', 'OJ',
                                              'CL', 'B', 'HO', 'RB', 'NG', 'ED']

cme_futures_tickerhead_list = ['LN', 'LC', 'FC',
                               'C', 'S', 'SM', 'BO', 'W', 'KW',
                               'CL', 'B', 'HO', 'RB', 'NG', 'ED',
                               'ES', 'NQ', 'EC', 'JY', 'AD', 'CD', 'BP',
                               'TY', 'US', 'FV', 'TU', 'GC', 'SI']

cme_option_tickerhead_list = ['LN', 'LC', 'ES', 'EC', 'JY', 'AD', 'CD', 'BP', 'GC', 'SI',
                          'TY', 'US', 'FV', 'TU', 'C', 'S', 'SM', 'BO', 'W', 'CL', 'NG',
                          'ED', 'E0', 'E2', 'E3', 'E4', 'E5']

option_tickerhead_list = cme_option_tickerhead_list


contract_name = {'LN': 'Lean Hog',
                 'LC': 'Live Cattle',
                 'FC': 'Feeder Cattle',
                 'C': 'Corn',
                 'S': 'Soybean',
                 'SM': 'Soybean Meal',
                 'BO': 'Soybean Oil',
                 'W': 'Chicago SRW Wheat',
                 'KW': 'Kansas HRW Wheat',
                 'SB': 'Sugar 11',
                 'KC': 'Coffee C',
                 'CC': 'Cocoa',
                 'CT': 'Cotton No. 2',
                 'OJ': 'FCOJ-A',
                 'CL': 'Crude Oil',
                 'B' : 'Brent Crude',
                 'HO': 'NY Harbor ULSD',
                 'RB': 'RBOB Gasoline',
                 'NG': 'Henry Hub Natural Gas',
                 'ED': 'Eurodollar',
                 'E0': 'Eurodollar 1 Year Mid-Curve Options',
                 'E2': 'Eurodollar 2 Year Mid-Curve Options',
                 'E3': 'Eurodollar 3 Year Mid-Curve Options',
                 'E4': 'Eurodollar 4 Year Mid-Curve Options',
                 'E5': 'Eurodollar 5 Year Mid-Curve Options',
                 'ES': 'E-mini S&P 500',
                 'NQ': 'E-mini Nasdaq 100',
                 'EC': 'Euro FX',
                 'JY': 'Japanese Yen',
                 'AD': 'Australian Dollar',
                 'CD': 'Canadian Dollar',
                 'BP': 'British Pound',
                 'TY': '10-Year T-Note',
                 'US': 'US Treasury Bond',
                 'FV': '5-Year T-Note',
                 'TU': '2-Year T-Note',
                 'GC': 'Gold',
                 'SI': 'Silver'}

relevant_max_cal_dte = {'LN': 720,
                        'LC': 720,
                        'FC': 720,
                        'C': 720,
                        'S': 720,
                        'SM': 720,
                        'BO': 720,
                        'W': 720,
                        'KW': 720,
                        'SB': 720,
                        'KC': 720,
                        'CC': 720,
                        'CT': 720,
                        'OJ': 720,
                        'CL': 720,
                        'B' : 720,
                        'HO': 720,
                        'RB': 720,
                        'NG': 720,
                        'ED': 1440}


def get_max_cal_dte(**kwargs):

    ticker_head = kwargs['ticker_head']
    ticker_month = kwargs['ticker_month']

    if ticker_head in ['LN', 'LC', 'FC', 'C', 'S', 'SM', 'BO', 'W', 'KW', 'SB', 'KC', 'CC', 'CT', 'OJ', 'HO', 'RB', 'NG']:
        max_cal_dte = 720
    elif ticker_head in ['ED', 'E0','E2', 'E3', 'E4', 'E5']:
        max_cal_dte = 1440
    elif ticker_head == 'CL':
        if ticker_month in [6, 12]:
            max_cal_dte = 1080
        else:
            max_cal_dte = 720
    elif ticker_head == 'B':
        if ticker_month in [6, 12]:
            max_cal_dte = 1440
        else:
            max_cal_dte = 720
    elif ticker_head in ['ES', 'NQ', 'TY', 'US', 'FV', 'TU', 'GC', 'SI']:
        max_cal_dte = 360
    elif ticker_head in ['EC', 'JY', 'AD', 'CD', 'BP']:
        max_cal_dte = 720

    return max_cal_dte

ticker_class = {'LN': 'Livestock',
                'LC': 'Livestock',
                'FC': 'Livestock',
                'ES': 'Index',
                'NQ': 'Index',
                'EC': 'FX',
                'JY': 'FX',
                'AD': 'FX',
                'CD': 'FX',
                'BP': 'FX',
                'GC': 'Metal',
                'SI': 'Metal',
                'TY': 'Treasury',
                'US': 'Treasury',
                'FV': 'Treasury',
                'TU': 'Treasury',
                'C': 'Ag',
                'S': 'Ag',
                'SM': 'Ag',
                'BO': 'Ag',
                'W': 'Ag',
                'KW': 'Ag',
                'SB': 'Soft',
                'KC': 'Soft',
                'CC': 'Soft',
                'CT': 'Soft',
                'OJ': 'Soft',
                'CL': 'Energy',
                'B' : 'Energy',
                'HO': 'Energy',
                'RB': 'Energy',
                'NG': 'Energy',
                'ED': 'STIR',
                'E0': 'STIR',
                'E2': 'STIR',
                'E3': 'STIR',
                'E4': 'STIR',
                'E5': 'STIR'}

contract_multiplier = {'LN': 400,
                       'LC': 400,
                       'FC': 500,
                       'C': 50,
                       'S': 50,
                       'SM': 100,
                       'BO': 600,
                       'W': 50,
                       'KW': 50,
                       'SB': 1120,
                       'KC': 375,
                       'CC': 10,
                       'CT': 500,
                       'OJ': 150,
                       'CL': 1000,
                       'B': 1000,
                       'HO': 42000,
                       'RB': 42000,
                       'NG': 10000,
                       'ED': 2500,
                       'ES': 50,
                       'NQ': 20,
                       'AD': 100000,
                       'CD': 100000,
                       'EC': 125000,
                       'JY': 1.25,
                       'BP': 62500,
                       'FV': 1000,
                       'TU': 2000,
                       'TY': 1000,
                       'US': 1000,
                       'GC': 100,
                       'SI': 5000}
t_cost = {'CL': 0.80,
          'NG': 0.78,
          'B': 0.93,
          'HO': 0.80,
          'LC': 0.61,
          'LN': 0.61,
          'FC': 0.61,
          'RB': 0.80,
          'C': 0.61,
          'W': 0.61,
          'S': 0.61,
          'SM': 0.61,
          'BO': 0.61,
          'KW': 0.61,
          'ED': 0.29,
          'KC': 2.1,
          'CC': 2.1,
          'SB': 2.1,
          'CT': 2.1}

def get_contract_specs(ticker):
    return {'ticker_head': ticker[:-5],
            'ticker_year': int(ticker[-4:]),
            'ticker_month_str': ticker[-5],
            'ticker_month_num': letter_month_string.find(ticker[-5])+1,
            'ticker_class': ticker_class[ticker[:-5]],
            'cont_indx': 100*int(ticker[-4:]) + letter_month_string.find(ticker[-5])+1}


def get_month_seperation_from_cont_indx(cont_indx1, cont_indx2):

    contract_month1 = cont_indx1%100
    contract_month2 = cont_indx2%100
    contract_year1 = m.floor(cont_indx1/100)
    contract_year2 = m.floor(cont_indx2/100)

    return 12*(contract_year1-contract_year2)+(contract_month1-contract_month2)
