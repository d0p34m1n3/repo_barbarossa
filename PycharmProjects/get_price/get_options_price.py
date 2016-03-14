__author__ = 'kocat_000'

import shared.directory_names as dn
import scipy.io
import pandas as pd

def get_column_names_4option_data():

    fwd_looking_window_list = [5, 10, 20, 40, 60]

    optional_cc_skew_columns = ['vegaPnL', 'pastAtmVol1',
            'atmThetaBs','atmVegaBs','atmVegaOu','atmGammaBs',
            'Kvol','LogPrice','SquaredPrice', 'KnsImpBsSkew', 'KnsImpOuSkew',
            'MFIVolPath', 'BkmImpSkewness', 'BkmImpKurtosis',
            'ImpSkewStickinessRatio','SkewStickinessRatio',
            'dImpVolDk', 'BSHistKnsRzVar', 'OUHistKnsRzVar',
            'KnsRzBsSkew', 'DRzOuSkewness', 'DRzSkewness','DRzKurtosis', 'BiPowerVol',
            'BSHistBkmImpBipowerRzVolPremAnn','BSHistSkewPremiumAnn','BSHistScaledKurtPremiumAnn',
            'HistKnsImpVar','HistKnsImpSkew','HistKnsImpKurt' , 'contINDX', 'optBpvs',
            'yzVol20', 'timeValue', 'pastAtmVol20', 'pastAtmVolChange20', 'pastAtmVolChange1', 'expectedDailyMove',
             'optionPnL', 'deltaPnL', 'gammaPnL', 'thetaPnL', 'dSpot', 'Theta30days']

    default_aligned_data_column_names = ['settleDates', 'TickerYear', 'TickerMonth', 'trDTE', 'calDTE',
        'underlying', 'atmVol', 'strike', 'theoValue', 'theoPremium',
        'impVol', 'delta', 'gamma', 'vega', 'theta', 'rate2OptExp',
        'dollarVega', 'dollarTheta', 'normDollarTheta', 'dollarGamma', 'dailyPnL', 'dailyUnhedgedPnL',
        'close2CloseVol10', 'close2CloseVol20', 'close2CloseVol60',
        'dailySdMove', 'success']

    window_fields = sum([['profit'+str(x), 'unhedgedProfit'+str(x), 'onceHedgedProfit'+str(x),
                         'underlyingProfit'+str(x), 'vegaProfit'+str(x), 'gammaProfit'+str(x),
                         'thetaProfit'+str(x), 'impVolFuture'+str(x), 'atmVolFuture'+str(x),
                         'futureAtmVolChange'+str(x), 'spotFuture'+str(x)] for x in fwd_looking_window_list],[])

    return default_aligned_data_column_names + window_fields + optional_cc_skew_columns


def load_aligend_options_data_file(**kwargs):

    ticker_head = kwargs['ticker_head']
    tr_dte_center = kwargs['tr_dte_center']

    option_data_dir = dn.get_directory_name(ext='aligned_time_series_output')

    if 'delta_center' in kwargs.keys():
        delta_center = kwargs['delta_center']
    else:
        delta_center = 0.5

    if 'model' in kwargs.keys():
        model = kwargs['model']
    else:
        model = 'BS'

    if 'contract_month_letter' in kwargs.keys():
        contract_month_str = '_' + kwargs['contract_month_letter']
    else:
        contract_month_str = ''

    if 'column_names' in kwargs.keys():
        column_names = kwargs['column_names']
    else:
        column_names = get_column_names_4option_data()

    file_dir = ticker_head + '_' + str(delta_center) + '_' + model + '_20_510204060_' + str(tr_dte_center) + contract_month_str + '.mat'

    mat_output = scipy.io.loadmat(option_data_dir+'/'+file_dir)

    return pd.DataFrame(mat_output['alignedDataMatrix'],columns=column_names)


