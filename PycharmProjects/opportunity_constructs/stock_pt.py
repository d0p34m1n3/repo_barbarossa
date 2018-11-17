
import fundamental_data.sector_data as sd
import signals.stock_pairs_trading as spt
import pandas as pd
import os.path
import ta.strategy as ts

def get_spt_sheet_4date(**kwargs):
    report_date = kwargs['report_date']

    output_dir = ts.create_strategy_output_dir(strategy_class='spt', report_date=report_date)

    if os.path.isfile(output_dir + '/summary.pkl'):
        pairs_frame = pd.read_pickle(output_dir + '/summary.pkl')
        return {'pairs_frame': pairs_frame, 'success': True}


    sector_frame = sd.create_sector_classification_file()
    select_frame = sector_frame[sector_frame['industry'] == 'Semiconductors']
    signal_output_list = []
    ticker1_list = []
    ticker2_list = []

    for i in range(len(select_frame.index) - 1):
        for j in range(i + 1, len(select_frame.index)):
            print(select_frame['ticker'].iloc[i] + ', ' + select_frame['ticker'].iloc[j])
            ticker1_list.append(select_frame['ticker'].iloc[i])
            ticker2_list.append(select_frame['ticker'].iloc[j])
            signal_output_list.append(spt.get_summary(symbol1=select_frame['ticker'].iloc[i],
                                                      symbol2=select_frame['ticker'].iloc[j],
                                                      report_date=report_date))

    pairs_frame = pd.DataFrame()
    pairs_frame['ticker1'] = ticker1_list
    pairs_frame['ticker2'] = ticker2_list
    pairs_frame['price1'] = [x['price1'] for x in signal_output_list]
    pairs_frame['price2'] = [x['price2'] for x in signal_output_list]
    pairs_frame['zScore'] = [x['zScore'] for x in signal_output_list]
    pairs_frame['p1'] = [x['p_value_1'] for x in signal_output_list]
    pairs_frame['p2'] = [x['p_value_2'] for x in signal_output_list]
    pairs_frame['beta1'] = [x['beta_1'] for x in signal_output_list]
    pairs_frame['beta2'] = [x['beta_2'] for x in signal_output_list]
    pairs_frame['corr'] = [x['corr'] for x in signal_output_list]
    pairs_frame['cagr1'] = [x['cagr1'] for x in signal_output_list]
    pairs_frame['cagr2'] = [x['cagr2'] for x in signal_output_list]
    pairs_frame['kalpha'] = [x['kalpha'] for x in signal_output_list]
    pairs_frame['kbeta'] = [x['kbeta'] for x in signal_output_list]
    pairs_frame['meanSpread'] = [x['meanSpread'] for x in signal_output_list]
    pairs_frame['stdSpread'] = [x['stdSpread'] for x in signal_output_list]

    pairs_frame.to_pickle(output_dir + '/summary.pkl')

    return {'pairs_frame': pairs_frame, 'success': True}

def backtest_spt():

    report_date = 20181102

    sector_frame = sd.create_sector_classification_file()
    select_frame = sector_frame[sector_frame['industry'] == 'Semiconductors']
    signal_output_list = []
    ticker1_list = []
    ticker2_list = []

    pnl_frame = pd.DataFrame()

    for i in range(len(select_frame.index) - 1):
        for j in range(i + 1, len(select_frame.index)):
            print(select_frame['ticker'].iloc[i] + ', ' + select_frame['ticker'].iloc[j])

            signal_output = spt.get_summary(symbol1=select_frame['ticker'].iloc[i], symbol2=select_frame['ticker'].iloc[j], report_date=report_date,
                                            get_diagnosticQ=True)

            if signal_output['cagr2']>=4:
                if len(pnl_frame.index)==0:
                    pnl_frame = signal_output['backtest_output']['data_frame']['pnl']
                else:
                    pnl_frame = pnl_frame + signal_output['backtest_output']['data_frame']['pnl']

    return pnl_frame













