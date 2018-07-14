
import contract_utilities.contract_meta_info as cmi
import opportunity_constructs.utilities as opUtil
import get_price.get_futures_price as gfp
import numpy as np


def calculate_contract_risk(**kwargs):

    contract_frame = kwargs['contract_frame']
    current_date = kwargs['current_date']
    contract_frame['risk'] = np.nan
    contract_frame['contract_multiplier'] = np.nan

    for i in range(len(contract_frame.index)):

        ticker_raw = contract_frame['ticker'].iloc[i]
        ticker_output = ticker_raw.split('-')
        contract_multiplier = cmi.contract_multiplier[contract_frame['ticker_head'].iloc[i]]
        contract_frame['contract_multiplier'].iloc[i] = contract_multiplier

        if len(ticker_output) == 1:
            ticker = ticker_output[0]
            data_out = gfp.get_futures_price_preloaded(ticker=ticker, settle_date_to=current_date)
            recent_data = data_out.iloc[-10:]
            contract_frame['risk'].iloc[i] = contract_multiplier * (recent_data['close_price'].max() - recent_data['close_price'].min())

        if len(ticker_output) > 1:
            aligned_output = opUtil.get_aligned_futures_data(contract_list=ticker_output, contracts_back=1,
                                                             aggregation_method=12, date_to=current_date)

            aligned_data = aligned_output['aligned_data']
            recent_data = aligned_data.iloc[-10:]
            spread_price = recent_data['c1']['close_price'] - recent_data['c2']['close_price']
            contract_frame['risk'].iloc[i] = contract_multiplier * (spread_price.max() - spread_price.min())

    return contract_frame



