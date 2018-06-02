
import ta.strategy as ts
import contract_utilities.contract_meta_info as cmi

def get_ocs_position(**kwargs):

    net_position = ts.get_net_position_4strategy_alias(**kwargs)

    empty_position_q = False
    correct_position_q = False
    scale = 0

    if len(net_position.index) == 0:
        empty_position_q = True
        return {'empty_position_q':empty_position_q, 'correct_position_q': correct_position_q, 'scale': scale,'sorted_position': net_position}

    net_position['cont_indx'] = [cmi.get_contract_specs(x)['cont_indx'] for x in net_position['ticker']]
    net_position.sort_values('cont_indx', ascending=True, inplace=True)

    if (len(net_position.index) == 2) & (net_position['qty'].sum() == 0):
        correct_position_q = True
        scale = net_position['qty'].iloc[0]

    return {'empty_position_q': empty_position_q, 'correct_position_q': correct_position_q, 'scale': scale,'sorted_position': net_position}

