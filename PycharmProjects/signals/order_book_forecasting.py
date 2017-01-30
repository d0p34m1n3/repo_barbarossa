
import reformat_intraday_data.reformat_ttapi_intraday_data as rtid


def get_orderbook_signals(**kwargs):

    snapshot_data = rtid.get_book_snapshot_4ticker(ticker=kwargs['ticker'],folder_date=kwargs['date_to'], freq_str='S')
    snapshot_data['time_stamp'] = [x.to_datetime() for x in snapshot_data.index]
    snapshot_data['hour_minute'] = [100*x.hour+x.minute for x in snapshot_data['time_stamp']]
    snapshot_data = snapshot_data[(snapshot_data['hour_minute']>830)&(snapshot_data['hour_minute']<1320)]

    snapshot_data['mid_p'] = (snapshot_data['best_bid_p']+snapshot_data['best_ask_p'])/2
    snapshot_data['target'] = 0

    for i in range(1, 121):
        snapshot_data['mid_p_d' + str(i)] = snapshot_data['mid_p'].shift(-i)-snapshot_data['mid_p']
        snapshot_data['target'] = snapshot_data['target'] + snapshot_data['mid_p_d' + str(i)]

    snapshot_data['target'] = snapshot_data['target']/10

    snapshot_data = snapshot_data[(snapshot_data['best_bid_q'].notnull()) &(snapshot_data['best_ask_q'].notnull())]

    snapshot_data['best_bid_q'] = snapshot_data['best_bid_q'].astype('int')
    snapshot_data['best_ask_q'] = snapshot_data['best_ask_q'].astype('int')

    snapshot_data['best_bid_p_1'] = snapshot_data['best_bid_p'].shift(1)
    snapshot_data['best_ask_p_1'] = snapshot_data['best_ask_p'].shift(1)

    snapshot_data['best_bid_q_d1'] = snapshot_data['best_bid_q']-snapshot_data['best_bid_q'].shift(1)
    snapshot_data['best_ask_q_d1'] = snapshot_data['best_ask_q']-snapshot_data['best_ask_q'].shift(1)

    snapshot_data.loc[snapshot_data['best_bid_p']>snapshot_data['best_bid_p_1'],'best_bid_q_d1'] = \
    snapshot_data.loc[snapshot_data['best_bid_p']>snapshot_data['best_bid_p_1'],'best_bid_q']

    snapshot_data.loc[snapshot_data['best_bid_p']<snapshot_data['best_bid_p_1'],'best_bid_q_d1'] = 0

    snapshot_data.loc[snapshot_data['best_ask_p']<snapshot_data['best_ask_p_1'],'best_ask_q_d1'] = \
    snapshot_data.loc[snapshot_data['best_ask_p']<snapshot_data['best_ask_p_1'],'best_ask_q']

    snapshot_data.loc[snapshot_data['best_ask_p']>snapshot_data['best_ask_p_1'],'best_ask_q_d1'] = 0

    snapshot_data['voi'] = snapshot_data['best_bid_q_d1']-snapshot_data['best_ask_q_d1']
    snapshot_data['voi1'] = snapshot_data['voi'].shift(1)
    snapshot_data['voi2'] = snapshot_data['voi'].shift(2)
    snapshot_data['voi3'] = snapshot_data['voi'].shift(3)
    snapshot_data['voi4'] = snapshot_data['voi'].shift(4)
    snapshot_data['voi5'] = snapshot_data['voi'].shift(5)

    snapshot_data['oir'] = (snapshot_data['best_bid_q']-snapshot_data['best_ask_q'])/(snapshot_data['best_bid_q']+snapshot_data['best_ask_q'])
    snapshot_data['oir1'] = snapshot_data['oir'].shift(1)
    snapshot_data['oir2'] = snapshot_data['oir'].shift(2)
    snapshot_data['oir3'] = snapshot_data['oir'].shift(3)
    snapshot_data['oir4'] = snapshot_data['oir'].shift(4)
    snapshot_data['oir5'] = snapshot_data['oir'].shift(5)

    snapshot_data = snapshot_data[(snapshot_data['voi5'].notnull())
                              &(snapshot_data['target'].notnull())]

    return snapshot_data
