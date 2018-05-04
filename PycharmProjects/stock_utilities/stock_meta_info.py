
import contract_utilities.expiration as exp
import get_price.save_stock_data as ssd

def get_ib_exchange_name(ticker):

    last_settle_date = exp.doubledate_shift_bus_days()
    symbol_frame = ssd.get_symbol_frame(frame_type='other', settle_date=last_settle_date)
    selected_frame = symbol_frame[symbol_frame['ACT Symbol'] == ticker]

    if len(selected_frame.index) == 0:
        exchange_name = 'SMART'
    else:
        exchange_code = selected_frame['Exchange'].iloc[0]
        exchange_name = ''

        if exchange_code=='A':
            exchange_name = 'AMEX'
        elif exchange_code=='N':
            exchange_name = 'NYSE'

    return exchange_name

def get_ib_t_cost(**kwargs):

    price = kwargs['price']
    quantity = kwargs['quantity']

    t_cost = 0.005*quantity
    t_cost = max(t_cost, 1)
    t_cost = min(t_cost,price*quantity*0.01)
    t_cost = max(t_cost, 1)

    return t_cost




