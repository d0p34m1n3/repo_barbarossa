
import numpy as np
import math as mth

def get_midprice(**kwargs):

    bid_price = kwargs['bid_price']
    ask_price = kwargs['ask_price']

    bid_quantity = kwargs['bid_quantity']
    ask_quantity = kwargs['ask_quantity']

    tick_size = kwargs['tick_size']
    direction = kwargs['direction']

    bid_ask_in_ticks = int(round((ask_price - bid_price) / tick_size))
    half_bid_ask = int(mth.floor(bid_ask_in_ticks / 2))

    limit_price = np.nan

    if direction>0:
        if ask_quantity < (bid_quantity / 2):
            limit_price = bid_price + (half_bid_ask + 1) * tick_size
        else:
            limit_price = bid_price + half_bid_ask * tick_size
    elif direction<0:
        if bid_quantity < (ask_quantity / 2):
            limit_price = ask_price - (half_bid_ask + 1) * tick_size
        else:
            limit_price = ask_price - half_bid_ask * tick_size

    return limit_price



