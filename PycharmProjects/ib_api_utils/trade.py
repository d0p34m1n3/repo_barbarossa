
from ibapi.order import (OrderComboLeg, Order)


def ComboLimitOrder(action: str, quantity: float, limitPrice: float,
                    nonGuaranteed: bool):
    # ! [combolimit]
    order = Order()
    order.action = action
    order.orderType = "LMT"
    order.totalQuantity = quantity
    order.lmtPrice = limitPrice
    if nonGuaranteed:
        order.smartComboRoutingParams = []
        order.smartComboRoutingParams.append(TagValue("NonGuaranteed", "1"))

    # ! [combolimit]
    return order