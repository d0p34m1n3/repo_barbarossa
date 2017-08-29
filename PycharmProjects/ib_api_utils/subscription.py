

from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.utils import iswrapper #just for decorator
from ibapi.contract import *
import numpy as np
import argparse
import collections
import inspect


class subscription(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, self)
        self.started = False
        self.next_val_id = None

    def connect(self,**kwargs):
        super().connect("127.0.0.1", 7496, clientId=kwargs['client_id'])

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        self.next_val_id = orderId
        # ! [nextvalidid]

        # we can start now
        self.start()

    def next_valid_id(self):
        oid = self.next_val_id
        self.next_val_id += 1
        return oid

    def start(self):
        if self.started:
            return

        self.started = True
        self.main_run()

