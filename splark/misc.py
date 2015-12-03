import random

from pickle import loads
from splark.cloudpickle import dumps


def randomID(length=10):
    return "".join([random.choice("abcdef1234567890") for i in range(10)])


def toCP(pyObj):
    return dumps(pyObj)


def fromCP(pklBytes):
    return loads(pklBytes)


# MRG NOTE: Use futures here instead?
class TinyPromise:
    def __init__(self, isReady, getResult, wrapped=False):
        self.isReady = isReady
        if wrapped:
            self.getResult = lambda: (getResult(),)
        else:
            self.getResult = getResult

    def __add__(self, otherPromise):
        bothReady = lambda: self.isReady() and otherPromise.isReady()
        results = lambda: self.getResult() + otherPromise.getResult()
        return TinyPromise(bothReady, results, wrapped=True)


class EmptyPromise:
    def isReady(self):
        return True

    def getResult(self):
        return None

    def __add__(self, otherPromise):
        return otherPromise

    def __radd__(self, otherPromise):
        return otherPromise
