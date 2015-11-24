import random

from pickle import loads
from splark.cloudpickle import dumps


def randomID(length=10):
    return "".join([random.choice("abcdef1234567890") for i in range(10)])


def toCP(pyObj):
    return dumps(pyObj)


def fromCP(pklBytes):
    return loads(pklBytes)
