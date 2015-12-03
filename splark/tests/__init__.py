import itertools

from splark.tests.DummyMaster import DummyMaster


workport = itertools.count(23456)
logport = itertools.count(34567)


def getTestingWorkPort():
    n = next(workport)
    print(n)
    return n


def getTestingLogPort():
    n = next(logport)
    print(">>>>>>>>>>>>>>>>>Testing Port Log:", n)
    return n

__all__ = [DummyMaster, getTestingLogPort, getTestingWorkPort]
