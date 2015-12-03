# import itertools, sys, traceback
# from functools import reduce
from nose.tools import timed

from splark.SplarkContext import ProcessWorkerContext
from splark.tests import getTestingWorkPort, getTestingLogPort


class TestingProcessWorkerContext(ProcessWorkerContext):
    def __init__(self, **kwargs):
        ProcessWorkerContext.__init__(self, workport=getTestingWorkPort(), logport=getTestingLogPort())


@timed(2)
def test_testing_context():
    with TestingProcessWorkerContext() as (sc, workers):
        pass


# @timed(10)
# def test_parallelize_and_collect():
#     with TestingContext() as (sc, workers):
#         data = list(range(10))
#         rdd = sc.parallelize(data)
#         print("Parallelized data.")
#         collected = rdd.collect()
#         assert collected == data, collected


# @timed(10)
# def test_map_and_collect():
#     with TestingContext() as (sc, workers):
#         data = list(range(10))
#         fun = lambda x: x + 1
#         rdd = sc.parallelize(data)
#         rdd2 = rdd.map(fun)
#         collected = rdd2.collect()
#         assert collected == list(map(fun, data)), collected


# @timed(10)
# def test_reduce():
#     with TestingContext() as (sc, workers):
#         data = list(range(10))
#         rdd = sc.parallelize(data)

#         fun = lambda x, y: x + y
#         reduced = rdd.reduce(fun)
#         assert reduced == reduce(fun, data), reduced
