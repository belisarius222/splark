# import itertools, sys, traceback
from functools import reduce

from splark.SplarkContext import TestingContext


def test_parallelize_and_collect():
    with TestingContext() as (sc, master, workers):
        data = list(range(10))
        rdd = sc.parallelize(data)
        print("Parallelized data.")
        collected = rdd.collect()
        assert collected == data, collected


def test_map_and_collect():
    with TestingContext() as (sc, master, workers):
        data = list(range(10))
        fun = lambda x: x + 1
        rdd = sc.parallelize(data)
        rdd2 = rdd.map(fun)
        collected = rdd2.collect()
        assert collected == list(map(fun, data)), collected


def test_reduce():
    with TestingContext() as (sc, master, workers):
        data = list(range(10))
        rdd = sc.parallelize(data)

        fun = lambda x, y: x + y
        reduced = rdd.reduce(fun)
        assert reduced == reduce(fun, data), reduced
