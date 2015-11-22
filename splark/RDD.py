from functools import reduce
from uuid import uuid4


class RDD:
    def __init__(self, master, map_func=None, previous_RDDs=None):
        self.master = master
        self.id = str(uuid4())
        self.map_func = map_func
        self.previous_RDDs = previous_RDDs
        # If this is the first RDD (i.e., the result of a parallelize() call),
        # then nothing needs to be done to evaluate us.
        self.is_evaluated = True if (previous_RDDs is None) else False

    def map(self, func):
        def map_func(partitionIndex, partition):
            return [func(element) for element in partition]

        return self.mapPartitionsWithIndex(map_func)

    def reduce(self, f):
        def func(iterator):
            iterator = iter(iterator)
            try:
                initial = next(iterator)
            except StopIteration:
                return
            yield reduce(f, iterator, initial)

        vals = self.mapPartitions(func).collect()
        if vals:
            return reduce(f, vals)
        raise ValueError("Can not reduce() empty RDD")

    def mapPartitions(self, func):
        def map_func(partitionIndex, partition):
            return func(partition)
        return self.mapPartitionsWithIndex(map_func)

    def mapPartitionsWithIndex(self, func):
        return self.__class__(self.master, map_func=func, previous_RDDs=(self,))

    def collect(self):
        self.evaluate()
        return self.master.get_data(self.id)

    def evaluate(self):
        if self.is_evaluated:
            return

        for rdd in self.previous_RDDs:
            rdd.evaluate()

        self.master.map(self.map_func, [rdd.id for rdd in self.previous_RDDs], self.id)
        self.is_evaluated = True
        return self
