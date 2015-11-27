import itertools
from functools import reduce
from uuid import uuid4


class RDD:
    def __init__(self, master, num_partitions, map_func=None, map_func_id=None, previous_RDDs=None):
        self.master = master
        self.num_partitions = num_partitions
        self.id = str(uuid4()).encode("ascii")
        self.map_func_id = map_func_id
        self.map_func = map_func
        self.previous_RDDs = previous_RDDs
        # If this is the first RDD (i.e., the result of a parallelize() call),
        # then nothing needs to be done to evaluate us.
        self.is_evaluated = True if (previous_RDDs is None) else False

    def parallelize(self, iterator):
        partitioned_data_iterator = self.partition(iterator)
        print(partitioned_data_iterator)
        self.master.set_data(self.id, partitioned_data_iterator)

    def map(self, func):
        def map_func(partition):
            print("partition", partition)
            return [func(element) for element in partition]

        return self.mapPartitions(map_func)

    def reduce(self, f):
        def func(iterator):
            iterator = iter(iterator)
            try:
                initial = next(iterator)
            except StopIteration:
                return
            return reduce(f, iterator, initial)

        vals = self.mapPartitions(func).collect()
        if vals:
            return reduce(f, vals)
        raise ValueError("Can not reduce() empty RDD")

    def mapPartitions(self, func):
        map_func_id = str(uuid4()).encode("ascii")
        return self.__class__(self.master, self.num_partitions, map_func=func, map_func_id=map_func_id, previous_RDDs=(self,))

    def collect(self):
        self.evaluate()
        partitioned_data = self.master.get_data(self.id)
        return self.unpartition(partitioned_data)

    def evaluate(self):
        if self.is_evaluated:
            return self

        for rdd in self.previous_RDDs:
            rdd.evaluate()

        self.master.set_data(self.map_func_id, itertools.repeat(self.map_func))
        previous_ids = tuple(rdd.id for rdd in self.previous_RDDs)
        self.master.map(self.map_func_id, previous_ids, self.id)
        self.master.wait_for_workers_to_finish()

        self.is_evaluated = True
        return self

    def partition(self, data):
        # TODO: figure out how to have this return an iterator, so that
        # parallelize() can be called on iterators that don't fit in memory.
        data_as_list = list(data)
        return [data_as_list[i::self.num_partitions] for i in range(self.num_partitions)]

    def unpartition(self, data):
        is_list_of_lists = "__len__" in dir(data[0])
        if not is_list_of_lists:
            return data

        rdd_length = sum(len(p) for p in data)
        ret = []
        for i in range(rdd_length):
            index_in_partition, partition_index = divmod(i, self.num_partitions)
            ret.append(data[partition_index][index_in_partition])

        return ret
