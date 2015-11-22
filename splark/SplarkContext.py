from splark import Master, RDD


class SplarkContext:
    def __init__(self, master=None, **kwargs):
        self.kwargs = kwargs
        if master is not None:
            self.master = master
        else:
            self.master = Master()

        self.default_parallelism = kwargs.get("default_parallelism", 4)

        self.master.wait_for_workers(4)

    def parallelize(self, iterable, num_partitions=None):
        num_partitions = num_partitions or self.default_parallelism
        rdd = RDD(self.master)
        self.master.set_data(rdd.id, iterable)
        return rdd
