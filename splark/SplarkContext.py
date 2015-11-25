import sys, traceback

from splark import Master, RDD


class SplarkContext:
    def __init__(self, master=None, num_workers=4, **kwargs):
        if master is not None:
            self.master = master
        else:
            self.master = Master()
        self.kwargs = kwargs
        self.default_parallelism = kwargs.get("default_parallelism", num_workers)

        self.master.wait_for_worker_connections(num_workers)

    def parallelize(self, iterable, num_partitions=None):
        num_partitions = num_partitions or self.default_parallelism
        rdd = RDD(self.master)
        self.master.set_data(rdd.id, iterable)
        return rdd

    def stop(self):
        self.master.kill_workers()

    def __enter__(self):
        return self

    def __exit__(self, *traceback_info):
        if all(arg is None for arg in traceback_info):
            return

        print("Error inside SplarkContext, exiting.", file=sys.stderr)
        traceback.print_exc()
        self.stop()
