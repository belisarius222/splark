import itertools
import sys
import traceback

from splark import Master, RDD
from splark.worker import Worker


class SplarkContext:
    def __init__(self, master=None, num_workers=4, **kwargs):
        if master is not None:
            self.master = master
        else:
            self.master = Master()
        self.kwargs = kwargs
        self.default_parallelism = kwargs.get("default_parallelism", num_workers)

        self.master.wait_for_worker_connections(num_workers)

    def parallelize(self, iterable, num_partitions=None, rdd_class=RDD):
        num_partitions = num_partitions or self.default_parallelism
        rdd = rdd_class(self.master, num_partitions)
        rdd.parallelize(iterable)
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


class ProcessWorkerContext:
    def __init__(self, workport=23456, logport=34567, num_workers=4):
        self.workport = workport
        self.logport = logport
        self.num_workers = num_workers

    def __enter__(self):
        self.master = Master(workport=self.workport, logport=self.logport)

        self.workers = []
        for i in range(self.num_workers):
            worker = Worker("tcp://localhost", workport=self.workport, logport=self.logport)
            self.workers.append(worker)
            worker.start()

        self.context = SplarkContext(master=self.master, num_workers=self.num_workers)
        print("Created context.")

        return self.context, self.workers

    def __exit__(self, *args):
        if any(arg is not None for arg in args):
            print("ContextWithWorkers.__exit__() caught an error.", file=sys.stderr)
            traceback.print_exc()

        dead_workers = [worker for worker in self.workers if not worker.is_alive()]
        if len(dead_workers) > 0:
            raise RuntimeError("Workers {} died during testing.".format(dead_workers))
            for worker in self.workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join()

        print("Killing workers.")
        self.master.kill_workers()
        self.master.release_ports()
        print("Closed master sockets.", flush=True)
