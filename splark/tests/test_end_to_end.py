import itertools, sys, traceback
from functools import reduce

from splark import Master, SplarkContext, Worker

workport = itertools.count(23456)
logport = itertools.count(34567)


class ContextWithWorkers:
    def __init__(self, num_workers=4):
        self.workport = next(workport)
        self.logport = next(logport)
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

        return self.context, self.master, self.workers

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


def test_parallelize_and_collect():
    with ContextWithWorkers() as (sc, master, workers):
        data = list(range(10))
        rdd = sc.parallelize(data)
        print("Parallelized data.")
        collected = rdd.collect()
        assert collected == data, collected


def test_map_and_collect():
    with ContextWithWorkers() as (sc, master, workers):
        data = list(range(10))
        fun = lambda x: x + 1
        rdd = sc.parallelize(data)
        rdd2 = rdd.map(fun)
        collected = rdd2.collect()
        assert collected == list(map(fun, data)), collected


def test_reduce():
    with ContextWithWorkers() as (sc, master, workers):
        data = list(range(10))
        rdd = sc.parallelize(data)

        fun = lambda x, y: x + y
        reduced = rdd.reduce(fun)
        assert reduced == reduce(fun, data), reduced
