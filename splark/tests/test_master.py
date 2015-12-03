import itertools, sys, traceback

from nose.tools import timed

from splark import Master, Worker
from splark.tests import getTestingWorkPort, getTestingLogPort


class MasterWithWorkers:
    def __init__(self, num_workers=4):
        self.num_workers = num_workers

        # Increment the ports each time, so we don't hit the "address already in use" error.
        self.workport = getTestingWorkPort()
        self.logport = getTestingLogPort()

        self.master = Master(workport=self.workport, logport=self.logport)

        self.workers = []
        for i in range(num_workers):
            worker = Worker("tcp://localhost", workport=self.workport, logport=self.logport, print_local=False)
            self.workers.append(worker)

    def __enter__(self):
        for worker in self.workers:
            worker.start()

        self.master.wait_for_worker_connections(self.num_workers)

        return self.master, self.workers

    def __exit__(self, *args):
        if any(arg is not None for arg in args):
            print("MasterWithWorkers.__exit__() caught an error.", file=sys.stderr)
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


@timed(10)
def test_master_wait_for_worker_connections():
    with MasterWithWorkers() as (master, workers):
        pass


@timed(10)
def test_master_getdata_setdata():
    with MasterWithWorkers() as (master, workers):
        data = tuple(range(4))
        data_id = b"data"

        print("Sending 'setdata'")
        master.set_data(data_id, data)

        print("Sending 'getdata'")
        recv_data = master.get_data(data_id)
        assert recv_data == data, repr(recv_data) + " != " + repr(data)


@timed(10)
def test_master_map():
    with MasterWithWorkers() as (master, workers):
        data = [[0, 1], [2, 3], [4], [5]]
        data_id = b"data"
        master.set_data(data_id, data)

        incrementFunc = lambda partition: [x + 1 for x in partition]
        func_id = b"fun"
        master.broadcast_datum(func_id, incrementFunc)

        output_id = b"output"
        master.map(func_id, (data_id,), output_id)
        master.wait_for_workers_to_finish()
        recv_data = master.get_data(output_id)
        assert recv_data == [incrementFunc(partition) for partition in data], recv_data
