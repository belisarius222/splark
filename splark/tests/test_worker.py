import zmq
from nose.tools import timed

from splark.misc import fromCP, toCP
from splark.worker.worker import Worker


class WorkerWithSocket:
    def __init__(self):
        self.worker = Worker("tcp://localhost")

    def __enter__(self):
        self.worker.start()
        self.socket = zmq.Context().socket(zmq.ROUTER)
        self.socket.bind("tcp://*:23456")
        assert self.socket.poll(1000, zmq.POLLIN), "No response from started worker"

        self.worker_id = self.socket.recv_multipart()[0]
        assert "worker-" in self.worker_id.decode("ascii")
        assert self.worker.is_alive()

        return self

    def __exit__(self, arg1, arg2, arg3):
        assert self.worker.is_alive(), "Process Died During Testing"
        self.socket.send_multipart((self.worker_id, b"", b"die"))
        self.worker.join()

    def transact_to_worker(self, *args, timeout=1000):
        self.socket.send_multipart((self.worker_id, b"",) + args)
        assert self.socket.poll(timeout, zmq.POLLIN), "Timeout waiting for response to: {}".format(args)
        worker_id, _, response = self.socket.recv_multipart()
        assert worker_id == self.worker_id
        return response


def test_worker_send_ping():
    with WorkerWithSocket() as test_harness:
        pong = test_harness.transact_to_worker(b"ping")
        assert fromCP(pong) == "pong", fromCP(pong)


def test_worker_set_data():
    with WorkerWithSocket() as test_harness:
        dataToSend = toCP(list(range(10)))
        pong = test_harness.transact_to_worker(b"setdata", b"daterz-idz", dataToSend)
        assert fromCP(pong), pong


def test_worker_set_get_data():
    with WorkerWithSocket() as test_harness:
        dataToSend = toCP(list(range(10)))

        pong = test_harness.transact_to_worker(b"setdata", b"daterz-idz", dataToSend)
        assert fromCP(pong), pong

        pickled_response = test_harness.transact_to_worker(b"getdata", b"daterz-idz")
        assert dataToSend == pickled_response


def test_worker_set_list_data():
    with WorkerWithSocket() as test_harness:
        dataToSend = toCP(list(range(10)))

        data_id = b"daterz-idz"
        pong = test_harness.transact_to_worker(b"setdata", data_id, dataToSend)
        assert fromCP(pong)

        pickled_listing = test_harness.transact_to_worker(b"listdata")
        listing = fromCP(pickled_listing)
        assert len(listing) == 1
        assert data_id in listing


@timed(1)
def test_worker_set_call_get_data():
    with WorkerWithSocket() as test_harness:
        data = list(range(10))
        data_pickle = toCP(data)
        data_id = b"data1"
        pong = test_harness.transact_to_worker(b"setdata", data_id, data_pickle)
        assert fromCP(pong)

        fun_id = b"fun1"
        fun = lambda x: x + 1
        fun_pickle = toCP(fun)
        pong = test_harness.transact_to_worker(b"setdata", fun_id, fun_pickle)
        assert fromCP(pong)

        map_output_id = b"data2"
        started_work_pickle = test_harness.transact_to_worker(b"map", fun_id, data_id, map_output_id)
        started_work = fromCP(started_work_pickle)
        assert type(started_work) == bool, "Malformed response from \"map\" command: {}".format(started_work)
        assert started_work is True, "Worker was unable to start \"map\" command."

        # Poll the worker repeatedly until it's done.
        isworking = True
        while isworking:
            isworking_pickle = test_harness.transact_to_worker(b"isworking")
            isworking = fromCP(isworking_pickle)
            assert type(isworking) == bool, "Malformed response from \"isworking\" command: {}".format(isworking)

        listing_pickle = test_harness.transact_to_worker(b"listdata")
        listing = fromCP(listing_pickle)

        assert set(listing) == {data_id, fun_id, map_output_id}, listing

        output_data_pickle = test_harness.transact_to_worker(b"getdata", map_output_id)
        assert fromCP(output_data_pickle) == list(map(fun, data)), fromCP(output_data_pickle)
