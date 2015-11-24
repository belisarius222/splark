import time
from pickle import loads, dumps

import zmq

from splark.cloudpickle import dumps as toCP
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
        assert loads(pong) == "pong", loads(pong)


def test_worker_set_data():
    with WorkerWithSocket() as test_harness:
        dataToSend = dumps(list(range(10)))
        pong = test_harness.transact_to_worker(b"setdata", b"daterz-idz", dataToSend)
        assert loads(pong), pong


def test_worker_set_get_data():
    with WorkerWithSocket() as test_harness:
        dataToSend = dumps(list(range(10)))

        pong = test_harness.transact_to_worker(b"setdata", b"daterz-idz", dataToSend)
        assert loads(pong), pong

        pickled_response = test_harness.transact_to_worker(b"getdata", b"daterz-idz")
        assert dataToSend == pickled_response


def test_worker_set_list_data():
    with WorkerWithSocket() as test_harness:
        dataToSend = dumps(list(range(10)))

        data_id = b"daterz-idz"
        pong = test_harness.transact_to_worker(b"setdata", data_id, dataToSend)
        assert loads(pong)

        pickled_listing = test_harness.transact_to_worker(b"listdata")
        listing = loads(pickled_listing)
        assert len(listing) == 1
        assert data_id in listing


def test_worker_set_call_get_data():
    with WorkerWithSocket() as test_harness:
        print("Sending Data")
        data = list(range(10))
        data_pickle = dumps(data)
        data_id = b"data1"
        pong = test_harness.transact_to_worker(b"setdata", data_id, data_pickle)
        assert loads(pong)

        print("Sending Function")
        fun_id = b"fun1"
        fun = lambda x: x + 1
        fun_pickle = toCP(fun)
        pong = test_harness.transact_to_worker(b"setdata", fun_id, fun_pickle)
        assert loads(pong)

        print("Sending Map Request")
        map_output_id = b"data2"
        pong = test_harness.transact_to_worker(b"map", fun_id, data_id, map_output_id)
        assert loads(pong), pong

        print("\tReturned")
        time.sleep(1)

        print("Sending list")
        listing_pickle = test_harness.transact_to_worker(b"listdata")
        listing = loads(listing_pickle)

        assert set(listing) == {data_id, fun_id, map_output_id}, listing

        output_data_pickle = test_harness.transact_to_worker(b"getdata", map_output_id)
        assert loads(output_data_pickle) == list(map(fun, data)), loads(output_data_pickle)
