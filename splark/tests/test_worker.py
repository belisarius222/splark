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

        return self.send_and_recv

    def __exit__(self, arg1, arg2, arg3):
        assert self.worker.is_alive(), "Process Died During Testing"
        self.socket.send_multipart((self.worker_id, b"", b"die"))
        self.worker.join()

    def send_and_recv(self, *args, timeout=1000, expect=None):
        self.socket.send_multipart((self.worker_id, b"",) + args)
        assert self.socket.poll(timeout, zmq.POLLIN), "Timeout waiting for response to: {}".format(args)

        worker_id, _, response_pickle = self.socket.recv_multipart()
        assert worker_id == self.worker_id

        response = fromCP(response_pickle)
        if expect is not None:
            assert response == expect, "Invalid response from '{}' command. Expected: {} Actual: {}".format(args[0], expect, response)
        return response


def test_worker_send_ping():
    with WorkerWithSocket() as send_and_recv:
        send_and_recv(b"ping", expect="pong")


def test_worker_set_data():
    with WorkerWithSocket() as send_and_recv:
        dataToSend = toCP(list(range(10)))
        send_and_recv(b"setdata", b"daterz-idz", dataToSend, expect=True)


def test_worker_set_get_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)

        send_and_recv(b"setdata", b"daterz-idz", data_pickle, expect=True)
        send_and_recv(b"getdata", b"daterz-idz", expect=data)


def test_worker_set_list_data():
    with WorkerWithSocket() as send_and_recv:
        dataToSend = toCP(list(range(10)))
        data_id = b"daterz-idz"

        send_and_recv(b"setdata", data_id, dataToSend, expect=True)
        send_and_recv(b"listdata", expect=[data_id])


@timed(1)
def test_worker_set_call_get_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)
        data_id = b"data1"
        send_and_recv(b"setdata", data_id, data_pickle, expect=True)

        fun_id = b"fun1"
        fun = lambda x: x + 1
        fun_pickle = toCP(fun)
        send_and_recv(b"setdata", fun_id, fun_pickle, expect=True)

        map_output_id = b"data2"
        send_and_recv(b"map", fun_id, data_id, map_output_id, expect=True)

        # Poll the worker repeatedly until it's done.
        isworking = True
        while isworking:
            isworking = send_and_recv(b"isworking")
            assert type(isworking) == bool, "Malformed response from \"isworking\" command: {}".format(isworking)

        listing = send_and_recv(b"listdata")
        assert set(listing) == {data_id, fun_id, map_output_id}, listing

        send_and_recv(b"getdata", map_output_id, expect=list(map(fun, data)))
