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

    def send_and_recv(self, *args, timeout=1000):
        self.socket.send_multipart((self.worker_id, b"",) + args)
        assert self.socket.poll(timeout, zmq.POLLIN), "Timeout waiting for response to: {}".format(args)
        worker_id, _, response = self.socket.recv_multipart()
        assert worker_id == self.worker_id
        return fromCP(response)


def test_worker_send_ping():
    with WorkerWithSocket() as send_and_recv:
        pong = send_and_recv(b"ping")
        assert pong == "pong", pong


def test_worker_set_data():
    with WorkerWithSocket() as send_and_recv:
        dataToSend = toCP(list(range(10)))
        pong = send_and_recv(b"setdata", b"daterz-idz", dataToSend)
        assert pong, "Invalid response from "


def test_worker_set_get_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)

        response = send_and_recv(b"setdata", b"daterz-idz", data_pickle)
        assert response is True, response

        response = send_and_recv(b"getdata", b"daterz-idz")
        assert response == data, response


def test_worker_set_list_data():
    with WorkerWithSocket() as send_and_recv:
        dataToSend = toCP(list(range(10)))

        data_id = b"daterz-idz"
        pong = send_and_recv(b"setdata", data_id, dataToSend)
        assert pong is True, pong

        listing = send_and_recv(b"listdata")
        assert len(listing) == 1
        assert data_id in listing


@timed(1)
def test_worker_set_call_get_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)
        data_id = b"data1"
        response = send_and_recv(b"setdata", data_id, data_pickle)
        assert response is True, response

        fun_id = b"fun1"
        fun = lambda x: x + 1
        fun_pickle = toCP(fun)
        response = send_and_recv(b"setdata", fun_id, fun_pickle)
        assert response is True, response

        map_output_id = b"data2"
        started_work = send_and_recv(b"map", fun_id, data_id, map_output_id)
        assert type(started_work) == bool, "Malformed response from \"map\" command: {}".format(started_work)
        assert started_work is True, "Worker was unable to start \"map\" command."

        # Poll the worker repeatedly until it's done.
        isworking = True
        while isworking:
            isworking = send_and_recv(b"isworking")
            assert type(isworking) == bool, "Malformed response from \"isworking\" command: {}".format(isworking)

        listing = send_and_recv(b"listdata")
        assert set(listing) == {data_id, fun_id, map_output_id}, listing

        output_data = send_and_recv(b"getdata", map_output_id)
        assert output_data == list(map(fun, data)), output_data
