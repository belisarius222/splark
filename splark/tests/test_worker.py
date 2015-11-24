from pickle import loads, dumps
import time

import zmq

from splark.dataWorker import Worker


class WorkerWithSocket:
    def __init__(self):
        self._w = Worker("tcp://localhost")

    def __enter__(self):
        self._w.start()
        self._skt = zmq.Context().socket(zmq.ROUTER)
        self._skt.bind("tcp://*:23456")
        assert self._skt.poll(1000, zmq.POLLIN), "No response from started worker"

        self._wid = self._skt.recv_multipart()[0]
        print("Worker reports in with id:", self._wid)
        assert "worker-" in self._wid.decode("ascii")
        assert self._w.is_alive()

        print("Enter Success")
        return self._w, self._wid, self._skt

    def __exit__(self, arg1, arg2, arg3):
        print("Exiting")
        assert self._w.is_alive(), "Process Died During Testing"
        self._skt.send_multipart((self._wid, b"", b"die"))
        self._w.join()


# def test_basic():
#     w = Worker("tcp://localhost")
#     w.start()

#     time.sleep(2)

#     assert w.is_alive()
#     w.terminate()
#     w.join()


def test_worker_send_ping():
    with WorkerWithSocket() as (wrkr, wid, skt):
        skt.send_multipart((wid, b"", b"ping"))
        assert skt.poll(1000, zmq.POLLIN)
        id, _, pong = skt.recv_multipart()
        assert loads(pong) == "pong"


def test_worker_send_data():
    with WorkerWithSocket() as (wrkr, wid, skt):
        dataToSend = dumps(list(range(10)))

        skt.send_multipart((wid, b"", b"setdata", b"daterz-idz", dataToSend))
        assert skt.poll(1000, zmq.POLLIN)
        id, _, pong = skt.recv_multipart()
        assert loads(pong)
