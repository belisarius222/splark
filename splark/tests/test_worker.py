from pickle import loads, dumps

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
        assert "worker-" in self._wid.decode("ascii")
        assert self._w.is_alive()

        return self._w, self._wid, self._skt

    def __exit__(self, arg1, arg2, arg3):
        assert self._w.is_alive(), "Process Died During Testing"
        self._skt.send_multipart((self._wid, b"", b"die"))
        self._w.join()


def test_worker_send_ping():
    with WorkerWithSocket() as (wrkr, wid, skt):
        skt.send_multipart((wid, b"", b"ping"))
        assert skt.poll(1000, zmq.POLLIN)
        id, _, pong = skt.recv_multipart()
        assert loads(pong) == "pong"


def test_worker_set_data():
    with WorkerWithSocket() as (wrkr, wid, skt):
        dataToSend = dumps(list(range(10)))

        skt.send_multipart((wid, b"", b"setdata", b"daterz-idz", dataToSend))
        assert skt.poll(1000, zmq.POLLIN)
        id, _, pong = skt.recv_multipart()
        assert loads(pong)


def test_worker_set_get_data():
    with WorkerWithSocket() as (wrkr, wid, skt):
        dataToSend = dumps(list(range(10)))

        skt.send_multipart((wid, b"", b"setdata", b"daterz-idz", dataToSend))
        assert skt.poll(1000, zmq.POLLIN)
        id, _, pong = skt.recv_multipart()
        assert loads(pong)

        skt.send_multipart((wid, b"", b"getdata", b"daterz-idz"))
        _, __, daterz2 = skt.recv_multipart()
        print(loads(daterz2))
        # NB: Comparing pickled strings of range(10)
        assert dataToSend == daterz2


def test_worker_set_list_data():
    with WorkerWithSocket() as (wrkr, wid, skt):
        dataToSend = dumps(list(range(10)))

        dataID = b"daterz-idz"
        skt.send_multipart((wid, b"", b"setdata", dataID, dataToSend))
        assert skt.poll(1000, zmq.POLLIN)
        id, _, pong = skt.recv_multipart()
        assert loads(pong)

        skt.send_multipart((wid, b"", b"listdata"))
        _, __, daterz2 = skt.recv_multipart()
        listing = loads(daterz2)
        assert len(listing) == 1
        assert dataID in listing
