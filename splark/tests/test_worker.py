import time, zmq
from nose.tools import timed

from splark.misc import toCP
from splark.protocol import Commands, WorkerConnection
from splark.worker.worker import Worker


class WorkerWithSocket:
    def __init__(self, attributes=("send_and_recv",)):
        self._enter_attributes = attributes
        self.worker = Worker("tcp://localhost", print_local=True)

    def __enter__(self):
        self.worker.start()
        ctx = zmq.Context()
        self.socket = WorkerConnection(ctx)
        self.socket.bind("tcp://*:23456")
        assert self.socket.poll(1000, zmq.POLLIN), "No response from started worker"

        self.worker_id, connect_message = self.socket.recv_response()
        assert connect_message == Commands.CONNECT, connect_message
        assert "worker-" in self.worker_id.decode("ascii")
        assert self.worker.is_alive()

        self.stdsocket = ctx.socket(zmq.PULL)
        self.stdsocket.bind("tcp://*:23457")

        attrs = [getattr(self, attr) for attr in self._enter_attributes]
        if len(attrs) == 1:
            return attrs[0]
        return attrs

    def __exit__(self, arg1, arg2, arg3):
        assert self.worker.is_alive(), "Process Died During Testing"
        self.socket.send_cmd(self.worker_id, Commands.DIE)
        self.worker.join()

    def send_and_recv(self, cmd, *args, timeout=1000, expect=None, **kwargs):
        self.socket.send_cmd(self.worker_id, cmd, *args)
        assert self.socket.poll(timeout, zmq.POLLIN), "Timeout waiting for response to: {}".format(args)

        worker_id, response = self.socket.recv_response(**kwargs)
        assert worker_id == self.worker_id, worker_id

        err_msg = "Invalid response from '{}' command: {}".format(Commands.items()[cmd], response)
        if expect is not None and callable(expect):
            assert expect(response), err_msg
        elif expect is not None:
            assert response == expect, err_msg
        return response


def test_worker_send_ping():
    with WorkerWithSocket() as send_and_recv:
        send_and_recv(Commands.PING, expect=b"pong", deserialize=lambda b: b)


def test_worker_set_data():
    with WorkerWithSocket() as send_and_recv:
        dataToSend = toCP(list(range(10)))
        send_and_recv(Commands.SETDATA, b"daterz-idz", dataToSend, expect=True)


def test_worker_set_get_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)

        send_and_recv(Commands.SETDATA, b"daterz-idz", data_pickle, expect=True)
        send_and_recv(Commands.GETDATA, b"daterz-idz", expect=data)


def test_worker_set_list_data():
    with WorkerWithSocket() as send_and_recv:
        dataToSend = toCP(list(range(10)))
        data_id = b"daterz-idz"

        send_and_recv(Commands.SETDATA, data_id, dataToSend, expect=True)
        send_and_recv(Commands.LISTDATA, expect=[data_id])


@timed(1)
def test_worker_set_call_get_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)
        data_id = b"data1"
        send_and_recv(Commands.SETDATA, data_id, data_pickle, expect=True)

        fun_id = b"fun1"
        fun = lambda partition: [x + 1 for x in partition]
        fun_pickle = toCP(fun)
        send_and_recv(Commands.SETDATA, fun_id, fun_pickle, expect=True)

        map_output_id = b"data2"
        send_and_recv(Commands.CALL, fun_id, data_id, map_output_id, expect=True)

        # Poll the worker repeatedly until it's done.
        isworking = True
        while isworking:
            isworking = send_and_recv(Commands.ISWORKING)
            time.sleep(0.1)
            assert type(isworking) == bool, "Malformed response from \"isworking\" command: {}".format(isworking)

        send_and_recv(Commands.LISTDATA, expect=lambda d: set(d) == {data_id, fun_id, map_output_id})
        send_and_recv(Commands.GETDATA, map_output_id, expect=fun(data))


@timed(1)
def test_worker_del_data():
    with WorkerWithSocket() as send_and_recv:
        data = list(range(10))
        data_pickle = toCP(data)
        data_id = b"data1"
        send_and_recv(Commands.SETDATA, data_id, data_pickle, expect=True)

        send_and_recv(Commands.DELDATA, data_id, expect=True)
        send_and_recv(Commands.DELDATA, data_id, expect=False)
        send_and_recv(Commands.GETDATA, data_id, expect=lambda e: isinstance(e, Exception))


@timed(1)
def test_worker_stdout_stream():
    with WorkerWithSocket(("send_and_recv", "stdsocket")) as (send_and_recv, stdsocket):
        data = list(range(10))
        data_pickle = toCP(data)
        data_id = b"data1"
        send_and_recv(Commands.SETDATA, data_id, data_pickle, expect=True)

        def fun(partition):
            ret = [x + 1 for x in partition]
            print(ret)
            return ret
        fun_pickle = toCP(fun)
        fun_id = b"fun"
        send_and_recv(Commands.SETDATA, fun_id, fun_pickle, expect=True)

        map_output_id = b"data2"
        send_and_recv(Commands.CALL, fun_id, data_id, map_output_id, expect=True)

        # Poll the worker repeatedly until it's done.
        isworking = True
        while isworking:
            isworking = send_and_recv(Commands.ISWORKING)
            time.sleep(0.1)
            assert type(isworking) == bool, "Malformed response from \"isworking\" command: {}".format(isworking)

        received_stdout = stdsocket.recv()
        # Ignore the outer worker's logging.
        while received_stdout.startswith(b"WORKER"):
            received_stdout = stdsocket.recv()
        assert received_stdout == str(fun(data)).encode("ascii"), received_stdout

        listing = send_and_recv(Commands.LISTDATA)
        assert set(listing) == {data_id, fun_id, map_output_id}, listing

        send_and_recv(Commands.GETDATA, map_output_id, expect=fun(data))
        print("")  # Make sure this test doesn't end on half a line
