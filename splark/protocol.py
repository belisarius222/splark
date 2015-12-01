import itertools
from enum import Enum

import zmq

from splark.misc import fromCP, toCP

_ordered_byte = map(lambda i: i.to_bytes(1, "little"), itertools.count(0))


class Commands(bytes, Enum):
    CONNECT = next(_ordered_byte)
    PING = next(_ordered_byte)
    DIE = next(_ordered_byte)
    GETDATA = next(_ordered_byte)
    SETDATA = next(_ordered_byte)
    DELDATA = next(_ordered_byte)
    LISTDATA = next(_ordered_byte)
    CALL = next(_ordered_byte)
    ISWORKING = next(_ordered_byte)
    RESETWORKER = next(_ordered_byte)

    @classmethod
    def items(cls):
        return {Commands[n].value: n for n in cls.__members__}


class AbstractSocketWrapper(zmq.Socket):
    socket_type = None

    def __new__(cls, ctx, *args, **kwargs):
        assert cls.socket_type is not None
        instance = ctx.socket(cls.socket_type)
        instance.__class__ = cls
        return instance


class WorkerConnection(AbstractSocketWrapper):
    """
    An object representing a connection to a splark worker.
    Behaves mostly like its underlying zmq socket.
    """
    socket_type = zmq.ROUTER

    def send_cmd(self, worker_id, cmd, *args, **zmq_options):
        assert type(worker_id) is bytes, worker_id
        assert cmd in Commands.items(), cmd
        for arg in args:
            assert type(arg) is bytes, arg

        return self.send_multipart((worker_id, b"", cmd) + args, **zmq_options)

    def recv_response(self, deserialize=fromCP):
        worker_id, delimiter, response_serial = self.recv_multipart()

        assert type(worker_id) is bytes, worker_id
        assert delimiter == b"", delimiter
        response = deserialize(response_serial)

        return worker_id, response


class MasterConnection(AbstractSocketWrapper):
    """
    An object representing a connection to a splark master.
    Behaves mostly like its underlying zmq socket.
    """
    socket_type = zmq.REQ

    def __init__(self, ctx, idee):
        assert type(idee) is bytes, idee
        self.setsockopt(zmq.IDENTITY, idee)

    def recv_cmd(self):
        cmd, *args = self.recv_multipart()
        assert cmd in Commands.items(), cmd
        cmd_name = Commands.items()[cmd]
        return cmd_name, args

    def send_response(self, response, serialize=toCP):
        if type(response) is bytes:
            self.send(response)
        else:
            self.send(serialize(response))

    def send_connect(self):
        self.send_response(Commands.CONNECT)
