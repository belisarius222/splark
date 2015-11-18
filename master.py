from functools import partial

import zmq

from splark import cloudpickle


class Master:
    def __init__(self, port):
        ctx = zmq.Context()
        self.sendSock = ctx.socket(zmq.PUSH)
        self.sendSock.bind("tcp://*:" + str(port))

    def execute(self, func, *args, **kwargs):
        funcToSend = partial(func, *args, **kwargs)
        serializedFunc = cloudpickle.dumps(funcToSend)
        self.sendToWorkers(serializedFunc)

    def sendToWorkers(self, bizzytes):
        self.sendSock.send(bizzytes)
