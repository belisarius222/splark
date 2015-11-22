import pickle
from uuid import uuid4

import zmq


class Worker:
    def __init__(self, masterEndpoint):
        self.id = str(uuid4()).encode('ascii')
        self.masterEndpoint = masterEndpoint

    def connect(self):
        ctx = zmq.Context()
        self.recvSock = ctx.socket(zmq.PULL)
        self.recvSock.connect(self.masterEndpoint)

    def run(self):
        while True:
            serializedFunc = self.recvSock.recv()
            func = pickle.loads(serializedFunc)
            func()
