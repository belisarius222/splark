import pickle

import zmq


class Worker:
    def __init__(self, masterEndpoint):
        ctx = zmq.Context()
        self.recvSock = ctx.socket(zmq.PULL)
        self.recvSock.connect(masterEndpoint)

    def run(self):
        while True:
            serializedFunc = self.recvSock.recv()
            func = pickle.loads(serializedFunc)
            func()
