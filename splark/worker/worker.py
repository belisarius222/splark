import sys
import time
from uuid import uuid4
from multiprocessing import Process

import zmq

from splark.worker.inner import InnerWorker


class Worker(Process):
    def __init__(self, endpoint, workport=23456, logport=23457, workerID=None):
        Process.__init__(self)
        self.endpoint = endpoint
        self.workport = workport
        self.logport = logport
        self.workerID = workerID

        if self.workerID is None:
            self.workerID = "worker-" + str(uuid4())

        self.ipcURI = "ipc:///tmp/splark_" + self.workerID + ".ipc"

        # Data id: cloudpickle
        self.data = {}

    def setupWorker(self):
        self.worker = InnerWorker(self.ipcURI, daemon=True)
        self.worker.start()

        self.working = False
        self.workingID = None

    def setupZMQ(self):
        self.ctx = zmq.Context()

        # Setup syncronous comms to master
        self.master = self.ctx.socket(zmq.REQ)
        self.master.setsockopt(zmq.IDENTITY, self.workerID.encode("ascii"))
        self.master.connect(self.endpoint + ":" + str(self.workport))

        # Send the initial worker ID to start a transaction going
        self.master.send(b"")

        # Setup async stdio/stderr
        self.stdsocket = self.ctx.socket(zmq.PUSH)

        # Setup IPC to inner worker
        self.inner = self.ctx.socket(zmq.REQ)
        self.inner.bind(self.ipcURI)

    def setup(self):
        self.setupZMQ()
        self.setupWorker()

    # All _handle_* functions map 1:1 with API calls
    # They all return a pyobject, which is the serialized
    # response to the call
    def _handle_setdata(self, id, blob):
        self.data[id] = blob
        print("setdata", id, ": ", blob)
        return True

    def _handle_getdata(self, id):
        return self.data[id]

    def _handle_listdata(self):
        return list(self.data.keys())

    def _handle_isworking(self):
        return self.working

    def _handle_resetworker(self):
        self.worker.terminate()
        time.sleep(1)
        self.setupWorker()
        return True

    def _handle_map(self, *ids):
        # Work not queued, already working
        if self.working:
            return False

        inputIDs = ids[:-1]
        outID = ids[-1]

        # All datae must already be here
        for id in inputIDs:
            if id not in self.data:
                return False
        print("Starting work!")
        self.startWork(inputIDs, outID)
        print("Started")
        return True

    def _handle_ping(self):
        return "pong"

    def _handle_die(self):
        self.worker.terminate()
        self.working = False
        sys.exit(0)

    def processRequests(self):
        # Check to see if we have something we need to do
        if not self.master.poll(100, zmq.POLLIN):
            return

        # Get the request and arguments from the exterior
        request = self.master.recv_multipart()
        requestType = request[0].decode("ascii")
        args = tuple(request[1:])

        # Get the handler for this request type
        fHandler = getattr(self, "_handle_" + requestType, None)
        assert fHandler is not None, "No handler for request:" + requestType

        # Call the handler on the remaining arguments
        result = fHandler(*args)
        if type(result) is bytes:
            self.master.send(result)
        else:
            self.master.send_pyobj(result)

    def startWork(self, inIDs, outID):
        assert not self.working

        # Dispatch a map to the inner worker
        self.inner.send_multipart(tuple(self.data[id] for id in inIDs))

        # Store state for when we return
        self.workingID = outID
        self.working = True

    def possiblyFinishWork(self):
        if not self.working:
            return
        if not self.inner.poll(100, zmq.POLLIN):
            return
        self.finishWork()

    def finishWork(self):
        # Return the work unit to the data pool with the above semaphores
        self.data[self.workingID] = self.inner.recv()

        # Clear the semaphors
        self.workingID = None
        self.working = False

    def run(self):
        self.setup()

        while True:
            # Look for incoming requests
            self.processRequests()

            # Finish work orders
            self.possiblyFinishWork()
