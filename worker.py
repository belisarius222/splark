import sys
import time
from uuid import uuid4
from multiprocessing import Process

from cloudpickle import toCP, fromCP
import zmq


class InnerWorker(Process):
    def __init__(self, uri, stdout=sys.stdout, stderr=sys.stderr):
        Process.__init__(self)

        self._uri = uri
        self._ctx = zmq.Context()
        self._skt = zmq.Socket(zmq.REP)

    def run(self):
        while True:
            parts = self._skt.recv_multi()

            # Uncloudpickel everything
            # f, ArgList1, ArgList2, ArgList3 . . .
            f, *args = [fromCP(part) for part in parts]

            # Do the actual work!
            # MRG TODO: This could be much more sophisticated.
            results = [f(fargs) for fargs in zip(*args)]

            self._skt.send(toCP(results))


class Worker(Process):
    def __init__(self, endpoint, workport=23456, logport=23457, workerID=None):
        Process.__init__(self)
        self.endpoint = endpoint
        self.workport = workport
        self.logport = logport
        self.workerID = workerID

        # Data id: cloudpickle
        self.data = {}

    def setupWorker(self):
        self.workerID = self.workerID if self.workerID else str(uuid4())
        self.ipcURI = "ipc:///tmp/splark_" + self.workerID + ".ipc"

        self.worker = InnerWorker(self.ipcURI)
        self.worker.start()

        self.working = False
        self.workingID = None

    def setupZMQ(self):
        self.ctx = zmq.Context()

        # Setup syncronous comms to master
        self.master = self.ctx.Socket(zmq.REQ)
        self.master.connect(self.endpoint + ":" + str(self.workport))

        # Send the initial worker ID to start a transaction going
        self.master.send(self.workerID)

        # Setup async stdio/stderr
        self.stdsocket = self.ctx.Socket(zmq.PUSH)

        # Setup IPC to inner worker
        self.inner = self.ctx.Socket(zmq.REQ)
        self.inner.bind(self.ipcURI)

    def setup(self):
        self.setupZMQ()
        self.setupWorker()

    def _handle_setdata(self, id, blob):
        self.data[id] = blob
        return ""

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
        return ""

    def _handle_pending_work(self):
        # Wait for the
        self.inner.recv()

    def _handle_map(self, *ids):
        if self.working:
            # Work not queued, already working
            return False

        inputIDs = ids[:-1]
        outID = ids[-1]

        # All datae must already be here
        for id in inputIDs:
            if id not in self.data:
                return False

        self.startWork(inputIDs, outID)
        return ""

    def _handle_ping(self):
        return "pong"

    def _handle_die(self):
        self.worker.terminate()
        self.working = False
        sys.exit(0)

    def processRequests(self):
        # Check to see if we have something we need to do
        if not self.workport.poll(100, zmq.POLLIN):
            return

        # Get the request and arguments from the exterior
        request = self.workport.recv_multi()
        requestType = request[0].decode("ascii")
        args = tuple(request[1:])

        # Get the handler for this request type
        fHandler = getattr(self, "_handle_" + requestType, None)
        assert fHandler is not None, "No handler for request:" + requestType

        # Call the handler on the remaining arguments
        fHandler(*args)

    def startWork(self, inIDs, outID):
        assert not self.working

        self.inner.send_multi(tuple(self.data[id] for id in inIDs))
        # Dispatch a map to the inner worker
        self.workingID = outID
        self.working = True

    def finishWork(self):
        if not self.working:
            return
        if not self.inner.poll(100, zmq.POLLIN):
            return

        self.data[self.workingID] = self.inner.recv()

        self.workingID = None
        self.working = False

    def run(self):
        while True:
            # Look for incoming requests
            self.processRequests()

            # Finish work orders
            self.finishWork()
