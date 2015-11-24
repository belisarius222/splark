import sys
import time
from uuid import uuid4
from multiprocessing import Process, Pipe

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
        self.inner_recv_pipe, inner_stdout = Pipe(duplex=False)
        # Make the pipe look like the inner worker's stdout. Surprisingly, this works.
        inner_stdout.write = inner_stdout.send
        inner_stdout.flush = lambda: None
        self.inner_worker = InnerWorker(self.ipcURI, daemon=True, stdout=inner_stdout, stderr=inner_stdout)
        self.inner_worker.start()

        self.working = False
        self.workingID = None

    def setupZMQ(self):
        self.ctx = zmq.Context()

        # Setup syncronous comms to master
        self.master_socket = self.ctx.socket(zmq.REQ)
        self.master_socket.setsockopt(zmq.IDENTITY, self.workerID.encode("ascii"))
        self.master_socket.connect(self.endpoint + ":" + str(self.workport))

        # Send the initial worker ID to start a transaction going
        self.master_socket.send(b"")

        # Setup async stdio/stderr
        self.stdsocket = self.ctx.socket(zmq.PUSH)
        self.stdsocket.connect(self.endpoint + ":" + str(self.logport))
        self.unsent_stdout = ""

        # Setup IPC to inner worker
        self.inner_socket = self.ctx.socket(zmq.REQ)
        self.inner_socket.bind(self.ipcURI)

    def setupPoller(self):
        self.poller = zmq.Poller()
        self.poller.register(self.master_socket, zmq.POLLIN)
        self.poller.register(self.inner_socket, zmq.POLLIN)
        self.poller.register(self.inner_recv_pipe.fileno(), zmq.POLLIN)

    def setup(self):
        self.log("Initializing.")
        self.setupZMQ()
        self.setupWorker()
        self.setupPoller()
        self.log("Initialization complete.")

    def log(self, *args, **kwargs):
        short_name = self.workerID.split("worker-")[1][:8]
        print(*("WORKER " + short_name + " >>>",) + args, **kwargs)

    # All _handle_* functions map 1:1 with API calls
    # They all return a pyobject, which is the serialized
    # response to the call
    def _handle_setdata(self, id, blob):
        self.data[id] = blob
        return True

    def _handle_getdata(self, id):
        return self.data[id]

    def _handle_listdata(self):
        return list(self.data.keys())

    def _handle_isworking(self):
        return self.working

    def _handle_resetworker(self):
        self.inner_worker.terminate()
        time.sleep(1)
        self.setupWorker()
        return True

    def _handle_map(self, *ids):
        # Work not queued, already working
        if self.working:
            return False

        *inputIDs, outID = ids

        # All datae must already be here
        if any(idee not in self.data for idee in inputIDs):
            return False

        self.log("Starting work!")
        self.startWork(inputIDs, outID)
        return True

    def _handle_ping(self):
        return "pong"

    def _handle_die(self):
        self.log("Terminating inner worker process.")
        self.inner_worker.terminate()
        self.working = False
        self.log("Exiting.")
        sys.exit(0)

    def processRequest(self):
        # Get the request and arguments from the exterior
        request = self.master_socket.recv_multipart()
        requestType = request[0].decode("ascii")
        args = tuple(request[1:])

        # Get the handler for this request type
        fHandler = getattr(self, "_handle_" + requestType, None)
        assert fHandler is not None, "No handler for request:" + requestType

        # Call the handler on the remaining arguments
        self.log("RECV \"{}\"".format(requestType))
        result = fHandler(*args)
        if type(result) is bytes:
            self.master_socket.send(result)
        else:
            self.master_socket.send_pyobj(result)

    def startWork(self, inIDs, outID):
        assert not self.working

        # Dispatch a map to the inner worker
        self.inner_socket.send_multipart(tuple(self.data[id] for id in inIDs))

        # Store state for when we return
        self.workingID = outID
        self.working = True

    def finishWork(self):
        # Return the work unit to the data pool with the above semaphores
        self.data[self.workingID] = self.inner_socket.recv()

        # Clear the semaphors
        self.workingID = None
        self.working = False

    def maybeSendStdout(self):
        self.unsent_stdout += self.inner_recv_pipe.recv()
        if self.unsent_stdout.endswith("\n"):
            string_to_send = self.unsent_stdout[:-1]  # Don't send the newline
            print("INNER >>>", string_to_send)
            self.stdsocket.send_string(string_to_send)
            self.unsent_stdout = ""

    def run(self):
        self.setup()

        while True:
            socks = dict(self.poller.poll())
            # Look for incoming requests
            if socks.get(self.master_socket) == zmq.POLLIN:
                self.processRequest()

            # Finish work orders
            if socks.get(self.inner_socket) == zmq.POLLIN:
                self.finishWork()

            if socks.get(self.inner_recv_pipe.fileno()) == zmq.POLLIN:
                self.maybeSendStdout()
