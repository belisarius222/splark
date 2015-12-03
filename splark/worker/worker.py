import sys
import time
from uuid import uuid4
from multiprocessing import Process, Pipe

import zmq

from splark.protocol import MasterConnection
from splark.worker.inner import InnerWorker


class Worker(Process):
    def __init__(self, endpoint, workport=23456, logport=23457, workerID=None, print_local=True):
        Process.__init__(self)
        self.endpoint = endpoint
        self.workport = workport
        self.logport = logport
        self.workerID = workerID
        self.print_local = print_local

        self._is_stdsocket_setup = False

        if self.workerID is None:
            self.workerID = "worker-" + str(uuid4())

        self.ipcURI = "ipc:///tmp/splark_" + self.workerID + ".ipc"

        # Data id: cloudpickle
        self.data = {}
        self.unsent_stdout = ""

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
        self.master_socket = MasterConnection(self.ctx, self.workerID.encode("ascii"))
        self.master_socket.connect(self.endpoint + ":" + str(self.workport))

        # Send the initial worker ID to start a transaction going
        self.master_socket.send_connect()

        # Setup async stdio/stderr
        self.stdsocket = self.ctx.socket(zmq.PUSH)
        self.stdsocket.connect(self.endpoint + ":" + str(self.logport))
        self._is_stdsocket_setup = True

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

    def log(self, *args):
        short_name = self.workerID.split("worker-")[1][:8]
        print_args = ("WORKER " + short_name + " >>>",) + args
        if self.print_local:
            print(*print_args)
        # Buffer this message to be sent over stdout.
        self.unsent_stdout += "\n" + " ".join(print_args) + "\n"
        self.flush_stdout_buffer()

    # All _handle_* functions map 1:1 with API calls
    # They all return a pyobject, which is the serialized
    # response to the call
    def _handle_SETDATA(self, id, blob):
        self.data[id] = blob
        return True

    def _handle_DELDATA(self, id):
        try:
            del self.data[id]
            return True
        except KeyError:
            return False

    def _handle_GETDATA(self, id):
        try:
            return self.data[id]
        except KeyError as e:
            return e  # TAB TODO: better error reporting

    def _handle_LISTDATA(self):
        return list(self.data.keys())

    def _handle_ISWORKING(self):
        return self.working

    def _handle_RESETWORKER(self):
        self.inner_worker.terminate()
        time.sleep(1)
        self.setupWorker()
        return True

    def _handle_CALL(self, *ids):
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

    def _handle_PING(self):
        return b"pong"

    def _handle_DIE(self):
        self.log("Terminating inner worker process.")
        self.inner_worker.terminate()
        self.working = False
        self.log("Exiting.")
        sys.exit(0)

    def processRequest(self):
        # Get the request and arguments from the exterior
        requestType, args = self.master_socket.recv_cmd()

        # Get the handler for this request type
        fHandler = getattr(self, "_handle_" + requestType, None)
        assert fHandler is not None, "No handler for request:" + requestType

        # Call the handler on the remaining arguments
        self.log("RECV \"{}\"".format(requestType))
        result = fHandler(*args)
        self.master_socket.send_response(result)

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

    def recv_stdout(self):
        self.unsent_stdout += self.inner_recv_pipe.recv()
        self.flush_stdout_buffer()

    def flush_stdout_buffer(self):
        if not self._is_stdsocket_setup:
            return

        *messages, self.unsent_stdout = self.unsent_stdout.split("\n")
        for message in messages:
            # Ignore blank lines. Ain't nobody got time for that.
            if message == "":
                continue
            if self.print_local:
                print(message)
            self.stdsocket.send_string(message)

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

            # Pipe inner worker's stdout and stderr to master
            if socks.get(self.inner_recv_pipe.fileno()) == zmq.POLLIN:
                self.recv_stdout()
