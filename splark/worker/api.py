import time
from asyncio import Future

import zmq

from splark.protocol import Commands
from splark.misc import toCP, EmptyPromise


class WorkerAPI:
    def __init__(self, wid, parentAPI):
        self._parent = parentAPI
        self.wid = wid

    def __getattr__(self, callname):
        def wrapper(*args):
            return self._parent.proxyCall(callname, self.wid, args)
        return wrapper


class MultiWorkerAPI:
    def __init__(self, bindURI):
        self.ctx = zmq.Context()

        self.worksock = self.ctx.socket(zmq.ROUTER)
        self.worksock.bind(bindURI)

        # WorkerID to cloudPickle
        self.widToResponse = {}

    def waitForWorkers(self, count, timeout=10):
        startTime = time.time()
        readyWorkerCount = 0
        while readyWorkerCount < count:
            self._pollForResponses(100)

            readyWorkerCount = sum([1 for f in self.widToResponse.values() if f.done()])
            print("Redy werkeks", readyWorkerCount)
            if (time.time() - startTime) > timeout:
                raise RuntimeError("Timed out waiting for %i workers" % count)

        wids = list(self.widToResponse.keys())[:count]
        return [WorkerAPI(wid, self) for wid in wids]

    def close(self, killWorkers=True):
        if killWorkers:
            for wid in self.getWorkerList():
                self.kill_worker_async(wid)
        self.worksock.close()

    def _pollForResponses(self, timeout=0):
        if self.worksock.poll(timeout):
            self._handle_worker_response()

    def _handle_worker_response(self):
        workerID, _, respCP = self.worksock.recv_multipart()

        # New worker connection
        if workerID not in self.widToResponse:
            assert respCP == Commands.CONNECT
            print("New Worker:", workerID)
            self.widToResponse[workerID] = Future()

        self.widToResponse[workerID].set_result(respCP)

    def _get_worker_response(self, workerID, timeout=None):
        assert workerID in self.widToResponse
        # NB: Will block/hang on dead worker w/o specifying a timeout
        startTime = time.time()
        while not self.widToResponse[workerID].done():
            # Check for timeout
            if (timeout is not None) and (time.time() - startTime) > timeout:
                raise RuntimeError("Timout waiting for worker response")

            # Handle responses waiting for come back
            self._pollForResponses(100)

        # Return the response, and reset the semaphore
        resp = self.widToResponse[workerID].result()
        self.widToResponse[workerID] = Future()
        # NB: Below can raise exceptions!
        return resp

    def _isReady(self, workerID):
        self._pollForResponses()
        return self.widToResponse[workerID].done()

    def _getWorkerPromise(self, workerID):
        return self.widToResponse[workerID]

    def _sendToWorker(self, workerID, *args):
        # All other functions route through this,
        # so harsh do sanity checks
        assert type(workerID) is bytes
        assert workerID in self.widToResponse
        assert args[0] in Commands

        self.worksock.send_multipart((workerID, b"") + args)

    def proxyCall(self, callname, workers, args, async=False, multi=False):
        assert hasattr(self, callname + "_async"), "Missing call named:" + callname
        if not multi:
            workers = [workers]

        call = getattr(self, callname + "_async")
        p = EmptyPromise()
        for wid in workers:
            p += call(wid, *args)

        if not async:
            return p.result()
        return p

    def kill_worker_async(self, workerID):
        self._sendToWorker(workerID, Commands.DIE)
        return self._getWorkerPromise(workerID)

    def getWorkerList(self):
        return list(self.widToResponse.keys())

    def isworking_async(self, workerID):
        self._sendToWorker(workerID, Commands.ISWORKING)
        return self._getWorkerPromise(workerID)

    def block_on_workers_done(self, workerList):
        # MRG NOTE: Optimize me
        workersStillWorking = list(workerList)
        while len(workersStillWorking) != 0:
            workersStillWorking = [wid for wid in workersStillWorking if self.isworking_sync(wid)]
            print("Waiting workers" + str(len(workersStillWorking)))

    def get_data_async(self, workerID, dataID):
        self._sendToWorker(workerID, Commands.GETDATA, dataID)
        return self._getWorkerPromise(workerID)

    def set_data_async(self, workerID, dataID, data):
        self._sendToWorker(workerID, Commands.SETDATA, dataID, data)
        return self._getWorkerPromise(workerID)

    def list_data_async(self, workerID):
        self._sendToWorker(workerID, Commands.LISTDATA)
        return self._getWorkerPromise(workerID)

    def ping_async(self, workerID):
        self._sendToWorker(workerID, Commands.PING)
        return self._getWorkerPromise(workerID)

    def send_function_async(self, workerID, functionID, functionOrCP):
        if callable(functionOrCP):
            functionOrCP = toCP(functionOrCP)
        self._send_setdata(workerID, functionOrCP)
        return self._getWorkerPromise(workerID)

    def call_async(self, workerID, functionID, argIDs, outID):
        allIDs = (Commands.MAP, functionID,) + argIDs + (outID,)
        self._sendToWorker(workerID, *allIDs)
        return self._getWorkerPromise(workerID)
