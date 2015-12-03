import time
from pickle import loads
from threading import Thread

import zmq

from splark.misc import toCP, TinyPromise, EmptyPromise


class MultiWorkerAPI:
    def __init__(self, bindURI):
        self.ctx = zmq.Context()

        self.worksock = self.ctx.socket(zmq.ROUTER)

        print("********")
        print(bindURI)
        print("*********")
        self.worksock.bind(bindURI)

        # WorkerID to cloudPickle
        self.responses = {}

    def waitForWorkers(self, count, timeout=10):
        startTime = time.time()
        while len(self.responses) < count:
            self._pollForResponses(100)

            if (time.time() - startTime) > timeout:
                raise RuntimeError("Timed out waiting for %i workers" % count)

        return list(self.responses.keys())[:count]

    def close(self):
        self.worksock.close()

    def _pollForResponses(self, timeout=0):
        if self.worksock.poll(timeout):
            self._handle_worker_response()

    def _handle_worker_response(self):
        mp = self.worksock.recv_multipart()
        if len(mp) != 3:
            print("Potentially invalid seq packet!  Ignoring it.")
            print(mp)
            return
        workerID, _, respCP = mp

        # New worker connection
        if workerID not in self.responses:
            assert respCP == b"connect"
            print("New Worker:", workerID)
            self.responses[workerID] = None
            return

        assert self.responses[workerID] is None
        self.responses[workerID] = respCP

    def _get_worker_response(self, workerID, timeout=None):
        assert workerID in self.responses
        # NB: Will block/hang on dead worker w/o specifying a timeout
        startTime = time.time()
        while self.responses[workerID] is None:
            # Check for timeout
            if (timeout is not None) and (time.time() - startTime) > timeout:
                raise RuntimeError("Timout waiting for worker response")

            # Handle responses waiting for come back
            self._pollForResponses(100)

        # Return the response, and reset the semaphore
        resp = self.responses[workerID]
        self.responses[workerID] = None
        # NB: Below can raise exceptions!
        pyobj = loads(resp)
        print(workerID, pyobj)
        return pyobj

    def _isReady(self, workerID):
        self._pollForResponses()
        return self.responses[workerID] is not None

    def _getWorkerPromise(self, workerID):
        return TinyPromise(lambda: self._isReady(workerID),
                           lambda: self._get_worker_response(workerID))

    def _sendToWorker(self, workerID, *args):
        # All other functions route through this,
        # so harsh do sanity checks
        assert type(workerID) is bytes
        assert workerID in self.responses
        assert all([type(arg) is bytes for arg in args])
        assert len(args) > 0

        self.worksock.send_multipart((workerID, b"") + args)

    def killWorker(self, workerID):
        self._sendToWorker(workerID, b"die")
        self.responses.pop(workerID)

    def killWorkers(self, iterable):
        for workerID in iterable:
            self.killWorker(workerID)

    def getWorkerList(self):
        return list(self.responses.keys())

    def isworking_async(self, workerID):
        self._sendToWorker(workerID, b"isworking")
        return self._getWorkerPromise(workerID)

    def isworking_sync(self, workerID):
        return self.isworking_async(workerID).getResult()

    def block_on_workers_done(self, workerList):
        # MRG NOTE: Optimize me
        workersStillWorking = list(workerList)
        while len(workersStillWorking) != 0:
            workersStillWorking = [wid for wid in workersStillWorking if self.isworking_sync(wid)]
            print("Waiting workers" + str(len(workersStillWorking)))

    def get_data_async(self, workerID, dataID):
        self._sendToWorker(workerID, b"getdata", dataID)
        return self._getWorkerPromise(workerID)

    def get_data_sync(self, workerID, dataID):
        return self.get_data_async(workerID, dataID).getResult()

    def set_data_async(self, workerID, dataID, data):
        self._sendToWorker(workerID, b"setdata", dataID, data)
        return self._getWorkerPromise(workerID)

    def set_data_sync(self, workerID, dataID, data):
        return self.set_data_async(workerID, dataID, data).getResult()

    def list_data_sync(self, workerID):
        return self.list_data_async(workerID).getResult()

    def list_data_async(self, workerID):
        self._sendToWorker(workerID, b"listdata")
        return self._getWorkerPromise(workerID)

    def send_function_sync(self, workerID, functionID, functionOrCP):
        return self.send_function_async(workerID, functionID, functionOrCP).getResult()

    def send_function_async(self, workerID, functionID, functionOrCP):
        if callable(functionOrCP):
            functionOrCP = toCP(functionOrCP)
        self._send_setdata(workerID, functionOrCP)
        return self._getWorkerPromise(workerID)

    def call_async(self, workerID, functionID, argIDs, outID):
        allIDs = (b"map", functionID,) + argIDs + (outID,)
        self._sendToWorker(workerID, *allIDs)
        return self._getWorkerPromise(workerID)

    def call_sync(self, workerID, functionID, argIDs, outID):
        return self.call_async(workerID, functionID, argIDs, outID).getResult()

    def call_multi_sync(self, workerIDs, functionID, argIDs, outID):
        assert len(workerIDs) > 0

        for workerID in workerIDs:
            self._send_call(workerID, functionID, argIDs, outID)

        return [self._get_worker_response(workerID) for workerID in workerIDs]

    def call_multi_async(self, workerIDs, functionID, argIDs, outID):
        p = EmptyPromise()
        for wid in workerIDs:
            p += self.call_async(wid, functionID, argIDs, outID)
        return p

    def map_sync(self, workerID, fID, argIDs, outID):
        return self.map_async(workerID, fID, argIDs, outID).getResult()

    def map_async(self, workerID, fID, argIDs, outID):
        # MRG Note, I don't like this implementation
        # This speaks to a need to differentiate map/call on worker-side
        def doMap(r):
            self.call_sync(workerID, fID, argIDs, outID)
            r.append(self.get_data_sync(workerID, outID))

        result = []
        t = Thread(target=doMap, args=(result,))
        t.start()

        def getResult():
            t.join()
            return result[0]

        return TinyPromise(lambda: t.is_alive, lambda: getResult)

    def map_multi_sync(self, workerIDs, fID, argIDs, outID):
        p = self.map_multi_async(workerIDs, fID, argIDs, outID)
        return p.getResult()

    def map_multi_async(self, workerIDs, fID, argIDs, outID):
        # MRG Note: suboptimal!  This blocks on all function
        # completions before making final getdata requests.
        # See note in map_async()
        p = self.call_multi_async(workerIDs, fID, argIDs, outID)
        p.getResult()

        promise = EmptyPromise()
        for workerID in workerIDs:
            promise += self.get_data_async(workerID, outID)

        return promise
