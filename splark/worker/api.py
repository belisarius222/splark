import time
from pickle import loads
from threading import Thread

import zmq

from splark.misc import toCP


class TinyPromise:
    def __init__(self, isReady, getResult):
        self.isReady = isReady
        self.getResult = getResult

    def __add__(self, otherPromise):
        bothReady = lambda: self.isReady() and otherPromise.isReady()
        results = lambda: (self.getResult(), otherPromise.getResult())
        return TinyPromise(bothReady, results)


class EmptyPromise:
    def isReady(self):
        return True

    def getResult(self):
        raise NotImplemented()

    def __add__(self, otherPromise):
        return otherPromise

    def __radd__(self, otherPromise):
        return otherPromise


class MultiWorkerAPI:
    def __init__(self, workport):
        self.ctx = zmq.Context()

        self.worksock = self.ctx.socket(zmq.ROUTER)
        self.worksock.bind("tcp://*:" + str(workport))

        self.workerIDs = []

        # WorkerID to cloudPickle
        self.responses = {}

    def make_sync(method):
        """
        A function to create synchronous versions of methods.
        This is not an instance method.
        """
        def sync_method(*args, **kwargs):
            promise = method(*args, **kwargs)
            return promise.getResult()

        return sync_method

    def close(self):
        self.worksock.close()

    def _pollForResponses(self, timeout=0):
        if self.worksock.poll(timeout):
            self._handle_worker_response()

    def _handle_worker_response(self):
        assert self.worksock.poll(0)
        workerID, _, responseCP = self.worksock.recv_multipart()

        # New worker connection
        if not self.is_worker_connected(workerID):
            self._handle_worker_response(workerID, responseCP)

        assert self.responses[workerID] is None
        self.responses[workerID] = responseCP

    def _handle_worker_connect(self, workerID, responseCP):
        assert loads(responseCP) == b"connect"
        print("New Worker:", workerID)
        self.responses[workerID] = None

    def _get_worker_response(self, workerID, timeout=None):
        # NB: Will block/hang on dead worker w/o specifying a timeout
        startTime = time.time()
        while not self.is_worker_connected(workerID):
            # Check for timeout
            if (timeout is not None) and (time.time() - startTime) > timeout:
                raise RuntimeError("Timout waiting for worker response")

            # Handle responses waiting for come back
            self._pollForResponses(100)

        # Return the response, and reset the semaphore
        resp = self.responses[workerID]
        self.responses[workerID] = None
        # NB: Below can raise exceptions!
        return loads(resp)

    def _isReady(self, workerID):
        self._pollForResponses()
        return self.responses[workerID] is not None

    def _getWorkerPromise(self, workerID):
        return TinyPromise(lambda: self._isReady(workerID),
                           lambda: self._get_worker_response(workerID))

    def _sendToWorker(self, workerID, *args):
        assert type(workerID) is bytes
        assert self.is_worker_connected(workerID)
        assert all([type(arg) is bytes for arg in args])

        self.workport.send((workerID, b"") + args)
        return self._getWorkerPromise(workerID)

    def get_worker_list(self):
        return list(self.responses.keys())

    def get_data_async(self, workerID, dataID):
        return self._sendToWorker(workerID, b"getdata", dataID)
    get_data_sync = make_sync(get_data_async)

    def set_data_async(self, workerID, dataID, data):
        return self._sendToWorker(workerID, b"setdata", dataID, data)
    set_data_sync = make_sync(set_data_async)

    def send_function_async(self, workerID, functionID, functionOrCP):
        if callable(functionOrCP):
            functionOrCP = toCP(functionOrCP)
        return self._send_setdata(workerID, functionOrCP)
    send_function_sync = make_sync(send_function_async)

    def call_async(self, workerID, functionID, argIDs, outID):
        allIDs = (functionID,) + argIDs + (outID,)
        return self._sendToWorker(workerID, *allIDs)
    call_sync = make_sync(call_async)

    def call_multi_sync(self, workerIDs, functionID, argIDs, outID):
        assert len(workerIDs) > 0

        for workerID in workerIDs:
            self._send_call(workerID, functionID, argIDs, outID)

        return [self._get_worker_response(workerID) for workerID in workerIDs]

    def call_multi_async(self, workerIDs, functionID, argIDs, outID):
        mappable = lambda wid: self.call_async(wid, functionID, argIDs, outID)
        return sum(map(mappable, workerIDs))

    def map_async(self, workerID, fID, argIDs, outID):
        # MRG Note, I don't like this implementation
        # This speaks to a need to differentiate map/call on worker-side
        def doMap(r):
            self.call_sync(workerID, fID, argIDs, outID)
            r.append(self.get_data_sync(outID))

        result = []
        t = Thread(target=doMap, args=(result,))
        t.start()

        def getResult():
            t.join()
            return result[0]

        return TinyPromise(lambda: not t.is_alive, getResult)
    map_sync = make_sync(map_async)

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
    map_multi_sync = make_sync(map_multi_async)

    def is_worker_connected(self, workerID):
        return workerID in self.responses
