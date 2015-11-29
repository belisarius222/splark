from multiprocessing import Process
import sys

import zmq

from splark.misc import fromCP, toCP


class InnerWorker(Process):
    def __init__(self, uri, stdout=sys.stdout, stderr=sys.stderr, **kwargs):
        self.stdout = stdout
        self.stderr = stderr
        Process.__init__(self, **kwargs)

        self._uri = uri

    def setup(self):
        self._ctx = zmq.Context()
        self._skt = self._ctx.socket(zmq.REP)
        self._skt.connect(self._uri)

        sys.stdout = self.stdout
        sys.stderr = self.stderr

    def run(self):
        self.setup()

        while True:
            parts = self._skt.recv_multipart()

            # Un-cloudpickle everything
            # f, ArgList1, ArgList2, ArgList3 . . .
            f, *args = [fromCP(part) for part in parts]

            # Do the actual work!
            # MRG TODO: This could be much more sophisticated.
            results = f(*args)

            self._skt.send(toCP(results))
