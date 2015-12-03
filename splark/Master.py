from threading import Thread

import zmq

from splark.worker import MultiWorkerAPI
from splark.misc import toCP, EmptyPromise


# MRG TODO: Make me a py-thread.
class StdoutHandler(Thread):
    def __init__(self, uri):
        self.uri = uri
        Thread.__init__(self)

    def setup(self):
        ctx = zmq.Context()

        self.stdsocket = ctx.socket(zmq.PULL)
        self.stdsocket.bind(self.uri)

        self.die = False

    def close(self):
        self.die = True
        self.join()
        self.stdsocket.close(linger=0)

    def run(self):
        self.setup()
        while not self.die:
            if not self.stdsocket.poll(100):
                continue

            line = self.stdsocket.recv()
            if type(line) is bytes:
                line = line.decode("utf-8")
            print(line)


class Master:
    def __init__(self, workport=23456, logport=23457):
        self.workport = workport
        self.logport = logport

        self.workapi = MultiWorkerAPI("tcp://*:" + str(workport))
        self.sHandler = StdoutHandler("tcp://*:" + str(logport))
        self.sHandler.start()

    def release_ports(self):
        self.workapi.close()
        # self.sHandler.close()

    def wait_for_worker_connections(self, n=4, timeout=1000):
        self.worker_ids = self.workapi.waitForWorkers(n, timeout)
        self.num_workers = len(self.worker_ids)
        assert self.num_workers == n

    def kill_workers(self):
        self.workapi.killWorkers(self.worker_ids)
        self.worker_ids = []
        self.num_workers = 0

    def wait_for_workers_to_finish(self):
        print("Waiting on worker completion")
        self.workapi.block_on_workers_done(self.worker_ids)

    def broadcast_datum(self, data_id, datum):
        promise = EmptyPromise()
        cp = toCP(datum)
        for wid in self.worker_ids:
            promise += self.workapi.set_data_async(wid, data_id, cp)

        responses = promise.getResult()
        failed = [wid for wid, response in zip(self.worker_ids, responses) if not response]
        if len(failed) != 0:
            raise ValueError("Workers {} failed 'setdata' command.".format(failed))

    def set_data(self, data_id, datae):
        assert len(datae) == self.num_workers

        promise = EmptyPromise()
        for wid, datum in zip(self.worker_ids, datae):
            promise += self.workapi.set_data_async(wid, data_id, toCP(datum))

        responses = promise.getResult()
        print("Setdata result:" + repr(responses))
        failed = [wid for wid, response in zip(self.worker_ids, responses) if not response]
        if len(failed) != 0:
            raise ValueError("Workers {} failed 'setdata' command.".format(failed))

    def get_data(self, data_id):
        promise = EmptyPromise()
        for wid in self.worker_ids:
            promise += self.workapi.get_data_async(wid, data_id)

        return promise.getResult()

    def map(self, func_id, ids, exit_id):
        promise = EmptyPromise()
        for wid in self.worker_ids:
            promise += self.workapi.map_async(wid, func_id, ids, exit_id)

        responses = promise.getResult()

        failed = [wid for wid, response in zip(self.worker_ids, responses) if not response]
        if len(failed) != 0:
            raise ValueError("Workers {} failed 'map' command.".format(failed))
