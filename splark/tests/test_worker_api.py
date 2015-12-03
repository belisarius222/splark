from uuid import uuid4
import sys
import traceback
from random import choice

from splark.worker import Worker
from splark.worker.api import MultiWorkerAPI
from splark.tests import getTestingLogPort, getTestingWorkPort


class ApiWithWorkers:
    def __init__(self, num_workers=4):
        self.num_workers = num_workers

        # Increment the ports each time, so we don't hit the "address already in use" error.
        self.workport = getTestingWorkPort()
        self.logport = getTestingLogPort()

        print("Working on port:", self.workport)
        self.api = MultiWorkerAPI("tcp://*:" + str(self.workport))

        self.workers = []
        for i in range(num_workers):
            worker = Worker("tcp://localhost", workport=self.workport, logport=self.logport, print_local=False)
            self.workers.append(worker)

    def __enter__(self):
        for worker in self.workers:
            worker.start()

        self.wids = self.api.waitForWorkers(self.num_workers)

        return self.api, self.workers

    def __exit__(self, *args):
        if any(arg is not None for arg in args):
            print("MasterWithWorkers.__exit__() caught an error.", file=sys.stderr)
            traceback.print_exc()

        dead_workers = [worker for worker in self.workers if not worker.is_alive()]
        if len(dead_workers) > 0:
            raise RuntimeError("Workers {} died during testing.".format(dead_workers))
            for worker in self.workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join()

        print("Killing workers.")
        self.api.killWorkers(self.wids)
        self.api.close()
        print("Closed master sockets.", flush=True)

        for worker in self.workers:
            worker.terminate()


def test_context_setup():
    with ApiWithWorkers() as (api, workers):  # NOQA
        pass


def test_wait_on_workers():
    with ApiWithWorkers() as (api, workers):
        workerIDs = api.waitForWorkers(4)
        assert len(workerIDs) == 4
        assert all([type(wid) == bytes for wid in workerIDs])


def test_setdata():
    with ApiWithWorkers() as (api, workers):
        workerIDs = api.waitForWorkers(4)
        for wid in workerIDs:
            api.set_data_sync(wid, b"foo", b"bar")

        for wid in workerIDs:
            result = api.get_data_sync(wid, b"foo")
            assert result == b"bar", repr(result)


def test_setdata_harder():
    with ApiWithWorkers() as (api, workers):
        workerIDs = api.waitForWorkers(4)

        for i in range(300):
            wid = choice(workerIDs)
            key = str(uuid4()).encode("ascii")
            data = str(uuid4()).encode("ascii")
            api.set_data_sync(wid, key, data)

            result = api.get_data_sync(wid, key)
            assert result == data, result


def test_set_list():
    with ApiWithWorkers() as (api, workers):
        workerIDs = api.waitForWorkers(4)

        ids = [str(uuid4()).encode("ascii") for i in range(300)]
        ids.sort()
        for i in ids:
            wid = choice(workerIDs)
            data = str(uuid4()).encode("ascii")
            api.set_data_sync(wid, i, data)

        listing = []
        for wid in workerIDs:
            listing = listing + api.list_data_sync(wid)
        listing.sort()
        assert listing == ids, repr(listing) + " - " + repr(ids)


def test_call_noargs():
    with ApiWithWorkers() as (api, workers):
        workerIDs = api.waitForWorkers(4)
