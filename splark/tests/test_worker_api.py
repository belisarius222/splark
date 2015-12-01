import itertools, time

from splark.worker import MultiWorkerAPI, Worker

_workport = itertools.count(23456)
_logport = itertools.count(34567)


class WorkerAPIContext:
    def __init__(self, num_workers=4):
        self.num_workers = num_workers

    def __enter__(self):
        workport = next(_workport)
        logport = next(_logport)

        self.api = MultiWorkerAPI(workport)

        self.workers = []
        for i in range(self.num_workers):
            worker = Worker("tcp://localhost", workport, logport)
            self.workers.append(worker)
            worker.start()

        return self.api, self.workers

    def __exit__(self, *args):
        self.api.close()
        for worker in self.workers:
            assert worker.is_alive(), "Worker died during testing."
            worker.terminate()

        for worker in self.workers:
            worker.join()


def test_get_worker_list():
    with WorkerAPIContext() as (api, workers):
        time.sleep(0.1)
        assert len(api.get_worker_list()) == len(workers), api.get_worker_list()
