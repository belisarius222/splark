import itertools, time

import zmq

from splark.misc import fromCP, toCP


class Master:
    def __init__(self, workport=23456, logport=23457):
        self.workport = workport
        self.logport = logport

        ctx = zmq.Context()

        self.worksock = ctx.socket(zmq.ROUTER)
        self.worksock.bind("tcp://*:" + str(workport))

        self.stdsocket = ctx.socket(zmq.PULL)
        self.stdsocket.bind("tcp://*:" + str(logport))

        self.num_workers = 0
        self.worker_ids = []

    def wait_for_worker_connections(self, n=4):
        poller = zmq.Poller()
        poller.register(self.worksock, zmq.POLLIN)
        poller.register(self.stdsocket, zmq.POLLIN)

        while len(self.workers) < n:
            socks = dict(poller.poll())
            if socks.get(self.worksock) == zmq.POLLIN:
                self._handle_worker_connect()

            if socks.get(self.stdsocket) == zmq.POLLIN:
                self._handle_worker_stdout()

    def _handle_worker_connect(self):
        worker_id, _ = self.recv_from_worker()
        self.workers.append(worker_id)

    def _handle_worker_stdout(self):
        print(self.stdsocket.recv())

    def kill_workers(self):
        self.transact_to_all_workers(b"die", timeout=5000)
        self.worker_ids = []

    def wait_for_workers_to_finish(self):
        pass

    def set_data(self, data_id, blobs):
        get_data_for_index = lambda worker_index: (b"setdata", data_id, toCP(blobs[worker_index]))
        cmd_tuple_iterator = map(get_data_for_index, itertools.count(0))

        responses = self.transact_to_all_workers(cmd_tuple_iterator)

        responses_are_true = [response is True for response in responses]
        if not all(responses_are_true):
            failed_worker_ids = [self.worker_ids[ix] for ix, ok in enumerate(responses_are_true) if not ok]
            raise ValueError("Workers {} failed 'setdata' command.".format(failed_worker_ids))

    def get_data(self, data_id):
        cmd_tuple = (b"getdata", data_id)
        ordered_responses = self.transact_to_all_workers(b"getdata", itertools.repeat(cmd_tuple))
        return ordered_responses

    def map(self, func, ids, exit_id):
        pass

    def transact_to_all_workers(self, cmd_tuple_iterator, timeout=1000):
        poller = zmq.Poller()
        poller.register(self.worksock, zmq.POLLIN)
        poller.register(self.stdsocket, zmq.POLLIN)

        start_time = time.time()

        self.send_cmd_to_all_workers(cmd_tuple_iterator)

        worker_id_to_response = {}
        while set(worker_id_to_response.keys()) != self.worker_ids:
            socks = dict(poller.poll())
            if socks.get(self.worksock) == zmq.POLLIN:
                worker_id, response = self.recv_from_worker()
                worker_id_to_response[worker_id] = response

            if socks.get(self.stdsocket) == zmq.POLLIN:
                self._handle_worker_stdout()

            if time.time() - start_time > (timeout / 1000):
                failed_worker_ids = set(self.worker_ids) - set(worker_id_to_response.keys())
                raise TimeoutError("Workers {} failed to respond to command".format(failed_worker_ids))

        ordered_responses = [worker_id_to_response[idee] for idee in self.worker_ids]
        return ordered_responses

    def send_cmd_to_all_workers(self, cmd_tuple_iterator):
        cmd_tuple_iterator = iter(cmd_tuple_iterator)
        for worker_id in self.worker_ids:
            args = next(cmd_tuple_iterator)
            self.send_cmd_to_worker(worker_id, *args)

    def send_cmd_to_worker(self, worker_id, *args):
        self.worksock.send_multipart((worker_id, b"") + args)

    def recv_from_worker(self):
        worker_id, _, response_pickle = self.worksock.recv_multipart()
        response = fromCP(response_pickle)
        return worker_id, response
