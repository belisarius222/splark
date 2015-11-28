import itertools, time

import zmq

from splark.misc import fromCP, toCP


class Master:
    def __init__(self, workport=23456, logport=23457):
        self.workport = workport
        self.logport = logport

        self.ctx = zmq.Context()

        self.worksock = self.ctx.socket(zmq.ROUTER)
        self.worksock.bind("tcp://*:" + str(workport))

        self.stdsocket = self.ctx.socket(zmq.PULL)
        self.stdsocket.bind("tcp://*:" + str(logport))

        self.num_workers = 0
        self.worker_ids = []

    def release_ports(self):
        self.worksock.close(linger=0)
        self.stdsocket.close(linger=0)
        self.ctx.term()

    def wait_for_worker_connections(self, n=4, timeout=1000):
        poller = zmq.Poller()
        poller.register(self.worksock, zmq.POLLIN)
        poller.register(self.stdsocket, zmq.POLLIN)

        start_time = time.time()

        while len(self.worker_ids) < n:
            socks = dict(poller.poll())
            if socks.get(self.worksock) == zmq.POLLIN:
                self._handle_worker_connect()

            if socks.get(self.stdsocket) == zmq.POLLIN:
                self._handle_worker_stdout()

            if time.time() - start_time > (timeout / 1000):
                raise TimeoutError("Failed to receive {} worker connections".format(n))

    def _handle_worker_connect(self):
        worker_id, connect_message = self.recv_from_worker()
        assert connect_message == b"connect", "Received invalid connect message from worker: {}".format(connect_message)
        self.worker_ids.append(worker_id)

    def _handle_worker_stdout(self):
        line = self.stdsocket.recv()
        if type(line) is bytes:
            line = line.decode("utf-8")
        print(line)

    def kill_workers(self):
        self.send_cmd_to_all_workers(itertools.repeat((b"die",)))
        self.worker_ids = []

    def wait_for_workers_to_finish(self, refresh_timeout=200):
        while True:
            responses = self.transact_to_all_workers(itertools.repeat((b"isworking",)))
            if all(response is False for response in responses):
                break
            time.sleep(refresh_timeout / 1000)

    def set_data(self, data_id, iterator):
        get_cmd_for_element = lambda element: (b"setdata", data_id, toCP(element))
        cmd_tuple_iterator = map(get_cmd_for_element, iterator)

        responses = self.transact_to_all_workers(cmd_tuple_iterator)

        responses_are_true = [response is True for response in responses]
        if not all(responses_are_true):
            failed_worker_ids = [self.worker_ids[ix] for ix, ok in enumerate(responses_are_true) if not ok]
            raise ValueError("Workers {} failed 'setdata' command.".format(failed_worker_ids))

    def get_data(self, data_id):
        cmd_tuple = (b"getdata", data_id)
        return self.transact_to_all_workers(itertools.repeat(cmd_tuple))

    def map(self, func_id, ids, exit_id):
        cmd_tuple = (b"map", func_id) + ids + (exit_id,)
        responses = self.transact_to_all_workers(itertools.repeat(cmd_tuple))
        responses_are_true = [response is True for response in responses]
        if not all(responses_are_true):
            failed_worker_ids = [self.worker_ids[ix] for ix, ok in enumerate(responses_are_true) if not ok]
            raise ValueError("Workers {} failed 'map' command.".format(failed_worker_ids))

    def transact_to_all_workers(self, cmd_tuple_iterator, timeout=1000):
        poller = zmq.Poller()
        poller.register(self.worksock, zmq.POLLIN)
        poller.register(self.stdsocket, zmq.POLLIN)

        start_time = time.time()

        self.send_cmd_to_all_workers(cmd_tuple_iterator)

        worker_id_to_response = {}
        while set(worker_id_to_response.keys()) != set(self.worker_ids):
            socks = dict(poller.poll(timeout))
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
