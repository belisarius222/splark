import zmq

from splark import cloudpickle


class Master:
    def __init__(self):
        pass

    def bind_ports(self, ports):
        pass

    def wait_for_workers(self, n=4):
        pass

    def kill_workers(self):
        pass

    def set_data(self, id, blob):
        pass

    def get_data(self, id):
        pass

    def map(self, func, ids, exit_id):
        pass
