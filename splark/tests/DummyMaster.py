class DummyMaster:
    def __init__(self, workport=23456, logport=23457):
        self.data = {}
        self.num_workers = 0

    def release_ports(self):
        pass

    def wait_for_worker_connections(self, n=4):
        self.num_workers = n

    def kill_workers(self):
        self.data = {}
        self.num_workers = 0

    def wait_for_workers_to_finish(self):
        pass

    def set_data(self, idee, data):
        data_iterator = iter(data)
        self.data[idee] = [next(data_iterator) for _ in range(self.num_workers)]

    def get_data(self, idee):
        return self.data[idee]

    def map(self, func_id, ids, exit_id):
        rdds = [self.data[idee] for idee in ids]
        zipped_RDD = list(zip(*rdds))

        func = self.data[func_id][0]
        self.data[exit_id] = [func(*p) for p in zipped_RDD]
