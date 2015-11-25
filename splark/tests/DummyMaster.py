class DummyMaster:
    def __init__(self, workport=23456, logport=23457):
        self.rdds = {}
        self.num_workers = 0

    def release_ports(self):
        pass

    def wait_for_worker_connections(self, n=4):
        self.num_workers = n

    def kill_workers(self):
        self.rdds = {}
        self.num_workers = 0

    def wait_for_workers_to_finish(self):
        pass

    def set_data(self, idee, rdd_data):
        self.rdds[idee] = partition_RDD(rdd_data, self.num_workers)

    def get_data(self, idee):
        return unpartition_RDD(self.rdds[idee])

    def map(self, func, ids, exit_id):
        rdds = [self.rdds[idee] for idee in ids]
        zipped_RDD = zip_RDDs(rdds)

        self.rdds[exit_id] = [func(i, p) for i, p in enumerate(zipped_RDD)]


def partition_RDD(rdd, num_partitions):
    return [list(rdd[i::num_partitions]) for i in range(num_partitions)]


def unpartition_RDD(rdd):
    rdd_as_list_of_lists = [list(partition) for partition in rdd]
    num_partitions = len(rdd_as_list_of_lists)
    rdd_length = sum(len(p) for p in rdd_as_list_of_lists)
    ret = []
    for i in range(rdd_length):
        index_in_partition, partition_index = divmod(i, num_partitions)
        ret.append(rdd_as_list_of_lists[partition_index][index_in_partition])

    return ret


def zip_RDDs(rdds):
    if len(rdds) == 1:
        return rdds[0]
    num_partitions = len(rdds[0])
    unpartitioned_RDDs = [unpartition_RDD(rdd) for rdd in rdds]
    return partition_RDD(zip(*unpartitioned_RDDs), num_partitions)
