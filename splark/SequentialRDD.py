import itertools

from splark import RDD


class SequentialRDD(RDD):
    def partition(self, data):
        if "__len__" not in dir(data):
            raise ValueError("SequentialRDD must have a fixed-length input.")

        partition_base_size, extra_elements = divmod(len(data), self.num_partitions)

        partitions = []
        data_index = 0
        for i in range(self.num_partitions):
            partition_size = partition_base_size

            # Store any extra elements near the front of the list, arbitrarily.
            if i <= extra_elements:
                partition_size += 1

            partitions.append(data[data_index:data_index + partition_size])
            data_index += partition_size

        return partitions

    def unpartition(self, data):
        return list(itertools.chain.from_iterable(data))
