import itertools
import os

from distributed import sync

from posix_ipc import SharedMemory, O_CREAT

class SharedMemoryBlock(object):
    def __init__(self, name, capacity):
        self._shared_memory = SharedMemory(name, flags = O_CREAT, read_only = False)
        ff = os.fdopen(self._shared_memory.fd, 'ab')
        ff.write(b'0' * capacity)

        self._name = self._shared_memory.name
        self._capacity = capacity
        self._variables = {}

    @property
    def capacity(self):
        return self._capacity

    def insert(self, name, size):
        self._variables[name] = size
        self._capacity -= size

    def __repr__(self):
        return 'SharedMemoryBlock(name = {})'.format(self._name)

class YellowPages(object):
    def __init__(self, executor):
        self._executor = executor
        self._blocks = {} # this is a dict of IP addresses and blocks

    def __getitem__(self, name):
        return self._blocks[name]

    def blocks(self):
        res = []
        for ip, block in self._blocks.items():
            for name in block:
                res.append((ip, name))

        return res

    def allocate(self, names, size):
        futures = self._executor.map(SharedMemoryBlock, names, len(names) * [size])
        self._executor.gather(futures)

        s = sync(self._executor.loop, self._executor.scheduler.who_has)
        for future in futures:
            key = itertools.chain(*s[future.key]).next()
            try:
                block = self._blocks[key]
            except KeyError:
                block = {}
                self._blocks[key] = block

            block[future.result()._name] = future.result()

    def insert(self, name, size):
        for blocks in self._blocks.values():
            for block in blocks.values():
                if size <= block.capacity:
                    block.insert(name, size)

    def nodes(self):
        return self._blocks

    def read(self):
        pass
