import atexit
import itertools
import os

import distributed

class YellowPages(object):
    class Block(object):
        def __init__(self, capacity):
            self.capacity = capacity
            self.size = 0
            self.variables = {}

        def __contains__(self, name):
            return name in self.variables

        def __getitem__(self, name):
            return self.variables[name]

        def insert(self, name, size):
            if (self.size + size > self.capacity):
                raise ValueError('block is at capacity')

            self.size += size
            self.variables[name] = size

        def remove(self, name):
            size = self.variables.pop(name)
            self.size -= size

    def __init__(self, executor):
        self._executor = executor
        self._blocks = {}

        atexit.register(self.clear)

    def __getitem__(self, name):
        return self._blocks[name]

    def allocate(self, names, size):
        def func(name, size):
            import posix_ipc

            sm = posix_ipc.SharedMemory(name, flags = posix_ipc.O_CREAT | posix_ipc.O_EXCL, size = size)
            sm.close_fd()

            return YellowPages.Block(size)

        futures = self._executor.map(func, names, itertools.repeat(size))
        self._executor.gather(futures)

        s = distributed.sync(self._executor.loop, self._executor.scheduler.who_has)
        for name, future in zip(names, futures):
            ip = itertools.chain(*s[future.key]).next()
            try:
                block = self._blocks[ip]
            except KeyError:
                block = {}
                self._blocks[ip] = block

            block[name] = future.result()

    def clear(self):
        def func(blocks):
            import posix_ipc

            for name in blocks:
                sm = posix_ipc.SharedMemory(name)
                sm.close_fd()
                sm.unlink()

        for ip in self._blocks:
            self._executor.submit(func, self._blocks[ip], workers = (ip,))

        self._blocks.clear()

    def ips(self):
        return self._blocks.keys()

    def blocks(self, ip_only = True):
        res = []
        for ip, blocks in self._blocks.items():
            for name in blocks:
                if ip_only:
                    res.append((ip, name))
                else:
                    block = blocks[name]
                    res.append((ip, name, block.capacity, block.size))

        return res

    def insert(self, name, size):
        for blocks in self._blocks.values():
            for block in blocks.values():
                try:
                    block.insert(name, size)
                    return
                except Exception:
                    pass

        raise ValueError('there is not enough space to insert variable \'{}\' of size {} bytes'.format(name, size))

    def remove(self, name):
        for blocks in self._blocks.values():
            for block in blocks.values():
                if name in block:
                    return block.remove(name)

    def find(self, variable, ip_only = True):
        for ip, blocks in self._blocks.items():
            for name, block in blocks.items():
                if variable in block:
                    if ip_only:
                        return (ip, name)
                    else:
                        return (ip, name, block.capacity, block.size, block[variable])

        raise LookupError('no variable with name \'{}\''.format(variable))
