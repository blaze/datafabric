import atexit
import itertools
import os

import distributed

class YellowPages(object):
    """ A manager class for shared memory blocks.

        `YellowPages` is a simple interface for allocating and interfacing with shared
        memory blocks. It is built on top of an executor from the `distributed` module.
    """

    class Block(object):
        def __init__(self, capacity):
            self.capacity = capacity
            self.size = 0
            self.offset = 0
            self.variables = {}

        def __contains__(self, name):
            return name in self.variables

        def __getitem__(self, name):
            return self.variables[name]

        def insert(self, name, size):
            if (self.offset + size > self.capacity):
                raise ValueError('block is at capacity')

            self.size += size
            self.variables[name] = size, self.offset
            self.offset += size

        def remove(self, name):
            size, offset = self.variables.pop(name)
            self.size -= size

    def __init__(self, executor):
        self._executor = executor
        self._blocks = {}

        atexit.register(self.clear)

    def allocate(self, names, size):
        """ Allocate shared memory blocks of the specified size. This method will find a worker
            to put each shared memory block.

            Parameters
            ----------
            names:
                an iterable of names for the shared memory blocks
            size:
                the size of each shared memory block, which should be a single value not an iterable
        """

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
        """ Safely remove all shared memory blocks managed by this instace of `YellowPages`.

            Normally, this *must* be called by the user, as we cannot necessarily rely on `__del__`
            being called. We do, however, prevent never deallocating the shared memory blocks by
            registering this method with `atexit`.
        """

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
        """ Return the IP addresses of all workers with shared memory blocks.
        """

        return self._blocks.keys()

    def blocks(self, ip_only = True):
        """ Return a list containing the IP addresses and the name of each shared memory block.

            If `ip_only = True`, then each item in the list is a tuple of the form `(ip, block_name)`.
            If `ip_only` is `False`, then each item in the list if a tuple of the form `(ip, block_name,
            block_capacity, block_size)`.
        """

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
        """ Insert a variable into a shared memory block with enough space.

            Parameters
            ----------
            name:
                the variable name as a string
            size:
                the size of the variable in bytes
        """

        for blocks in self._blocks.values():
            for block in blocks.values():
                try:
                    block.insert(name, size)
                    return
                except Exception:
                    pass

        raise ValueError('there is not enough space to insert variable \'{}\' of size {} bytes'.format(name, size))

    def remove(self, name):
        """ Remove a variable from a shared memory block.

            Parameters
            ----------
            name:
                the variable name as a string
        """

        for blocks in self._blocks.values():
            for block in blocks.values():
                if name in block:
                    return block.remove(name)

    def find(self, name, ip_only = True):
        """ Find which shared memory block contains a variable.

            If `ip_only = True`, then a tuple of the form `(ip, block_name)` is returned.
            If `ip_only` is `False`, then a tuple of the form `(ip, block_name, block_capacity, block_size, variable_size, variable_offset)`
            is returned.

            Parameters
            ----------
            name:
                the variable name as a string
            ip_only:
                a flag indicating whether only the IP address should be returned
        """

        for ip, blocks in self._blocks.items():
            for block_name, block in blocks.items():
                if name in block:
                    if ip_only:
                        return (ip, block_name)
                    else:
                        return (ip, block_name, block.capacity, block.size) + block[name]

        raise LookupError('no variable with name \'{}\''.format(name))
