
class SharedMemoryManager(object):
    def __init__(self):
        self._blocks = {} # this is a dict of IP addresses and blocks

    def __getitem__(self, name):
        return self._blocks[name]

    def get_locations(self, name):
        pass

    def allocate(self, names, size):
        futures = e.map(_allocate, names, len(names) * [size])
        e.gather(futures)

        s = sync(e.loop, e.scheduler.who_has)
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
