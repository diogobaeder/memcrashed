class ProxyRepository(object):
    def __init__(self, io_loop):
        self.io_loop = io_loop

    def proxy_for_key(self, key):
        return Proxy(key, self.io_loop)


class Proxy(object):
    def __init__(self, key, io_loop):
        self.key = key
        self.io_loop = io_loop
