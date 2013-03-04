class ProxyRepository(object):
    def proxy_for_key(self, key):
        return Proxy(key)


class Proxy(object):
    def __init__(self, key):
        self.key = key
