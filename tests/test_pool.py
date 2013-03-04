from nose.tools import istest

from memcrashed.proxy import Proxy, ProxyRepository
from .utils import ServerTestCase


class PoolRepositoryTest(ServerTestCase):
    @istest
    def gets_pool_for_key(self):
        repository = ProxyRepository()

        pool = repository.proxy_for_key('foo')

        self.assertIsInstance(pool, Proxy)
