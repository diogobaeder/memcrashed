from nose.tools import istest

from memcrashed.proxy import Proxy, ProxyRepository
from .utils import ServerTestCase


class PoolRepositoryTest(ServerTestCase):
    @istest
    def gets_pool_for_key(self):
        repository = ProxyRepository()

        proxy = repository.proxy_for_key('foo')

        self.assertIsInstance(proxy, Proxy)
        self.assertEqual(proxy.key, 'foo')
