from nose.tools import istest

from memcrashed.pool import Pool, PoolRepository
from .utils import ServerTestCase


class PoolRepositoryTest(ServerTestCase):
    @istest
    def gets_pool_for_key(self):
        repository = PoolRepository()

        pool = repository.pool_for_key('foo')

        self.assertIsInstance(pool, Pool)
