from contextlib import contextmanager
import os
import subprocess
import sys
import time

import memcache
from mock import patch
from tornado.testing import AsyncTestCase

try:
    import pylibmc  # NOQA
except ImportError:
    pylibmc = None
    PYLIBMC_EXISTS = False
else:
    PYLIBMC_EXISTS = True
PYLIBMC_SKIP_REASON = "Can't run in Python 3 because pylibmc is not yet ported."


PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))


@contextmanager
def server_running(host, port, args=[]):
    server_path = os.path.join(PROJECT_ROOT, 'memcrashed', 'server.py')
    command_args = [
        sys.executable,
        server_path,
        '-p', str(port),
        '-a', host,
    ]
    command_args.extend(args)
    env = {
        'PYTHONPATH': PROJECT_ROOT,
    }
    proc = subprocess.Popen(command_args, env=env)
    time.sleep(0.2)
    try:
        yield
    finally:
        proc.kill()


@contextmanager
def proxy_memcached(client):
    recv_results = []
    sent_messages = []
    host = client.buckets[0]
    orig_recv = host.recv
    orig_readline = host.readline
    orig_send_cmd = host.send_cmd

    def recv(*args, **kwargs):
        result = orig_recv(*args, **kwargs)
        recv_results.append(result.strip())
        return result

    def readline():
        result = orig_readline()
        recv_results.append(result.strip())
        return result

    def send_cmd(cmd):
        sent_messages.append(cmd)
        return orig_send_cmd(cmd)

    with patch.object(host, 'recv', recv), patch.object(host, 'send_cmd', send_cmd), patch.object(host, 'readline', readline):
        yield (recv_results, sent_messages)


class ServerTestCase(AsyncTestCase):
    def setUp(self):
        super(ServerTestCase, self).setUp()
        host = '127.0.0.1'
        port = 11211
        self.memcached_client = memcache.Client(['%s:%s' % (host, port)])
        self.memcached_client.flush_all()
        self.memcached_client.disconnect_all()
