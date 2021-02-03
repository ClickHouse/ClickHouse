import pytest

import os
import sys
import tempfile

from .server import ServerThread


def pytest_addoption(parser):
    parser.addoption(
        "--builddir", action="store", default=None, help="Path to build directory to use binaries from",
    )


@pytest.fixture(scope='module')
def cmdopts(request):
    return {
        'builddir': request.config.getoption("--builddir"),
    }


@pytest.fixture(scope='module')
def bin_prefix(cmdopts):
    prefix = 'clickhouse'
    if cmdopts['builddir'] is not None:
        prefix = os.path.join(cmdopts['builddir'], 'programs', prefix)
    return prefix


# TODO: also support stateful queries.
QUERIES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '0_stateless')

@pytest.fixture(scope='module', params=[f for f in os.listdir(QUERIES_PATH) if f.endswith('.sql')])
def sql_query(request):
    return os.path.join(QUERIES_PATH, os.path.splitext(request.param)[0])


@pytest.fixture(scope='module', params=[f for f in os.listdir(QUERIES_PATH) if f.endswith('.sh')])
def shell_query(request):
    return os.path.join(QUERIES_PATH, os.path.splitext(request.param)[0])


@pytest.fixture
def standalone_server(bin_prefix, tmp_path):
    server = ServerThread(bin_prefix, str(tmp_path))
    server.start()
    wait_result = server.wait()

    if wait_result is not None:
        with open(os.path.join(server.log_dir, 'server', 'stdout.txt'), 'r') as f:
            print(f.read(), file=sys.stderr)
        with open(os.path.join(server.log_dir, 'server', 'stderr.txt'), 'r') as f:
            print(f.read(), file=sys.stderr)
        pytest.fail('Server died unexpectedly with code {code}'.format(code=server._proc.returncode), pytrace=False)

    yield server

    server.stop()
