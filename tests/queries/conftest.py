import os
import re
import sys

import pytest

from .server import ServerThread


# Command-line arguments

def pytest_addoption(parser):
    parser.addoption("--builddir", action="store", default=None, help="Path to build directory to use binaries from")
    parser.addoption("--antlr", action="store_true", default=False, help="Use ANTLR parser")


# HTML report hooks

def pytest_html_report_title(report):
    report.title = "ClickHouse Functional Stateless Tests (PyTest)"


RE_TEST_NAME = re.compile(r"\[(.*)\]")
def pytest_itemcollected(item):
    match = RE_TEST_NAME.search(item.name)
    if match:
        item._nodeid = match.group(1)


# Fixtures

@pytest.fixture(scope='module')
def cmdopts(request):
    return {
        'builddir': request.config.getoption("--builddir"),
        'antlr': request.config.getoption("--antlr"),
    }


@pytest.fixture(scope='module')
def bin_prefix(cmdopts):
    prefix = 'clickhouse'
    if cmdopts['builddir'] is not None:
        prefix = os.path.join(cmdopts['builddir'], 'programs', prefix)
    # FIXME: does this hangs the server start for some reason?
    # if not os.path.isabs(prefix):
    #     prefix = os.path.abspath(prefix)
    return prefix


@pytest.fixture(scope='module')
def use_antlr(cmdopts):
    return cmdopts['antlr']


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
