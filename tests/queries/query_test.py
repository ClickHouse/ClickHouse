import pytest

import difflib
import os
import random
import string
import subprocess
import sys


def run_client(bin_prefix, port, query, reference, replace_map={}):
    client = subprocess.Popen([bin_prefix + '-client', '--port', str(port), '-m', '-n', '--testmode'],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, error = client.communicate(query)
    assert client.returncode is not None, "Client should exit after processing all queries"

    for old, new in replace_map.iteritems():
        result = result.replace(old, new)

    if client.returncode != 0:
        print >> sys.stderr, error
        pytest.fail('Client died unexpectedly with code {code}'.format(code=client.returncode), pytrace=False)
    elif result != reference:
        pytest.fail("Query output doesn't match reference:{eol}{diff}".format(
                eol=os.linesep,
                diff=os.linesep.join(l.strip() for l in difflib.unified_diff(reference.splitlines(), result.splitlines(), fromfile='expected', tofile='actual'))),
            pytrace=False)


def random_str(length=10):
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


@pytest.mark.timeout(timeout=10, method='signal')
def test_query(bin_prefix, sql_query, standalone_server):
    tcp_port = standalone_server.tcp_port

    query_path = sql_query + ".sql"
    reference_path = sql_query + ".reference"

    if not os.path.exists(reference_path):
        pytest.skip('No .reference file found')

    with open(query_path, 'r') as file:
        query = file.read()
    with open(reference_path, 'r') as file:
        reference = file.read()

    random_name = 'test_{random}'.format(random=random_str())
    query = 'CREATE DATABASE {random}; USE {random}; {query}'.format(random=random_name, query=query)
    run_client(bin_prefix, tcp_port, query, reference, {random_name: 'default'})

    query = "SELECT 'SHOW ORPHANED TABLES'; SELECT name FROM system.tables WHERE database != 'system' ORDER BY (database, name);"
    run_client(bin_prefix, tcp_port, query, 'SHOW ORPHANED TABLES\n')

    run_client(bin_prefix, tcp_port, 'DROP DATABASE {random};'.format(random=random_name), '')

    query = "SELECT 'SHOW ORPHANED DATABASES'; SHOW DATABASES;"
    run_client(bin_prefix, tcp_port, query, 'SHOW ORPHANED DATABASES\n_temporary_and_external_tables\ndefault\nsystem\n')
