import pytest

import difflib
import os
import random
import string
import subprocess
import sys


def check_result(result, error, return_code, reference, replace_map):
    for old, new in replace_map.items():
        result = result.replace(old.encode('utf-8'), new.encode('utf-8'))

    if return_code != 0:
        try:
            print(error.decode('utf-8'), file=sys.stderr)
        except UnicodeDecodeError:
            print(error.decode('latin1'), file=sys.stderr)  # encoding with 1 symbol per 1 byte, covering all values
        pytest.fail('Client died unexpectedly with code {code}'.format(code=return_code), pytrace=False)
    elif result != reference:
        pytest.fail("Query output doesn't match reference:{eol}{diff}".format(
                eol=os.linesep,
                diff=os.linesep.join(l.strip() for l in difflib.unified_diff(reference.decode('utf-8').splitlines(),
                                                                             result.decode('utf-8').splitlines(),
                                                                             fromfile='expected', tofile='actual'))),
            pytrace=False)


def run_client(bin_prefix, port, query, reference, replace_map={}):
    # We can't use `text=True` since some tests may return binary data
    client = subprocess.Popen([bin_prefix + '-client', '--port', str(port), '-m', '-n', '--testmode', '--use_antlr_parser=1'],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, error = client.communicate(query.encode('utf-8'))
    assert client.returncode is not None, "Client should exit after processing all queries"

    check_result(result, error, client.returncode, reference, replace_map)


def run_shell(bin_prefix, server, database, path, reference, replace_map={}):
    env = {
        'CLICKHOUSE_BINARY': bin_prefix,
        'CLICKHOUSE_DATABASE': database,
        'CLICKHOUSE_PORT_TCP': str(server.tcp_port),
        'CLICKHOUSE_PORT_TCP_SECURE': str(server.tcps_port),
        'CLICKHOUSE_PORT_HTTP': str(server.http_port),
        'CLICKHOUSE_PORT_INTERSERVER': str(server.inter_port),
        'CLICKHOUSE_TMP': server.tmp_dir,
        'CLICKHOUSE_CONFIG_CLIENT': server.client_config
    }
    shell = subprocess.Popen([path], env=env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, error = shell.communicate()
    assert shell.returncode is not None, "Script should exit after executing all commands"

    check_result(result, error, shell.returncode, reference, replace_map)


def random_str(length=10):
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


def test_sql_query(bin_prefix, sql_query, standalone_server):
    tcp_port = standalone_server.tcp_port

    query_path = sql_query + ".sql"
    reference_path = sql_query + ".reference"

    if not os.path.exists(reference_path):
        pytest.skip('No .reference file found')

    with open(query_path, 'r') as file:
        query = file.read()
    with open(reference_path, 'rb') as file:
        reference = file.read()

    random_name = 'test_{random}'.format(random=random_str())
    query = 'CREATE DATABASE {random}; USE {random}; {query}'.format(random=random_name, query=query)
    run_client(bin_prefix, tcp_port, query, reference, {random_name: 'default'})

    query = "SELECT 'SHOW ORPHANED TABLES'; SELECT name FROM system.tables WHERE database != 'system' ORDER BY (database, name);"
    run_client(bin_prefix, tcp_port, query, b'SHOW ORPHANED TABLES\n')

    query = 'DROP DATABASE {random};'.format(random=random_name)
    run_client(bin_prefix, tcp_port, query, b'')

    query = "SELECT 'SHOW ORPHANED DATABASES'; SHOW DATABASES;"
    run_client(bin_prefix, tcp_port, query, b'SHOW ORPHANED DATABASES\n_temporary_and_external_tables\ndefault\nsystem\n')


def test_shell_query(bin_prefix, shell_query, standalone_server):
    tcp_port = standalone_server.tcp_port

    shell_path = shell_query + ".sh"
    reference_path = shell_query + ".reference"

    if not os.path.exists(reference_path):
        pytest.skip('No .reference file found')

    with open(reference_path, 'rb') as file:
        reference = file.read()

    random_name = 'test_{random}'.format(random=random_str())
    query = 'CREATE DATABASE {random};'.format(random=random_name)
    run_client(bin_prefix, tcp_port, query, b'')

    run_shell(bin_prefix, standalone_server, random_name, shell_path, reference, {random_name: 'default'})

    query = "SELECT 'SHOW ORPHANED TABLES'; SELECT name FROM system.tables WHERE database != 'system' ORDER BY (database, name);"
    run_client(bin_prefix, tcp_port, query, b'SHOW ORPHANED TABLES\n')

    query = 'DROP DATABASE {random};'.format(random=random_name)
    run_client(bin_prefix, tcp_port, query, b'')

    query = "SELECT 'SHOW ORPHANED DATABASES'; SHOW DATABASES;"
    run_client(bin_prefix, tcp_port, query, b'SHOW ORPHANED DATABASES\n_temporary_and_external_tables\ndefault\nsystem\n')
