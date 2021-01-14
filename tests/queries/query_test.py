import pytest

import difflib
import os
import random
import string
import subprocess
import sys


SKIP_LIST = [
    # these couple of tests hangs everything
    "00600_replace_running_query",
    "00987_distributed_stack_overflow",

    # just fail
    "00302_http_compression",
    "00463_long_sessions_in_http_interface",
    "00505_secure",
    "00505_shard_secure",
    "00506_union_distributed",  # flaky
    "00646_url_engine",
    "00821_distributed_storage_with_join_on.sql",  # flaky
    "00834_cancel_http_readonly_queries_on_client_close",
    "00933_test_fix_extra_seek_on_compressed_cache",
    "00965_logs_level_bugfix",
    "00965_send_logs_level_concurrent_queries",
    "00990_hasToken",
    "00990_metric_log_table_not_empty",
    "01014_lazy_database_concurrent_recreate_reattach_and_show_tables",
    "01018_Distributed__shard_num",
    "01018_ip_dictionary",
    "01023_materialized_view_query_context",  # flaky
    "01035_lc_empty_part_bug",  # flaky
    "01037_polygon_dicts_simple_functions.sh",  # flaky
    "01046_materialized_view_with_join_over_distributed",  # flaky
    "01050_clickhouse_dict_source_with_subquery",
    "01053_ssd_dictionary",
    "01054_cache_dictionary_overflow_cell",
    "01057_http_compression_prefer_brotli",
    "01080_check_for_error_incorrect_size_of_nested_column",
    "01083_expressions_in_engine_arguments",
    "01086_odbc_roundtrip",
    "01088_benchmark_query_id",
    "01098_temporary_and_external_tables",
    "01099_parallel_distributed_insert_select",  # flaky
    "01103_check_cpu_instructions_at_startup",
    "01114_database_atomic",
    "01148_zookeeper_path_macros_unfolding",
    "01193_metadata_loading.sh",  # flaky
    "01274_alter_rename_column_distributed",  # flaky
    "01280_ssd_complex_key_dictionary",
    "01293_client_interactive_vertical_multiline",  # expect-test
    "01293_client_interactive_vertical_singleline",  # expect-test
    "01293_show_clusters",
    "01294_lazy_database_concurrent_recreate_reattach_and_show_tables",
    "01294_system_distributed_on_cluster",
    "01300_client_save_history_when_terminated",  # expect-test
    "01304_direct_io",
    "01306_benchmark_json",
    "01320_create_sync_race_condition_zookeeper",
    "01355_CSV_input_format_allow_errors",
    "01370_client_autocomplete_word_break_characters",  # expect-test
    "01375_storage_file_tsv_csv_with_names_write_prefix",  # flaky
    "01376_GROUP_BY_injective_elimination_dictGet",
    "01393_benchmark_secure_port",
    "01418_custom_settings",
    "01451_wrong_error_long_query",
    "01455_opentelemetry_distributed",
    "01473_event_time_microseconds",
    "01474_executable_dictionary",
    "01507_clickhouse_server_start_with_embedded_config",
    "01514_distributed_cancel_query_on_error",
    "01520_client_print_query_id",  # expect-test
    "01527_dist_sharding_key_dictGet_reload",
    "01545_url_file_format_settings",
    "01553_datetime64_comparison",
    "01555_system_distribution_queue_mask",
    "01558_ttest_scipy",
    "01561_mann_whitney_scipy",
    "01582_distinct_optimization",
    "01586_storage_join_low_cardinality_key",
    "01599_multiline_input_and_singleline_comments",
    "01600_benchmark_query",
    "01601_custom_tld",
    "01601_proxy_protocol",
]


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
    client = subprocess.Popen([bin_prefix + '-client', '--port', str(port), '-m', '-n', '--testmode'],
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
    for test in SKIP_LIST:
        if test in sql_query:
            pytest.skip("Test matches skip-list: " + test)
            return

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
    run_client(bin_prefix, tcp_port, query, b'SHOW ORPHANED DATABASES\ndefault\nsystem\n')


def test_shell_query(bin_prefix, shell_query, standalone_server):
    for test in SKIP_LIST:
        if test in shell_query:
            pytest.skip("Test matches skip-list: " + test)
            return

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
    run_client(bin_prefix, tcp_port, query, b'SHOW ORPHANED DATABASES\ndefault\nsystem\n')
