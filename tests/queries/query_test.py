import difflib
import os
import random
import string
import subprocess
import sys

import pytest


SKIP_LIST = [
    # these couple of tests hangs everything
    "00600_replace_running_query",
    "00987_distributed_stack_overflow",

    # just fail
    "00133_long_shard_memory_tracker_and_exception_safety",
    "00463_long_sessions_in_http_interface",
    "00505_secure",
    "00505_shard_secure",
    "00646_url_engine",
    "00725_memory_tracking",  # BROKEN
    "00738_lock_for_inner_table",
    "00821_distributed_storage_with_join_on",
    "00825_protobuf_format_array_3dim",
    "00825_protobuf_format_array_of_arrays",
    "00825_protobuf_format_enum_mapping",
    "00825_protobuf_format_nested_in_nested",
    "00825_protobuf_format_nested_optional",
    "00825_protobuf_format_no_length_delimiter",
    "00825_protobuf_format_persons",
    "00825_protobuf_format_squares",
    "00825_protobuf_format_table_default",
    "00834_cancel_http_readonly_queries_on_client_close",
    "00877_memory_limit_for_new_delete",
    "00900_parquet_load",
    "00933_test_fix_extra_seek_on_compressed_cache",
    "00965_logs_level_bugfix",
    "00965_send_logs_level_concurrent_queries",
    "00974_query_profiler",
    "00990_hasToken",
    "00990_metric_log_table_not_empty",
    "01014_lazy_database_concurrent_recreate_reattach_and_show_tables",
    "01017_uniqCombined_memory_usage",
    "01018_Distributed__shard_num",
    "01018_ip_dictionary_long",
    "01035_lc_empty_part_bug",  # FLAKY
    "01050_clickhouse_dict_source_with_subquery",
    "01053_ssd_dictionary",
    "01054_cache_dictionary_overflow_cell",
    "01057_http_compression_prefer_brotli",
    "01080_check_for_error_incorrect_size_of_nested_column",
    "01083_expressions_in_engine_arguments",
    "01086_odbc_roundtrip",
    "01088_benchmark_query_id",
    "01092_memory_profiler",
    "01098_temporary_and_external_tables",
    "01099_parallel_distributed_insert_select",
    "01103_check_cpu_instructions_at_startup",
    "01107_atomic_db_detach_attach",
    "01114_database_atomic",
    "01148_zookeeper_path_macros_unfolding",
    "01152_cross_replication",  # tcp port in reference
    "01175_distributed_ddl_output_mode_long",
    "01181_db_atomic_drop_on_cluster",  # tcp port in reference
    "01280_ssd_complex_key_dictionary",
    "01293_client_interactive_vertical_multiline",  # expect-test
    "01293_client_interactive_vertical_singleline",  # expect-test
    "01293_show_clusters",
    "01293_show_settings",
    "01293_system_distribution_queue",  # FLAKY
    "01294_lazy_database_concurrent_recreate_reattach_and_show_tables_long",
    "01294_system_distributed_on_cluster",
    "01300_client_save_history_when_terminated",  # expect-test
    "01304_direct_io",
    "01306_benchmark_json",
    "01035_lc_empty_part_bug",  # FLAKY
    "01175_distributed_ddl_output_mode_long",  # tcp port in reference
    "01320_create_sync_race_condition_zookeeper",
    "01355_CSV_input_format_allow_errors",
    "01370_client_autocomplete_word_break_characters",  # expect-test
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
    "01526_client_start_and_exit",  # expect-test
    "01526_max_untracked_memory",
    "01527_dist_sharding_key_dictGet_reload",
    "01528_play",
    "01545_url_file_format_settings",
    "01553_datetime64_comparison",
    "01555_system_distribution_queue_mask",
    "01558_ttest_scipy",
    "01561_mann_whitney_scipy",
    "01582_distinct_optimization",
    "01591_window_functions",
    "01594_too_low_memory_limits",
    "01599_multiline_input_and_singleline_comments",  # expect-test
    "01601_custom_tld",
    "01606_git_import",
    "01610_client_spawn_editor",  # expect-test
    "01654_test_writer_block_sequence",  # No module named 'pandas'
    "01658_read_file_to_stringcolumn",
    "01666_merge_tree_max_query_limit",
    "01674_unicode_asan",
    "01676_clickhouse_client_autocomplete",  # expect-test (partially)
    "01676_long_clickhouse_client_autocomplete",
    "01683_text_log_deadlock",  # secure tcp
    "01684_ssd_cache_dictionary_simple_key",
    "01685_ssd_cache_dictionary_complex_key",
    "01737_clickhouse_server_wait_server_pool_long",
    "01746_executable_pool_dictionary",
    "01747_executable_pool_dictionary_implicit_key.sql",
    "01747_join_view_filter_dictionary",
    "01748_dictionary_table_dot",
    "01754_cluster_all_replicas_shard_num",
    "01759_optimize_skip_unused_shards_zero_shards",
    "01763_max_distributed_depth",  # BROKEN
    "01780_clickhouse_dictionary_source_loop",
    "01801_s3_cluster",
    "01802_test_postgresql_protocol_with_row_policy",
    "01804_dictionary_decimal256_type",  # hardcoded path
    "01848_http_insert_segfault",
    "01875_ssd_cache_dictionary_decimal256_type",
    "01880_remote_ipv6",
    "01889_check_row_policy_defined_using_user_function",
    "01889_clickhouse_client_config_format",
    "01903_ssd_cache_dictionary_array_type",
]


def check_result(result, error, return_code, reference, replace_map):
    if replace_map:
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


def run_client(use_antlr, bin_prefix, port, database, query, reference, replace_map=None):
    # We can't use `text=True` since some tests may return binary data
    cmd = [bin_prefix + '-client', '--port', str(port), '-d', database, '-m', '-n', '--testmode']
    if use_antlr:
        cmd.append('--use_antlr_parser=1')
    client = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, error = client.communicate(query.encode('utf-8'))
    assert client.returncode is not None, "Client should exit after processing all queries"

    check_result(result, error, client.returncode, reference, replace_map)


def run_shell(use_antlr, bin_prefix, server, database, path, reference, replace_map=None):
    env = {
        'CLICKHOUSE_BINARY': bin_prefix,
        'CLICKHOUSE_DATABASE': database,
        'CLICKHOUSE_PORT_TCP': str(server.tcp_port),
        'CLICKHOUSE_PORT_TCP_SECURE': str(server.tcps_port),
        'CLICKHOUSE_PORT_TCP_WITH_PROXY': str(server.proxy_port),
        'CLICKHOUSE_PORT_HTTP': str(server.http_port),
        'CLICKHOUSE_PORT_INTERSERVER': str(server.inter_port),
        'CLICKHOUSE_PORT_POSTGRESQL': str(server.postgresql_port),
        'CLICKHOUSE_TMP': server.tmp_dir,
        'CLICKHOUSE_CONFIG_CLIENT': server.client_config,
        'PROTOC_BINARY': os.path.abspath(os.path.join(os.path.dirname(bin_prefix), '..', 'contrib', 'protobuf', 'protoc')),  # FIXME: adhoc solution
    }
    if use_antlr:
        env['CLICKHOUSE_CLIENT_OPT'] = '--use_antlr_parser=1'
    shell = subprocess.Popen([path], env=env, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, error = shell.communicate()
    assert shell.returncode is not None, "Script should exit after executing all commands"

    check_result(result, error, shell.returncode, reference, replace_map)


def random_str(length=10):
    alphabet = string.ascii_lowercase + string.digits
    random.seed(os.urandom(8))
    return ''.join(random.choice(alphabet) for _ in range(length))


def test_sql_query(use_antlr, bin_prefix, sql_query, standalone_server):
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
    run_client(use_antlr, bin_prefix, tcp_port, 'default', 'CREATE DATABASE {random};'.format(random=random_name), b'')

    run_client(use_antlr, bin_prefix, tcp_port, random_name, query, reference, {random_name: 'default'})

    query = "SELECT 'SHOW ORPHANED TABLES'; SELECT name FROM system.tables WHERE database != 'system' ORDER BY (database, name);"
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'SHOW ORPHANED TABLES\n')

    query = 'DROP DATABASE {random};'.format(random=random_name)
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'')

    query = "SELECT 'SHOW ORPHANED DATABASES'; SHOW DATABASES;"
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'SHOW ORPHANED DATABASES\ndefault\nsystem\n')


def test_shell_query(use_antlr, bin_prefix, shell_query, standalone_server):
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
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'')

    run_shell(use_antlr, bin_prefix, standalone_server, random_name, shell_path, reference, {random_name: 'default'})

    query = "SELECT 'SHOW ORPHANED TABLES'; SELECT name FROM system.tables WHERE database != 'system' ORDER BY (database, name);"
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'SHOW ORPHANED TABLES\n')

    query = 'DROP DATABASE {random};'.format(random=random_name)
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'')

    query = "SELECT 'SHOW ORPHANED DATABASES'; SHOW DATABASES;"
    run_client(use_antlr, bin_prefix, tcp_port, 'default', query, b'SHOW ORPHANED DATABASES\ndefault\nsystem\n')
