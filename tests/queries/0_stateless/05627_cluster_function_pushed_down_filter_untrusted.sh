#!/usr/bin/env bash
# A client can send `cluster_table_function_pushed_down_filter` with a query that reaches a `fileCluster` replica
# through `remote` and a view. The replica rejects what its initiator never sends: a constant without a value,
# and a set that is not a set of values, such as a table it would otherwise look up for a reader without access to it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

file_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}.parquet"
view_name="filecluster_untrusted_filter_view"
hidden_table="filecluster_untrusted_filter_hidden"
reader="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cluster_file="fileCluster('test_cluster_two_shards_localhost', '${file_name}', 'Parquet', 'id UInt64')"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${view_name}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${hidden_table}"
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${reader}"
    rm -f "${USER_FILES_PATH}/${file_name}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${file_name}', 'Parquet', 'id UInt64') SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW ${view_name} AS SELECT id FROM ${cluster_file}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${hidden_table} (id UInt64) ENGINE = MergeTree ORDER BY id"

# The reader can query the view, but has no grant on the hidden table.
${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${reader}"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${reader} IDENTIFIED WITH PLAINTEXT_PASSWORD BY 'hello'"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${view_name} TO ${reader}"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON FILE, CREATE TEMPORARY TABLE ON *.* TO ${reader}"
${CLICKHOUSE_CLIENT} --query "GRANT REMOTE ON *.* TO ${reader}"

# Sends the filter to a replica that runs the query of the view as the given user, and prints the error.
send_filter()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT sum(id) FROM remote('127.0.0.2', currentDatabase(), ${view_name}, '$2', '$3')
        SETTINGS cluster_table_function_pushed_down_filter = '$1', serialize_query_plan = 0, enable_parallel_replicas = 0" 2>&1 \
        | grep -o -m1 -E 'has no value|has no record|but its child has type|cannot have the node|only a set of values is allowed|is not a StorageSet|UNKNOWN_TABLE|ACCESS_DENIED' || true
}

# `base64Encode` is not in every build, so the bytes are encoded by `base64`.
hex_filter()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT unhex('$1') FORMAT RawBLOB" | base64 -w0
}

# One `COLUMN` node `f` of type `UInt8` without a value, as the only output, and no sets.
send_filter 'AQEBZgEAAAABAAA=' default ''

# `in(id, s)` with a `Set` constant `s`, and no set records.
send_filter 'AwACaWQEAAABAXMhAAMBAQIDBAUGBwgJCgsMDQ4PEAQBZgECAAEAAmluAQABAgA=' default ''

# Two `Set` constants `s` and `t`, with `s` as the only output, and a record of one row of `UInt64` for `s` only.
set_hash="0102030405060708090a0b0c0d0e0f10"
other_set_hash="1112131415161718191a1b1c1d1e1f20"
send_filter "$(hex_filter "0201017321000101${set_hash}01017421000101${other_set_hash}00010001${set_hash}020101040100000000000000")" default ''

# `equals(a, t)`, where `a` is an alias of type `Tuple(UInt64)` of a constant `n` of type `UInt64`, and `t` is a tuple constant.
send_filter 'BAEBbgQAAwEAAAAAAAAAAgFhHwEEAQAAAQF0HwEEAAMBAAAAAAAAAAQBZgECAQIABmVxdWFscwABAwA=' default ''

# `arrayJoin` of a constant `[1]` of type `Array(UInt8)`, as the only output. It is well-formed, but no initiator sends it.
send_filter "$(hex_filter "020101631e010003010103016a0101000000010100")" default ''

# One `COLUMN` node `s` of type `Set`, as the only output, and the record of its set, which starts with its kind:
# 1 is a `StorageSet` named by the string that follows, 3 is a plan of a subquery.
set_record_prefix="0101017321000101${set_hash}00010001${set_hash}"

storage_set_filter()
{
    ${CLICKHOUSE_CLIENT} --query "
        WITH concat(currentDatabase(), '.', '$1') AS table_name
        SELECT concat(unhex('${set_record_prefix}01'), char(length(table_name)), table_name) FORMAT RawBLOB" | base64 -w0
}

# The hidden table and a missing one are rejected the same way, before either is looked up.
send_filter "$(storage_set_filter "${hidden_table}")" "${reader}" 'hello'
send_filter "$(storage_set_filter "filecluster_untrusted_filter_missing")" "${reader}" 'hello'
send_filter "$(hex_filter "${set_record_prefix}03")" default ''

# The server is still up.
${CLICKHOUSE_CLIENT} --query "SELECT 1"
