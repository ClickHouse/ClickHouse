#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Boundary cases that previously wrapped around an unsigned integer (which UBSan now reports).

echo "-- formatReadableTimeDelta with a number of sub-second units above UInt64"
$CLICKHOUSE_LOCAL -q "SELECT formatReadableTimeDelta(20000000000, 'nanoseconds')"
$CLICKHOUSE_LOCAL -q "SELECT formatReadableTimeDelta(18446744073.9, 'nanoseconds')"
$CLICKHOUSE_LOCAL -q "SELECT formatReadableTimeDelta(1.5, 'nanoseconds')"

echo "-- the maximum numbers of download and parsing threads"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$CUR_DIR/data_parquet/delta_length_byte_array_split_page.parquet', Parquet) SETTINGS max_threads = 1, max_download_threads = 18446744073709551615"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$CUR_DIR/data_parquet/delta_length_byte_array_split_page.parquet', Parquet) SETTINGS max_threads = 1, max_parsing_threads = 18446744073709551615"
$CLICKHOUSE_LOCAL -q "SELECT * FROM format(CSV, 'x UInt8', '1\n2') SETTINGS max_threads = 1, max_parsing_threads = 18446744073709551615, max_memory_usage = 1000000000"

echo "-- reading in order from a table with the maximum index granularity"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_granularity (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 18446744073709551615;
    INSERT INTO t_granularity VALUES (1);
    SELECT n FROM t_granularity ORDER BY n SETTINGS max_threads = 1, optimize_read_in_order = 1;"

echo "-- text index with the maximum dictionary and posting list block sizes"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_text_dictionary (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', dictionary_block_size = 18446744073709551615))
    ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_text_dictionary VALUES ('foo');
    SELECT count() FROM t_text_dictionary WHERE hasToken(s, 'foo');"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_text_postings (s String, INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_block_size = 18446744073709551615, posting_list_codec = 'bitpacking'))
    ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_text_postings SELECT 'foo' FROM numbers(200);
    SELECT count() FROM t_text_postings WHERE hasToken(s, 'foo');"

echo "-- tokenbf_v1 with the maximum seed"
$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_bloom_seed (s String, INDEX idx s TYPE tokenbf_v1(16, 3, 18446744073709551615) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_bloom_seed VALUES ('foo');
    SELECT count() FROM t_bloom_seed WHERE hasToken(s, 'foo');"

echo "-- proportionsZTest with a sum of trials above UInt64"
$CLICKHOUSE_LOCAL -q "SELECT isFinite(tupleElement(proportionsZTest(toUInt64(1), toUInt64(1), toUInt64(9223372036854775808), toUInt64(9223372036854775809), 0.95, 'unpooled'), 1))"
$CLICKHOUSE_LOCAL -q "SELECT isFinite(tupleElement(proportionsZTest(toUInt64(9223372036854775808), toUInt64(9223372036854775808), toUInt64(9223372036854775808), toUInt64(9223372036854775809), 0.95, 'pooled'), 1))"

echo "-- generateRandom with the maximum numbers of threads and rows in a block"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM (SELECT * FROM generateRandom('x UInt8') LIMIT 1) SETTINGS max_threads = 18446744073709551615, max_block_size = 18446744073709551615"
