#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Parquet is not supported in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A substring filter from PREWHERE is checked on plain-encoded Parquet string pages with one search over
# a batch of values together with their 4-byte length prefixes. The result must be the same with the
# setting enabled and disabled. The second file has length prefixes that contain the needles: a needle shorter than
# 4 bytes or with a zero byte is checked value by value, because its occurrence could start in a prefix.
# The third file has 100000 short values and one of 16 MiB, whose length prefix contains the first byte of a 4-byte needle.

FILE_A="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan.parquet"
FILE_B="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan_prefixes.parquet"
FILE_C="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan_large.parquet"
FILE_D="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan_dense.parquet"
FILE_E="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan_one_eighth.parquet"

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE_A', Parquet, 'k UInt64, s String')
SELECT number, multiIf(number % 3 = 0, '', cityHash64(number) % 100 < 2, concat('xx needle ', toString(number)), concat('plain value ', toString(number)))
FROM numbers(200000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 0, output_format_parquet_row_group_size = 100000;

INSERT INTO FUNCTION file('$FILE_B', Parquet, 'k UInt64, s String')
SELECT number, multiIf(
    number % 6 = 0, repeat('a', 103),
    number % 6 = 1, repeat('a', 111),
    number % 6 = 2, if(number < 60, repeat('x', 28527), 'q'),
    number % 6 = 3, concat('ab', '\x0A\0\0\0ab', 'zz'),
    number % 6 = 4, 'zoo',
    'zz')
FROM numbers(6000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 0, output_format_parquet_row_group_size = 100000,
    output_format_parquet_data_page_size = 4096;

INSERT INTO FUNCTION file('$FILE_C', Parquet, 'k UInt64, s String')
SELECT number AS k, if(number = 9001, repeat(repeat('x', 1024), (number = 9001) * 16384), 'zz') AS s
FROM numbers(100000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 0, output_format_parquet_row_group_size = 100000,
    output_format_parquet_data_page_size = 4096, output_format_parquet_write_page_index = 1;

INSERT INTO FUNCTION file('$FILE_D', Parquet, 's String')
SELECT if(number % 128 < 17, 'abc needle', 'zz') FROM numbers(40000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 0, output_format_parquet_row_group_size = 100000;

INSERT INTO FUNCTION file('$FILE_E', Parquet, 's String')
SELECT if(number % 128 < 16, 'abc needle', 'zz') FROM numbers(40000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 0, output_format_parquet_row_group_size = 100000;
"

for cond in "s LIKE '%needle%'" "s LIKE '%needle%' AND s LIKE '%1%'" "s LIKE '%needle%' AND endsWith(s, '7')" "s LIKE '%value%'"; do
    for enable in 0 1; do
        echo "A | $cond | $enable | $($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(s)), sum(k) FROM file('$FILE_A', Parquet) PREWHERE $cond SETTINGS apply_string_filters_during_scan = $enable, use_columns_cache = 0")"
    done
done

for cond in "position(s, 'abzz') > 0" "position(s, 'g') > 0" "position(s, 'o') > 0" "position(s, 'oo') > 0" "position(s, '\x0A\0\0\0ab') > 0" "s LIKE '%ab%'" "s LIKE '%oo%' AND s LIKE '%z%'"; do
    for enable in 0 1; do
        echo "B | $cond | $enable | $($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(s)), sum(k) FROM file('$FILE_B', Parquet) PREWHERE $cond SETTINGS apply_string_filters_during_scan = $enable, use_columns_cache = 0")"
    done
done

for enable in 0 1; do
    echo "C | position(s, '\x01xxx') > 0 | $enable | $($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(s)), sum(k) FROM file('$FILE_C', Parquet) PREWHERE position(s, '\x01xxx') > 0 SETTINGS apply_string_filters_during_scan = $enable, use_columns_cache = 0")"
done

# Values decided by the bulk scan do not count towards the minimum size of skipped values, so the filter stays applied to
# every value; checked one by one, these short values turn it off after 8192 checks.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_A', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_applied' FORMAT Null"
# A found value is also checked against the other conditions: the values with 'needle' but without '1' are replaced too.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_A', Parquet) PREWHERE s LIKE '%needle%' AND s LIKE '%1%' SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_other_conditions' FORMAT Null"
# The 103-byte values have the length prefix 'g' and the values 'zz' the prefix '\x02\0\0\0', but no value contains these
# needles, so every value is replaced.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_B', Parquet) PREWHERE position(s, 'g') > 0 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_prefixes' FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_B', Parquet) PREWHERE position(s, '\x02\0\0\0zz') > 0 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_prefixes_zero' FORMAT Null"
# The filter stays applied to every value, and the re-check rejects the 16 MiB value, where the scan finds '\x01xxx' in the length prefix.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_C', Parquet) PREWHERE position(s, '\x01xxx') > 0 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_large_prefix' FORMAT Null"
# With a page index, the second PREWHERE step decodes the rows with an even k one row range at a time, so it checks them one by one.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_C', Parquet) PREWHERE k % 2 = 0 AND position(s, '\x01xxx') > 0 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, input_format_parquet_use_offset_index = 1, enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0, log_comment = '05407_ranges_after_prewhere_step' FORMAT Null"
# 17 of the first 128 values contain the needle, more than 1/8, so the rest of the page is checked one by one and these short
# values turn the filter off after 8192 checks. With 16 of every 128 values, every value is bulk-scanned and 34992 are replaced.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_D', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, input_format_parquet_max_block_size = 65536, log_comment = '05407_dense_matches' FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_E', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_one_eighth' FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT log_comment, sum(ProfileEvents['StringValueFilterValuesReplaced'])
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05407_plain_bulk_scan_%'
GROUP BY log_comment
ORDER BY log_comment;

SELECT log_comment, sum(ProfileEvents['StringValueFilterValuesChecked']) BETWEEN 8192 AND 40000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05407_ranges_after_prewhere_step'
GROUP BY log_comment;

SELECT log_comment, sum(ProfileEvents['StringValueFilterValuesChecked']) BETWEEN 8192 AND 16383
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05407_dense_matches'
GROUP BY log_comment;
"
