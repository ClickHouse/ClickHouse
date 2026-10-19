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

FILE_A="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan.parquet"
FILE_B="${CLICKHOUSE_DATABASE}/t_plain_bulk_scan_prefixes.parquet"

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

# A column that can be written to the columns cache is decoded without the filter, so that the cache gets its values.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_A', Parquet) PREWHERE s LIKE '%needle%' SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, log_comment = '05407_plain_bulk_scan_applied' FORMAT Null"
# The 103-byte values have the length prefix 'g' and the values 'zz' the prefix '\x02\0\0\0', but no value contains these
# needles, so every value is replaced.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_B', Parquet) PREWHERE position(s, 'g') > 0 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_prefixes' FORMAT Null"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('$FILE_B', Parquet) PREWHERE position(s, '\x02\0\0\0zz') > 0 SETTINGS apply_string_filters_during_scan = 1, use_columns_cache = 0, max_threads = 1, log_comment = '05407_plain_bulk_scan_prefixes_zero' FORMAT Null"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT log_comment, if(log_comment = '05407_plain_bulk_scan_applied', toUInt64(sum(ProfileEvents['StringValueFilterValuesReplaced']) > 0), sum(ProfileEvents['StringValueFilterValuesReplaced']))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05407_plain_bulk_scan_%'
GROUP BY log_comment
ORDER BY log_comment;
"
