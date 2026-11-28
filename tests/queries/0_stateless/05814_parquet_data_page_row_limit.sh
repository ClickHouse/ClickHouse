#!/usr/bin/env bash
# Tags: no-fasttest
# A sorted column compresses into one data page unless the writer limits the rows per page;
# with the limit, the page index prunes the pages a filter cannot match.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for limit in 10000 0; do
    FILE="$CLICKHOUSE_TMP/05814_parquet_data_page_row_limit_$limit.parquet"
    $CLICKHOUSE_LOCAL -q "
        INSERT INTO FUNCTION file('$FILE', Parquet) SELECT number AS n FROM numbers(100000)
        SETTINGS output_format_parquet_data_page_row_limit = $limit, engine_file_truncate_on_insert = 1"
    echo "limit $limit"
    QUERY="SELECT count() FROM file('$FILE', Parquet) WHERE n BETWEEN 20000 AND 20999 SETTINGS input_format_parquet_page_filter_push_down = 1"
    $CLICKHOUSE_LOCAL -q "$QUERY"
    # The pages may be pruned by different threads, so the profile event can be reported in several increments.
    $CLICKHOUSE_LOCAL --print-profile-events -q "$QUERY" 2>&1 | grep 'ParquetPrunedPages' | sed 's/^.*] //' \
        | awk '{ sum += $2 } END { print "pruned pages: " (sum > 0 ? "yes" : "no") }'
    rm -f "$FILE"
done

# A nested column counts rows, not values: 100 values per row give 10 pages of about 10000 rows,
# where counting values would give 910.
FILE="$CLICKHOUSE_TMP/05814_parquet_data_page_row_limit_nested.parquet"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$FILE', Parquet) SELECT range(100) AS a FROM numbers(100000)
    SETTINGS output_format_parquet_data_page_row_limit = 10000, engine_file_truncate_on_insert = 1"
$CLICKHOUSE_LOCAL --print-profile-events -q "SELECT sum(length(a)) FROM file('$FILE', Parquet) FORMAT Null" 2>&1 \
    | grep 'ParquetReadPages' | sed 's/^.*] //' | awk '{ pages += $2 } END { print "nested pages: " pages }'
rm -f "$FILE"
