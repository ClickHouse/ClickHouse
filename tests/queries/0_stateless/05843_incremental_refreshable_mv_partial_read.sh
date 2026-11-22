#!/usr/bin/env bash
# Tags: atomic-database

# An incremental refresh must not finish successfully with a partial result: a read limit with the overflow mode `break`
# stops the read, while the cursor of a `MergeTree` read advances to the end of the snapshot, so the rows that were
# not read would be skipped by all the next refreshes. The settings of the definer are checked before every refresh,
# and such a refresh is rejected: it appends nothing, and the rows are appended once the limit is removed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}_$(random_str 8)"

$CLICKHOUSE_CLIENT -q "
    CREATE USER ${user};
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${user};

    CREATE TABLE src (k UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    CREATE TABLE dst (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src SELECT number FROM numbers(100000);

    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO dst EMPTY
        DEFINER = ${user} SQL SECURITY DEFINER
        AS SELECT k FROM src;

    ALTER USER ${user} SETTINGS max_rows_to_read = 100, read_overflow_mode = 'break', max_threads = 1, max_block_size = 1;
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv; -- { serverError REFRESH_FAILED }
    SELECT 'rejected', count() FROM dst;
    SELECT exception LIKE '%read_overflow_mode%' FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv';

    ALTER USER ${user} SETTINGS max_rows_to_read = 0;
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT 'without the limit', count(), uniqExact(k) FROM dst;

    INSERT INTO src VALUES (100000);
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT 'next refresh', count(), uniqExact(k), max(k) FROM dst;

    DROP TABLE mv;
    DROP TABLE dst;
    DROP TABLE src;
    DROP USER ${user};
"
