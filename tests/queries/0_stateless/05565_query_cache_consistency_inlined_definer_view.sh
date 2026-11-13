#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: a `SQL SECURITY DEFINER` view is not inlined when the query ships to parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

# With `analyzer_inline_views = 1`, the body of a `SQL SECURITY DEFINER` view is inlined into the query and
# planned under the context of the definer. A subquery cached with `query_cache_use_only_when_data_was_not_changed`
# must not hash the tables of the inlined body under the context of the caller only: the
# `additional_table_filters` of the definer read another table, whose changes must not be hidden by the cache.

caller="caller_05565_${CLICKHOUSE_DATABASE}"
definer="definer_05565_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${caller};
    DROP USER IF EXISTS ${definer};
    CREATE USER ${caller};
    CREATE USER ${definer};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${definer};

    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE aux (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    INSERT INTO aux VALUES (1);

    CREATE VIEW v DEFINER = ${definer} SQL SECURITY DEFINER AS SELECT x FROM src;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v TO ${caller};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.src TO ${caller};
"

# A user profile takes the map as a string.
$CLICKHOUSE_CLIENT -q "ALTER USER ${definer} SETTINGS additional_table_filters = '{\\'src\\': \\'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.aux)\\', \\'${CLICKHOUSE_DATABASE}.src\\': \\'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.aux)\\'}'"

query="
    SELECT * FROM
    (
        SELECT sum(x) AS s FROM ${CLICKHOUSE_DATABASE}.v
        SETTINGS use_query_cache = 1,
            query_cache_use_only_when_data_was_not_changed = 1,
            query_cache_min_query_runs = 0,
            query_cache_min_query_duration = 0
    )
    SETTINGS use_query_cache = 0, query_cache_for_subqueries = 0, analyzer_inline_views = 1"

$CLICKHOUSE_CLIENT --user "${caller}" -q "$query"

$CLICKHOUSE_CLIENT -q "INSERT INTO aux VALUES (2)"

# The definer filter now passes both rows of `src`.
$CLICKHOUSE_CLIENT --user "${caller}" -q "$query"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE v;
    DROP TABLE aux;
    DROP TABLE src;
    DROP USER ${caller};
    DROP USER ${definer};
"
