#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -eu

# `query_cache_use_only_when_data_was_not_changed` must fail closed for dependencies that the query
# text does not show: an `ALIAS` column expanded into a `dictGet` by the analyzer, a niladic function
# spelled without parentheses, and filters applied by a settings `profile` in a subquery.

qc="use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1"
profile="profile_05412_${CLICKHOUSE_DATABASE}"

# An `ALIAS` column reading a dictionary. The `DIRECT` layout reads the source on every lookup.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE dict_source (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dict_source VALUES (1, 'old');
    CREATE DICTIONARY dict (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'dict_source')) LAYOUT(DIRECT());
    CREATE TABLE t_alias (k UInt64, y String ALIAS dictGet('${CLICKHOUSE_DATABASE}.dict', 'v', k)) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_alias VALUES (1);
"
$CLICKHOUSE_CLIENT -q "SELECT y FROM t_alias SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE dict_source; INSERT INTO dict_source VALUES (1, 'new');"
$CLICKHOUSE_CLIENT -q "SELECT y FROM t_alias SETTINGS ${qc}"

# A niladic function spelled as a bare identifier: the result must not be stored.
$CLICKHOUSE_CLIENT -q "SELECT CURRENT_TIMESTAMP, '05412 niladic ${CLICKHOUSE_DATABASE}' FORMAT Null SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "SELECT 'niladic entries', count() FROM system.query_cache WHERE query LIKE '%05412 niladic ${CLICKHOUSE_DATABASE}%'"

# A subquery that applies a settings profile carrying `additional_table_filters` reading another table.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1), (2);
    CREATE TABLE aux (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO aux VALUES (1);
    DROP SETTINGS PROFILE IF EXISTS ${profile};
    CREATE SETTINGS PROFILE ${profile} SETTINGS additional_table_filters = '{\\'${CLICKHOUSE_DATABASE}.src\\': \\'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.aux)\\'}';
"
query="SELECT sum(x) FROM (SELECT x FROM ${CLICKHOUSE_DATABASE}.src SETTINGS profile = '${profile}') SETTINGS ${qc}"
$CLICKHOUSE_CLIENT -q "${query}"
$CLICKHOUSE_CLIENT -q "INSERT INTO aux VALUES (2)"
$CLICKHOUSE_CLIENT -q "${query}"

$CLICKHOUSE_CLIENT -q "
    DROP SETTINGS PROFILE ${profile};
    DROP TABLE t_alias;
    DROP DICTIONARY dict;
    DROP TABLE dict_source;
    DROP TABLE src;
    DROP TABLE aux;
"
