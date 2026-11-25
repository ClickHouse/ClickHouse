#!/usr/bin/env bash
# Tags: atomic-database, no-replicated-database
# Two checks of the definition of an incremental refresh:
# - A `profile` in the `SETTINGS` clause of the query is resolved to the settings of the profile, so it cannot
#   bring the overflow mode `break` past the check (a partial result would lose the rows of the source).
# - A source in a database with `lazy_load_tables` is a stand-in until something reads it. The check of the
#   capabilities of the source must look through it instead of refusing a `MergeTree` table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROFILE="${CLICKHOUSE_DATABASE}_break_profile"
LAZY_DATABASE="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE src (k UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src SELECT number FROM numbers(10);

    DROP SETTINGS PROFILE IF EXISTS ${PROFILE};
    CREATE SETTINGS PROFILE ${PROFILE} SETTINGS read_overflow_mode = 'break';
"

echo '-- The overflow mode break from a profile'
# Over HTTP, because the native client applies the `SETTINGS` clause of a query to itself and does not know the profile.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT k FROM src
        SETTINGS max_rows_to_read = 1, read_overflow_mode = 'throw', profile = '${PROFILE}'" | grep -o -F -m1 "'break' is not supported in the query of an incremental refresh"
# The settings are applied in the order of the clause: the mode is `throw` again after the profile.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT k FROM src
        SETTINGS profile = '${PROFILE}', read_overflow_mode = 'throw'"
${CLICKHOUSE_CLIENT} --query "
    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT count() FROM tgt;
    DROP TABLE mv;
    TRUNCATE TABLE tgt;
    DROP SETTINGS PROFILE ${PROFILE};
"

echo '-- A source that is not loaded yet'
${CLICKHOUSE_CLIENT} --query "
    DROP DATABASE IF EXISTS ${LAZY_DATABASE};
    CREATE DATABASE ${LAZY_DATABASE} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${LAZY_DATABASE}.src (k UInt64) ENGINE = MergeTree ORDER BY k
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
    INSERT INTO ${LAZY_DATABASE}.src SELECT number FROM numbers(10);
    CREATE MATERIALIZED VIEW mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO tgt EMPTY
        AS SELECT k FROM ${LAZY_DATABASE}.src;

    DETACH DATABASE ${LAZY_DATABASE};
    ATTACH DATABASE ${LAZY_DATABASE};
    SELECT engine FROM system.tables WHERE database = '${LAZY_DATABASE}' AND name = 'src';

    SYSTEM REFRESH VIEW mv;
    SYSTEM WAIT VIEW mv;
    SELECT count() FROM tgt;

    DROP TABLE mv;
    DROP DATABASE ${LAZY_DATABASE};
    DROP TABLE tgt;
    DROP TABLE src;
"
