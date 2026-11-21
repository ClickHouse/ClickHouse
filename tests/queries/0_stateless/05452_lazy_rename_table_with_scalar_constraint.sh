#!/usr/bin/env bash
# Tags: no-replicated-database
# Regression: `RENAME TABLE` of an unloaded lazy table (`lazy_load_tables = 1`) must not load it while
# `DatabaseAtomic::renameTable` holds the database mutex, neither to ask it whether it can be renamed nor
# to create the symlink to its data. Loading the table below executes the scalar subquery of its
# `CONSTRAINT`, which resolves another table of the same database and needs that mutex again, so the
# rename used to deadlock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Keep this file's DDL out of the server-side AST fuzzer (see 04908_constraint_subquery_loading_dependency.sh).
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --ast_fuzzer_any_query 0"

LAZY_DB="${CLICKHOUSE_DATABASE}_lazy_constraint"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${LAZY_DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${LAZY_DB}.src (id UInt64) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${LAZY_DB}.dst (x UInt64, CONSTRAINT c CHECK x < (SELECT max(id) + 1000 FROM ${LAZY_DB}.src)) ENGINE = MergeTree ORDER BY tuple()"

# After re-attach the tables are unloaded proxies.
$CLICKHOUSE_CLIENT -q "DETACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "ATTACH DATABASE ${LAZY_DB}"
$CLICKHOUSE_CLIENT -q "SELECT name, engine FROM system.tables WHERE database = '${LAZY_DB}' ORDER BY name"

$CLICKHOUSE_CLIENT -q "RENAME TABLE ${LAZY_DB}.dst TO ${LAZY_DB}.dst_new"

$CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = '${LAZY_DB}' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ${LAZY_DB}.dst_new"

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${LAZY_DB}"
