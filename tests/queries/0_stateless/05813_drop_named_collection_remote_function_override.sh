#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: SQL named collections are local to a server, so the replicas of the test database cannot resolve the collection when they execute the replicated `CREATE TABLE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `remote(nc, rand())` resolves through the collection `nc`: a non-`equals` function after the collection
# name is accepted as an override and skipped. Such a definition must hold the collection both when it is
# the persisted target of a `Remote` table and when a `Remote` table is loaded lazily.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_1"

${CLICKHOUSE_CLIENT} -q "CREATE NAMED COLLECTION ${NC} AS host = '127.0.0.1', database = 'system', table = 'one'"

echo "--- ENGINE = Remote(..., remote(nc, rand())) ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE t_nested (dummy UInt8) ENGINE = Remote('127.0.0.1', remote(${NC}, rand()));
SELECT dummy FROM t_nested;
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_nested;
"

echo "--- ENGINE = Remote(nc, rand()) loaded lazily ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DATABASE ${DB} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE ${DB}.t (dummy UInt8) ENGINE = Remote(${NC}, rand());
DETACH DATABASE ${DB};
ATTACH DATABASE ${DB};
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
SELECT dummy FROM ${DB}.t;
DROP DATABASE ${DB} SYNC;
"

echo "--- the collection can be dropped once the tables are gone ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
