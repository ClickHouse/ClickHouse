#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: SQL named collections are local to a server, so the replicas of the test database cannot resolve the collection when they execute the replicated `CREATE TABLE`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `elasticsearch` table function resolves its arguments through named collections, so a nested
# `elasticsearch(nc)` target of a `Remote` table must hold the collection while the table exists.

NC="es_nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
SET enable_elasticsearch = 1;
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:9200', index = 'i';
CREATE TABLE t_engine (doc JSON) ENGINE = Remote('127.0.0.1', elasticsearch(${NC}));
CREATE TABLE t_as (doc JSON) AS remote('127.0.0.1', elasticsearch(${NC}));
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_engine;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE t_as;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
