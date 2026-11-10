#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: a named collection is server-global, not database-scoped

# The storage opens no connection at creation time, so every check below runs without an Elasticsearch server.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NC="nc_05443_${CLICKHOUSE_DATABASE}"

# The tables are dropped before the collection they use.
trap '${CLICKHOUSE_CLIENT} -q "SET ast_fuzzer_any_query = 0; DROP TABLE IF EXISTS es_query_persisted_collection; DROP TABLE IF EXISTS es_query_collection_function; DROP TABLE IF EXISTS es_query_collection_key" && ${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION IF EXISTS ${NC}"' EXIT

# ast_fuzzer_any_query = 0: the AST fuzzer must not replay these DDLs as clones that outlive the collection.
${CLICKHOUSE_CLIENT} -q "$(cat <<EOF
SET enable_elasticsearch = 1;
SET ast_fuzzer_any_query = 0;

SELECT * FROM elasticsearch('http://localhost:1', 'idx', query = '{}'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', query()); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', query('{}', '{}')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', query(1)); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', query('{}'), query('{}')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch('http://localhost:1', 'idx', query('{}'), 'u'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS es_query_engine;
CREATE TABLE es_query_engine (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', query = '{}'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE es_query_engine (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', query('not json')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE es_query_engine (doc JSON) ENGINE = Elasticsearch('http://localhost:1', 'idx', 'u', 'pw', query('{"term": {"level": "error"}}'));
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name = 'es_query_engine';
DROP TABLE es_query_engine;

DROP NAMED COLLECTION IF EXISTS ${NC};
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:1', index = 'idx', query = 'not json';

-- The evaluated query(...) is saved in the definition, so a reload does not evaluate it again.
SET log_comment = '{"term": {"id": 2}}';
DROP TABLE IF EXISTS es_query_persisted_function;
DROP TABLE IF EXISTS es_query_persisted_engine;
DROP TABLE IF EXISTS es_query_persisted_collection;
CREATE TABLE es_query_persisted_function AS elasticsearch('http://localhost:1', 'idx', query(getSetting('log_comment')));
CREATE TABLE es_query_persisted_engine ENGINE = Elasticsearch('http://localhost:1', 'idx', query(getSetting('log_comment')));
CREATE TABLE es_query_persisted_collection ENGINE = Elasticsearch(${NC}, query(getSetting('log_comment')));

SET log_comment = '';
DETACH TABLE es_query_persisted_function;
ATTACH TABLE es_query_persisted_function;
DETACH TABLE es_query_persisted_engine;
ATTACH TABLE es_query_persisted_engine;
DETACH TABLE es_query_persisted_collection;
ATTACH TABLE es_query_persisted_collection;

SHOW CREATE TABLE es_query_persisted_function FORMAT TSVRaw;
SHOW CREATE TABLE es_query_persisted_engine FORMAT TSVRaw;
SHOW CREATE TABLE es_query_persisted_collection FORMAT TSVRaw;
DROP TABLE es_query_persisted_function;
DROP TABLE es_query_persisted_engine;
DROP TABLE es_query_persisted_collection;

SELECT * FROM elasticsearch(${NC}); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch(${NC}, query('[1, 2]')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch(${NC}, query('{}'), query = '{}'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS es_query_collection_function;
DROP TABLE IF EXISTS es_query_collection_key;
CREATE TABLE es_query_collection_function AS elasticsearch(${NC}, query('{}'));
CREATE TABLE es_query_collection_key AS elasticsearch(${NC}, query = '{}');
DROP TABLE es_query_collection_function;
DROP TABLE es_query_collection_key;

-- A positional argument after query(...) is rejected, but the query is still logged, and the secret must be masked.
SELECT * FROM elasticsearch('http://localhost:1', 'idx', 'u', query('{}'), 'secret_value_do_not_log'); -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;
SELECT query FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND query LIKE '%elasticsearch(%''u''%'
  AND query NOT LIKE '%system.query_log%'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
ORDER BY event_time_microseconds
FORMAT TSVRaw;
EOF
)"
