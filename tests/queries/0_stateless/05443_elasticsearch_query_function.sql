-- The storage opens no connection at creation time, so every check below runs without an Elasticsearch server.
SET enable_elasticsearch = 1;

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

DROP TABLE IF EXISTS es_query_persisted;
CREATE TABLE es_query_persisted AS elasticsearch('http://localhost:1', 'idx', query(concat('{', '}')));
SHOW CREATE TABLE es_query_persisted FORMAT TSVRaw;
DROP TABLE es_query_persisted;

DROP NAMED COLLECTION IF EXISTS nc_05443_elasticsearch;
CREATE NAMED COLLECTION nc_05443_elasticsearch AS url = 'http://localhost:1', index = 'idx', query = 'not json';

SELECT * FROM elasticsearch(nc_05443_elasticsearch); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch(nc_05443_elasticsearch, query('[1, 2]')); -- { serverError BAD_ARGUMENTS }
SELECT * FROM elasticsearch(nc_05443_elasticsearch, query('{}'), query = '{}'); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS es_query_collection_function;
DROP TABLE IF EXISTS es_query_collection_key;
CREATE TABLE es_query_collection_function AS elasticsearch(nc_05443_elasticsearch, query('{}'));
CREATE TABLE es_query_collection_key AS elasticsearch(nc_05443_elasticsearch, query = '{}');
DROP TABLE es_query_collection_function;
DROP TABLE es_query_collection_key;

DROP NAMED COLLECTION nc_05443_elasticsearch;

-- A positional argument after `query(...)` is rejected, but the query is still logged, and the secret must be masked.
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
