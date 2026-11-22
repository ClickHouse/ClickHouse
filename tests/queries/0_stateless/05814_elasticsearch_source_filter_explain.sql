-- The read step opens no connection at planning time, so every check below runs without an Elasticsearch server.

SET enable_elasticsearch = 1;

DROP TABLE IF EXISTS es;
DROP TABLE IF EXISTS es_typed;
DROP TABLE IF EXISTS es_overlapping;

CREATE TABLE es ENGINE = Elasticsearch('http://localhost:1', 'idx');
CREATE TABLE es_typed (doc JSON(a Array(UInt32))) ENGINE = Elasticsearch('http://localhost:1', 'idx');
CREATE TABLE es_overlapping (doc JSON(a Array(JSON), a.b Int64)) ENGINE = Elasticsearch('http://localhost:1', 'idx');

SELECT 'path', trimBoth(explain) FROM (EXPLAIN SELECT document.a.b FROM es) WHERE explain LIKE '%Source:%';
SELECT 'type hint', trimBoth(explain) FROM (EXPLAIN SELECT document.a.b.:Int64 FROM es) WHERE explain LIKE '%Source:%';
SELECT 'array hint', trimBoth(explain) FROM (EXPLAIN SELECT document.a.:`Array(JSON)`.c FROM es) WHERE explain LIKE '%Source:%';
SELECT 'sub-object', trimBoth(explain) FROM (EXPLAIN SELECT document.^a.b FROM es) WHERE explain LIKE '%Source:%';
SELECT 'combined', trimBoth(explain) FROM (EXPLAIN SELECT document.@a FROM es) WHERE explain LIKE '%Source:%';
SELECT 'prefix inside path', trimBoth(explain) FROM (EXPLAIN SELECT document.a.^b FROM es) WHERE explain LIKE '%Source:%';
SELECT 'whole document', trimBoth(explain) FROM (EXPLAIN SELECT document FROM es) WHERE explain LIKE '%Source:%';
SELECT 'whole document and path', trimBoth(explain) FROM (EXPLAIN SELECT document, document.a FROM es) WHERE explain LIKE '%Source:%';
SELECT 'virtual only', trimBoth(explain) FROM (EXPLAIN SELECT _id, _index FROM es) WHERE explain LIKE '%Source:%';
SELECT 'path in WHERE', trimBoth(explain) FROM (EXPLAIN SELECT _id FROM es WHERE document.x = 1) WHERE explain LIKE '%Source:%';
SELECT 'duplicates', trimBoth(explain) FROM (EXPLAIN SELECT document.a.b, document.a.b.:Int64, document.x FROM es) WHERE explain LIKE '%Source:%';
SELECT 'inside typed path', trimBoth(explain) FROM (EXPLAIN SELECT doc.a.size0 FROM es_typed) WHERE explain LIKE '%Source:%';
SELECT 'typed path', trimBoth(explain) FROM (EXPLAIN SELECT doc.a FROM es_typed) WHERE explain LIKE '%Source:%';
SELECT 'overlapping typed path', trimBoth(explain) FROM (EXPLAIN SELECT doc.a.b FROM es_overlapping) WHERE explain LIKE '%Source:%';
SELECT 'escaped dot, setting off', trimBoth(explain) FROM (EXPLAIN SELECT document.`a%2Eb` FROM es) WHERE explain LIKE '%Source:%';
SELECT 'escaped dot, setting on', trimBoth(explain) FROM (EXPLAIN SELECT document.`a%2Eb` FROM es) WHERE explain LIKE '%Source:%' SETTINGS json_type_escape_dots_in_keys = 1;
SELECT 'plain dot, setting on', trimBoth(explain) FROM (EXPLAIN SELECT document.a.b FROM es) WHERE explain LIKE '%Source:%' SETTINGS json_type_escape_dots_in_keys = 1;

SELECT 'json', extract(explain, '"Source": "([^"]*)"') FROM (EXPLAIN json = 1, actions = 1, description = 0 SELECT document.a.b, document.x FROM es);

DROP TABLE es;
DROP TABLE es_typed;
DROP TABLE es_overlapping;
