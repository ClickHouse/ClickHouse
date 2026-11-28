-- The read step opens no connection at planning time, so every check below runs without an Elasticsearch server.

SET enable_elasticsearch = 1;

SELECT trimBoth(explain) FROM (EXPLAIN SELECT * FROM elasticsearch('http://localhost:1', 'idx'))
WHERE explain LIKE '%ReadFromElasticsearch%' OR explain LIKE '%Index:%' OR explain LIKE '%Query:%';

SELECT trimBoth(explain) FROM (EXPLAIN SELECT _id FROM elasticsearch('http://localhost:1', 'logs-*', query('{"term":{"level":"error"}}')))
WHERE explain LIKE '%ReadFromElasticsearch%' OR explain LIKE '%Index:%' OR explain LIKE '%Query:%';

SELECT countIf(explain LIKE '%(ReadFromElasticsearch)%') > 0, countIf(explain LIKE '%ElasticsearchSource%') > 0
FROM (EXPLAIN PIPELINE SELECT * FROM elasticsearch('http://localhost:1', 'idx'));
