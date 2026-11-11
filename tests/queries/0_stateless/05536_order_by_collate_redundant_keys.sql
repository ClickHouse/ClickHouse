-- Tags: no-fasttest, shard
-- no-fasttest: needs ICU
-- Different values can be equal with a collation, so a sort key is needed after the same key with COLLATE.

SET enable_positional_arguments = 1, optimize_redundant_functions_in_order_by = 1;

SELECT s FROM (SELECT arrayJoin(['b', 'A', 'a', 'A']) AS s ORDER BY 1 COLLATE 'en-u-ks-level2', s LIMIT 1 WITH TIES);
SELECT '-';
SELECT s FROM (SELECT arrayJoin(['b', 'A', 'a', 'A']) AS s ORDER BY s COLLATE 'en-u-ks-level2', s LIMIT 1 WITH TIES);
SELECT '-';
SELECT s FROM (SELECT arrayJoin(['b', 'A', 'a', 'A']) AS s ORDER BY 1 COLLATE 'en-u-ks-level2', 1 DESC LIMIT 1 WITH TIES);
SELECT '-';
SELECT s FROM (SELECT arrayJoin(['b', 'A', 'a', 'A']) AS s ORDER BY s COLLATE 'en-u-ks-level2', s DESC LIMIT 1 WITH TIES);
SELECT '-';
SELECT hex(s) FROM (SELECT arrayJoin([unhex('C3A9'), unhex('65CC81')]) AS s ORDER BY 1 COLLATE 'en', s LIMIT 1 WITH TIES);
SELECT '-';
SELECT hex(s) FROM (SELECT arrayJoin([unhex('C3A9'), unhex('65CC81')]) AS s ORDER BY s COLLATE 'en', s LIMIT 1 WITH TIES);
SELECT '-';
SELECT number % 2 ? 'a' : 'A' AS s FROM remote('127.0.0.{2,3}', numbers(2)) ORDER BY 1 COLLATE 'en-u-ks-level2', s;
SELECT '-';
SELECT number % 2 ? 'a' : 'A' AS s FROM remote('127.0.0.{2,3}', numbers(2)) ORDER BY s COLLATE 'en-u-ks-level2', s;
SELECT '-';

-- And functions of it.
SELECT s FROM (SELECT x AS s FROM (SELECT arrayJoin(['b', 'A', 'a', 'A']) AS x) ORDER BY 1 COLLATE 'en-u-ks-level2', hex(s) LIMIT 1 WITH TIES);
SELECT '-';
SELECT s FROM (SELECT x AS s FROM (SELECT arrayJoin(['b', 'A', 'a', 'A']) AS x) ORDER BY s COLLATE 'en-u-ks-level2', hex(s) LIMIT 1 WITH TIES);
SELECT '-';

-- Not needed after the same key without COLLATE.
SELECT countIf(explain LIKE '%COLLATE%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT toString(number) AS s FROM numbers(2) ORDER BY s, s COLLATE 'en');
SELECT countIf(explain LIKE '%COLLATE%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT toString(number) AS s FROM numbers(2) ORDER BY 1, 1 COLLATE 'en');
