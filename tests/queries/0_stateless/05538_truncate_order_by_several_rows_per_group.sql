-- Tags: shard
-- `ORDER BY` cannot be truncated if there can be several rows with equal values of `GROUP BY` keys.

SET enable_positional_arguments = 1;
SET optimize_truncate_order_by_after_group_by_keys = 1;

SELECT 'arrayJoin';
SELECT number % 3 AS x, row_number() OVER (PARTITION BY x) AS r, count() AS c FROM numbers(6) GROUP BY 1 HAVING arrayJoin([1, 1]) = count() - 1 ORDER BY x, r DESC;
SELECT '-';
SELECT number % 3 AS x, row_number() OVER (PARTITION BY x) AS r, count() AS c FROM numbers(6) GROUP BY x HAVING arrayJoin([1, 1]) = count() - 1 ORDER BY x, r DESC;
SELECT '-';
SELECT number % 3 AS x, row_number() OVER (PARTITION BY x) AS r, count() AS c FROM numbers(6) GROUP BY x HAVING arrayJoin([1, 1]) = count() - 1 ORDER BY 1, r DESC;

SELECT '-';
SELECT countIf(explain LIKE '%count() DESC%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT number % 3 AS x, count() AS c FROM numbers(6) GROUP BY 1 QUALIFY arrayJoin([1, 1]) = row_number() OVER (PARTITION BY x) ORDER BY x, c DESC);
SELECT '-';
SELECT number % 2 AS k, arrayJoin([1, 2]) AS j FROM numbers(4) GROUP BY k ORDER BY k, arrayMap(y -> -y, [j]);
SELECT '-';
SELECT toYear(toDate(number * 400)) AS k, arrayJoin([1, 2]) AS j FROM numbers(2) GROUP BY 1 ORDER BY 1, j DESC, k = 1970;

SELECT 'equal values of different keys';
SELECT if(number % 2, -0., 0.) AS x, count() AS c FROM numbers(5) GROUP BY 1 ORDER BY x, c;
SELECT '-';
SELECT if(number % 2, -0., 0.) AS x, count() AS c FROM numbers(5) GROUP BY 1 ORDER BY x, c DESC;
SELECT '-';
SELECT if(number % 2, -0., 0.) AS x, count() AS c FROM numbers(5) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT if(number % 2, -0., 0.) AS x, count() AS c FROM numbers(5) GROUP BY x ORDER BY x, c DESC;

SELECT '-';
SELECT arrayJoin([0., -0., 0.]) AS x, count() AS c FROM numbers(2) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT arrayJoin([0., -0., 0.]) AS x, count() AS c FROM numbers(2) GROUP BY 1 ORDER BY 1, c DESC;

SELECT 'stateful functions';
SELECT number % 3 AS x, rowNumberInAllBlocks() AS r, count() AS c FROM numbers(6) GROUP BY 1 ORDER BY x, r DESC FORMAT Null;
SELECT countIf(explain LIKE '%rowNumberInAllBlocks() DESC%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT number % 3 AS x, rowNumberInAllBlocks() AS r, count() AS c FROM numbers(6) GROUP BY 1 ORDER BY x, r DESC);
SELECT countIf(explain LIKE '%count() DESC%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT number % 3 AS x, count() AS c FROM numbers(6) GROUP BY 1 ORDER BY x, c DESC);

SELECT 'equal keys with other constants';
SELECT arrayJoin([number % 2, 2]) AS x, count() AS c FROM numbers(4) GROUP BY x ORDER BY x, arrayJoin([number % 2, toUInt8(2)]);
SELECT '-';
SELECT arrayJoin([number % 2, 2]) AS x, number AS y FROM numbers(2) GROUP BY 1, 2 ORDER BY 1, 2, arrayJoin([number % 2, toUInt8(2)]);
SELECT '-';
SELECT intDiv(number, 4) AS x, count() AS c FROM numbers(8) GROUP BY x ORDER BY x, intDiv(number, toUInt8(4)) SETTINGS optimize_truncate_order_by_after_group_by_keys = 0;
SELECT '-';
SELECT intDiv(number, 4) AS x, count() AS c FROM remote('127.0.0.{2,3}', numbers(8)) GROUP BY 1 ORDER BY 1, intDiv(number, toUInt8(4)) SETTINGS optimize_truncate_order_by_after_group_by_keys = 0;

SELECT 'distributed_group_by_no_merge';
SET distributed_group_by_no_merge = 2;

SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 ORDER BY x, c;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 ORDER BY x, c DESC;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x ORDER BY x, c DESC;
SELECT '-';
SELECT number % 2 AS x, shardNum() AS s FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY x ORDER BY x, shardNum();
SELECT '-';
SELECT number % 2 AS x, shardNum() AS s FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY 1 ORDER BY 1, shardNum() DESC;
SELECT '-';
SELECT number AS x FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY x ORDER BY x, count() OVER ();
