-- Tags: shard
-- There can be several rows with the same `GROUP BY` keys if the groups of different shards are not merged.

SET enable_positional_arguments = 1;
SET optimize_truncate_order_by_after_group_by_keys = 1;
SET distributed_group_by_no_merge = 2;

SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 ORDER BY x, c;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 ORDER BY x, c DESC;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x ORDER BY x, c;
SELECT '-';
SELECT number % 3 AS x, max(shardNum()) AS c FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY x ORDER BY x, c DESC;
