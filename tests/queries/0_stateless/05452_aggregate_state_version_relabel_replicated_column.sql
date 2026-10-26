-- A versioned aggregate state column that arrives lazily replicated from a shard (here after `ARRAY JOIN`)
-- is relabelled to the version of the header without losing its replicated rows.
DROP TABLE IF EXISTS uniq_legacy;
CREATE TABLE uniq_legacy (state AggregateFunction(0, uniq, UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO uniq_legacy SELECT uniqState(number) FROM numbers(1000);

SELECT uniqMerge(state), count()
FROM (SELECT state FROM remote('127.0.0.2', currentDatabase(), uniq_legacy) ARRAY JOIN [1, 2, 3] AS x)
SETTINGS prefer_localhost_replica = 0, enable_lazy_columns_replication = 1;

DROP TABLE uniq_legacy;
