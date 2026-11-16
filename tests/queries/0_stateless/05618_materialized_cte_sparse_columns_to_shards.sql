-- A materialized CTE keeps the `Sparse` columns it has read, and the table behind it is sent to the shards
-- as an external table. A shard accepts only the default serialization in the blocks it receives.

SET enable_materialized_cte = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_sparse;

CREATE TABLE t_sparse (id UInt64, x UInt64, t Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;

INSERT INTO t_sparse SELECT number, if(number % 50 = 0, number, 0), (if(number % 30 = 0, number, 0), '') FROM numbers(1000);

SELECT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_sparse' AND active AND column = 'x';

-- The CTE is referenced twice, otherwise it is inlined. `127.0.0.2` is not a local address: the shard is reached over the network.
WITH m AS MATERIALIZED (SELECT id, x, t FROM t_sparse)
SELECT count(), sum(m.x), countIf(m.t != (0, ''))
FROM remote('127.0.0.2', numbers(1000)) AS r
INNER JOIN m ON r.number = m.id
WHERE m.x IN (SELECT x FROM m);

DROP TABLE t_sparse;
