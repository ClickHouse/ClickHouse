-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-parallel-replicas
-- Parallel replicas cannot serialize a distributed plan for UNIQUE KEY tables.
-- `CLEAR COLUMN IF EXISTS` of a Nested parent that has only an alias child is a no-op on UNIQUE KEY.

SET enable_unique_key = 1;

DROP TABLE IF EXISTS t_uk_alias_nested;
CREATE TABLE t_uk_alias_nested (id UInt32, `n.x` UInt32 ALIAS id)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id);

INSERT INTO t_uk_alias_nested VALUES (1);

ALTER TABLE t_uk_alias_nested CLEAR COLUMN IF EXISTS n IN PARTITION ID 'all';
SELECT id, n.x FROM t_uk_alias_nested;

DROP TABLE t_uk_alias_nested;
