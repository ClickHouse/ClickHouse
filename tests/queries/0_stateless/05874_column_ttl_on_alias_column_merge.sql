-- A column TTL on a column that is not stored (ALIAS, EPHEMERAL) must not break TTL merges.

DROP TABLE IF EXISTS t_alias_ttl;
CREATE TABLE t_alias_ttl (id UInt64, dt Date, arr Array(UInt64), al Array(UInt64) ALIAS arr TTL dt + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias_ttl VALUES (1, '2000-01-01', [1, 2, 3]), (2, '2100-01-01', [4]);
OPTIMIZE TABLE t_alias_ttl FINAL;
SELECT id, arr, al FROM t_alias_ttl ORDER BY id;
DROP TABLE t_alias_ttl;

DROP TABLE IF EXISTS t_ephemeral_ttl;
CREATE TABLE t_ephemeral_ttl (id UInt64, dt Date, e UInt64 EPHEMERAL 0 TTL dt + INTERVAL 1 MONTH, v UInt64 DEFAULT e)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_ephemeral_ttl (id, dt, e) VALUES (1, '2000-01-01', 5);
OPTIMIZE TABLE t_ephemeral_ttl FINAL;
SELECT id, v FROM t_ephemeral_ttl;
DROP TABLE t_ephemeral_ttl;
