-- The elements of tuples are compared positionally by `IN`, so the conversion of set elements into the
-- input of a deterministic key transform (`sipHash64(v)`) must be positional too, otherwise the index
-- prunes rows that satisfy the predicate.

DROP TABLE IF EXISTS t_partition;
DROP TABLE IF EXISTS t_order;

CREATE TABLE t_partition (v Tuple(a UInt8, b UInt8)) ENGINE = MergeTree PARTITION BY sipHash64(v) ORDER BY tuple();
INSERT INTO t_partition VALUES ((1, 2));

SELECT sum(v IN (SELECT CAST((1, 2), 'Tuple(b UInt8, a UInt8)'))) FROM t_partition;
SELECT count() FROM t_partition WHERE v IN (SELECT CAST((1, 2), 'Tuple(b UInt8, a UInt8)'));
SELECT count() FROM t_partition WHERE v IN (SELECT CAST((2, 1), 'Tuple(b UInt8, a UInt8)'));
SELECT count() FROM t_partition WHERE v = CAST((1, 2), 'Tuple(b UInt8, a UInt8)');

CREATE TABLE t_order (v Tuple(a UInt8, b UInt8)) ENGINE = MergeTree ORDER BY sipHash64(v) SETTINGS index_granularity = 1;
INSERT INTO t_order VALUES ((1, 2)), ((3, 4)), ((5, 6));

SELECT count() FROM t_order WHERE v IN (SELECT CAST((1, 2), 'Tuple(b UInt8, a UInt8)'));
SELECT count() FROM t_order WHERE v IN (SELECT CAST((1, 2), 'Tuple(b UInt8, a UInt8)')) SETTINGS use_primary_key = 0;
SELECT count() FROM t_order WHERE v = CAST((3, 4), 'Tuple(b UInt8, a UInt8)');
SELECT count() FROM t_order WHERE v = CAST((3, 4), 'Tuple(b UInt8, a UInt8)') SETTINGS use_primary_key = 0;

DROP TABLE t_partition;
DROP TABLE t_order;
