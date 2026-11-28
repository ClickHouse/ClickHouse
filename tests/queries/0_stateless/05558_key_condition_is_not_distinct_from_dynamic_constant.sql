-- Random settings limits: optimize_use_projections=(1, None); optimize_use_implicit_projections=(1, None)

-- `<=>` compares a `Dynamic` constant together with its type, so it matches no key value of another type.

DROP TABLE IF EXISTS t_nsd;
CREATE TABLE t_nsd (k UInt8, s String) ENGINE = MergeTree ORDER BY (k, s) SETTINGS index_granularity = 1;
INSERT INTO t_nsd SELECT number % 3, toString(number) FROM numbers(6);

SELECT count() FROM t_nsd WHERE k <=> '1'::Dynamic;
SELECT count() FROM t_nsd WHERE '1'::Dynamic <=> k;
SELECT groupArray(s) FROM (SELECT s FROM t_nsd WHERE NOT (k <=> '1'::Dynamic) ORDER BY s);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd WHERE NOT (k <=> toUInt16(1)::Dynamic) ORDER BY s);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd WHERE NOT (k <=> toFloat64(1)::Dynamic) ORDER BY s);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd WHERE NOT (k <=> '{"a" : 1}'::JSON.a) ORDER BY s);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd WHERE NOT (k <=> toUInt16(1)::Dynamic(max_types = 0)) ORDER BY s);

-- A constant of the key type still uses the primary key.
SELECT count() FROM t_nsd WHERE k <=> toUInt8(1)::Dynamic;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_nsd WHERE k <=> toUInt8(1)::Dynamic) WHERE explain ILIKE '%Condition: (k in [1, 1])%' SETTINGS enable_parallel_replicas = 0;

-- `X IS TRUE` written as `X <=> 1`: a `Dynamic` 1 of another type than `X` is distinct from every `X`.
SELECT count() FROM t_nsd WHERE (k = 1) <=> toUInt16(1)::Dynamic;
SELECT count() FROM t_nsd WHERE toUInt16(1)::Dynamic <=> (k = 1);
SELECT count() FROM t_nsd WHERE (k = 1) <=> toUInt16(1)::Dynamic(max_types = 0);
SELECT count() FROM t_nsd WHERE (k = 1) <=> toUInt8(1)::Dynamic;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_nsd WHERE (k = 1) <=> toUInt8(1)::Dynamic) WHERE explain ILIKE '%Condition: (k in [1, 1])%' SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_nsd;

DROP TABLE IF EXISTS t_nsd_nullable;
CREATE TABLE t_nsd_nullable (k Nullable(UInt8), s String) ENGINE = MergeTree ORDER BY (k, s) SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_nsd_nullable SELECT if(number = 5, NULL, number % 3), toString(number) FROM numbers(6);
SELECT count() FROM t_nsd_nullable WHERE k <=> '1'::Dynamic;
SELECT groupArray(s) FROM (SELECT s FROM t_nsd_nullable WHERE NOT (k <=> '1'::Dynamic) ORDER BY s);
SELECT count() FROM t_nsd_nullable WHERE (k = 1) <=> toUInt16(1)::Dynamic;
DROP TABLE t_nsd_nullable;

DROP TABLE IF EXISTS t_nsd_lc;
CREATE TABLE t_nsd_lc (k LowCardinality(String), s String) ENGINE = MergeTree ORDER BY (k, s) SETTINGS index_granularity = 1;
INSERT INTO t_nsd_lc SELECT toString(number % 3), toString(number) FROM numbers(6);
SELECT count() FROM t_nsd_lc WHERE k <=> toLowCardinality('1')::Dynamic;
SELECT count() FROM t_nsd_lc WHERE k <=> '1'::Dynamic;
SELECT count() FROM t_nsd_lc WHERE (k = '1') <=> toUInt16(1)::Dynamic;
SELECT count() FROM t_nsd_lc WHERE (k = '1') <=> toUInt8(1)::Dynamic;
DROP TABLE t_nsd_lc;

DROP TABLE IF EXISTS t_nsd_partition;
CREATE TABLE t_nsd_partition (k UInt8, s String) ENGINE = MergeTree PARTITION BY k ORDER BY s;
INSERT INTO t_nsd_partition SELECT number % 3, toString(number) FROM numbers(6);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd_partition WHERE NOT (k <=> '1'::Dynamic) ORDER BY s);
DROP TABLE t_nsd_partition;

DROP TABLE IF EXISTS t_nsd_tuple;
CREATE TABLE t_nsd_tuple (t Tuple(UInt8, String), s String) ENGINE = MergeTree ORDER BY (t, s) SETTINGS index_granularity = 1;
INSERT INTO t_nsd_tuple SELECT (number % 3, toString(number % 3)), toString(number) FROM numbers(6);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd_tuple WHERE NOT (t <=> (toUInt16(1), '1')::Tuple(Dynamic, String)) ORDER BY s);
SELECT groupArray(s) FROM (SELECT s FROM t_nsd_tuple WHERE NOT (t <=> (toUInt16(1), '1')::Dynamic) ORDER BY s);
DROP TABLE t_nsd_tuple;
