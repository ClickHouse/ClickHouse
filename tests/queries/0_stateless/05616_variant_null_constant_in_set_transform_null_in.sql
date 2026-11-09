-- A constant IN set keeps a NULL member for a key type that can hold NULL (Variant, LowCardinality(Nullable)) under transform_null_in = 1, like the subquery-built set.

SET transform_null_in = 1;

SELECT 'A in_null', CAST(NULL, 'Variant(UInt64, String)') IN (NULL);
SELECT 'A in_null_scalar', CAST(NULL, 'Variant(UInt64, String)') IN NULL;
SELECT 'A in_null_1', CAST(NULL, 'Variant(UInt64, String)') IN (NULL, 1);
SELECT 'A in_array', CAST(NULL, 'Variant(UInt64, String)') IN [NULL, 1];
SELECT 'A in_str_null', CAST(NULL, 'Variant(UInt64, String)') IN ('abc', NULL);
SELECT 'A in_variant_null', CAST(NULL, 'Variant(UInt64, String)') IN (CAST(NULL, 'Variant(UInt64, String)'));
SELECT 'A not_in_null_1', CAST(NULL, 'Variant(UInt64, String)') NOT IN (NULL, 1);
SELECT 'A materialized', materialize(CAST(NULL, 'Variant(UInt64, String)')) IN (NULL, 1);
SELECT 'A nullIn', nullIn(CAST(NULL, 'Variant(UInt64, String)'), (NULL, 1));
SELECT 'A notNullIn', notNullIn(CAST(NULL, 'Variant(UInt64, String)'), (NULL, 1));
SELECT 'A ctl_in_1', CAST(NULL, 'Variant(UInt64, String)') IN (1);
SELECT 'A ctl_1_in', CAST(1::UInt64, 'Variant(UInt64, String)') IN (NULL, 1);
SELECT 'A ctl_2_in', CAST(2::UInt64, 'Variant(UInt64, String)') IN (NULL, 1);
SELECT 'A ctl_str_in', CAST('abc', 'Variant(UInt64, String)') IN ('abc', NULL);
SELECT 'A ctl_nullable', CAST(NULL, 'Nullable(UInt64)') IN (NULL);
SELECT 'A ctl_uint_0', 0::UInt64 IN (NULL);
SELECT 'A ctl_uint_1', 1::UInt64 IN (NULL, 1);

DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_mt;
CREATE TABLE t_mem (id UInt8, v Variant(UInt64, String)) ENGINE = Memory;
CREATE TABLE t_mt (id UInt8, v Variant(UInt64, String), INDEX i_set v TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_mem VALUES (1, NULL), (2, 1), (3, 'abc'), (4, 2);
INSERT INTO t_mt VALUES (1, NULL), (2, 1), (3, 'abc'), (4, 2);

SELECT 'B mem_in', groupArray(id) FROM (SELECT id FROM t_mem WHERE v IN (NULL, 1) ORDER BY id);
SELECT 'B mem_not_in', groupArray(id) FROM (SELECT id FROM t_mem WHERE v NOT IN (NULL, 1) ORDER BY id);
SELECT 'B mem_in_array', groupArray(id) FROM (SELECT id FROM t_mem WHERE v IN [NULL, 1] ORDER BY id);
SELECT 'B mem_in_subquery', groupArray(id) FROM (SELECT id FROM t_mem WHERE v IN (SELECT CAST(NULL, 'Variant(UInt64, String)') UNION ALL SELECT CAST(1::UInt64, 'Variant(UInt64, String)')) ORDER BY id);
SELECT 'B mt_in', groupArray(id) FROM (SELECT id FROM t_mt WHERE v IN (NULL, 1) ORDER BY id);
SELECT 'B mt_in_no_skip_index', groupArray(id) FROM (SELECT id FROM t_mt WHERE v IN (NULL, 1) ORDER BY id) SETTINGS use_skip_indexes = 0;
SELECT 'B mt_not_in', groupArray(id) FROM (SELECT id FROM t_mt WHERE v NOT IN (NULL, 1) ORDER BY id);
SELECT 'B mt_in_array', groupArray(id) FROM (SELECT id FROM t_mt WHERE v IN [NULL, 1] ORDER BY id);
SELECT 'B mt_in_subquery', groupArray(id) FROM (SELECT id FROM t_mt WHERE v IN (SELECT CAST(NULL, 'Variant(UInt64, String)') UNION ALL SELECT CAST(1::UInt64, 'Variant(UInt64, String)')) ORDER BY id);

SELECT 'C tuple', (CAST(NULL, 'Variant(UInt64, String)'), 1) IN ((NULL, 1), (2, 2));
SELECT 'C array', (CAST(NULL, 'Variant(UInt64, String)'), 1) IN [(NULL, 1), (2, 2)];
SELECT 'C other_key', (CAST(NULL, 'Variant(UInt64, String)'), 2) IN ((NULL, 1), (2, 2));
SELECT 'C table', groupArray(id) FROM (SELECT id FROM t_mem WHERE (v, id) IN ((NULL, 1), (1, 2)) ORDER BY id);
SELECT 'C ctl_nullable', (CAST(NULL, 'Nullable(UInt64)'), 1) IN ((NULL, 1), (2, 2));

SELECT 'D field', CAST((NULL, 1), 'Nullable(Tuple(Variant(UInt64, String), UInt8))') IN ((NULL, 1), (2, 2));
SELECT 'D fields', CAST((NULL, NULL), 'Nullable(Tuple(Variant(UInt64, String), Variant(UInt64, String)))') IN ((NULL, NULL), (1, 1));
SELECT 'D single_field', CAST(tuple(NULL), 'Nullable(Tuple(Variant(UInt64, String)))') IN (tuple(NULL), tuple('a'));
SELECT 'D single_lc_field', CAST(tuple(NULL), 'Nullable(Tuple(LowCardinality(Nullable(String))))') IN (tuple(NULL), tuple('a'));
SELECT 'D literal_variants', CAST((NULL, NULL), 'Nullable(Tuple(Variant(UInt64, String), Variant(UInt64, String)))') IN (NULL, NULL);
SELECT 'D literal_variant_nullable', CAST((NULL, NULL), 'Nullable(Tuple(Variant(UInt64, String), Nullable(UInt64)))') IN (NULL, NULL);
SELECT 'D literal_lc', CAST((NULL, NULL), 'Nullable(Tuple(LowCardinality(Nullable(String)), LowCardinality(Nullable(String))))') IN (NULL, NULL);
SELECT 'D ctl_literal_nullable', CAST((NULL, NULL), 'Nullable(Tuple(Nullable(UInt64), Nullable(UInt64)))') IN (NULL, NULL);
SELECT 'D ctl_outer_null', CAST(NULL, 'Nullable(Tuple(Variant(UInt64, String), Variant(UInt64, String)))') IN (NULL, NULL);

DROP TABLE t_mem;
DROP TABLE t_mt;
