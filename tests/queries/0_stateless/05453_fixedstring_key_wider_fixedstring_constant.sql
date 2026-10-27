-- Tags: no-parallel-replicas
-- no-parallel-replicas: `EXPLAIN indexes = 1` differs with parallel replicas.

-- https://github.com/ClickHouse/ClickHouse/issues/74974
-- `FixedString` values of different widths compare as if the narrower one were padded with zero bytes, and `IN`
-- pads the key to a wider set element, so `toFixedString('a', 3)` matches the `FixedString(1)` key value `'a'`.
-- Each predicate is evaluated without an index (`countIf`), with the primary key only and with partition pruning only,
-- and `EXPLAIN` shows that the indexes prune, also with the primary key alone.

SET optimize_use_implicit_projections = 0, use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_fixedstring_key;
CREATE TABLE t_fixedstring_key (a FixedString(1)) ENGINE = MergeTree PARTITION BY a ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_fixedstring_key SELECT toFixedString(char(97 + number % 3), 1) FROM numbers(30);
OPTIMIZE TABLE t_fixedstring_key FINAL;

SELECT 'equals';
SELECT countIf(a = toFixedString('a', 3)) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a = toFixedString('a', 3) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a = toFixedString('a', 3) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a = toFixedString('a', 3)) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a = toFixedString('a', 3) SETTINGS use_partition_pruning = 0) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';

SELECT 'in';
SELECT countIf(a IN (SELECT toFixedString('a', 3))) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a IN (SELECT toFixedString('a', 3)) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a IN (SELECT toFixedString('a', 3)) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a IN (SELECT toFixedString('a', 3))) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a IN (SELECT toFixedString('a', 3)) SETTINGS use_partition_pruning = 0) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';

SELECT 'less';
SELECT countIf(a < toFixedString('b', 3)) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a < toFixedString('b', 3) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a < toFixedString('b', 3) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a < toFixedString('b', 3)) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';

SELECT 'a nonzero byte past the key width matches nothing';
SELECT countIf(a = toFixedString('ab', 3)) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a = toFixedString('ab', 3) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a = toFixedString('ab', 3) SETTINGS use_primary_key = 0;
SELECT countIf(a != toFixedString('ab', 3)) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a != toFixedString('ab', 3) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a != toFixedString('ab', 3) SETTINGS use_primary_key = 0;
SELECT countIf(a IN (SELECT arrayJoin([toFixedString('ab', 3), toFixedString('b', 3)]))) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a IN (SELECT arrayJoin([toFixedString('ab', 3), toFixedString('b', 3)])) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a IN (SELECT arrayJoin([toFixedString('ab', 3), toFixedString('b', 3)])) SETTINGS use_primary_key = 0;
SELECT countIf(a NOT IN (SELECT toFixedString('ab', 3))) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a NOT IN (SELECT toFixedString('ab', 3)) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a NOT IN (SELECT toFixedString('ab', 3)) SETTINGS use_primary_key = 0;

SELECT 'not equals';
SELECT countIf(a != toFixedString('a', 3)) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a != toFixedString('a', 3) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a != toFixedString('a', 3) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a != toFixedString('a', 3)) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';

SELECT 'not in';
SELECT countIf(a NOT IN (SELECT toFixedString('a', 3))) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a NOT IN (SELECT toFixedString('a', 3)) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a NOT IN (SELECT toFixedString('a', 3)) SETTINGS use_primary_key = 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT a FROM t_fixedstring_key WHERE a NOT IN (SELECT toFixedString('a', 3))) WHERE explain ILIKE '%Parts:%' OR explain ILIKE '%Granules:%';

SELECT 'nullable set';
SELECT countIf(a IN (SELECT CAST(toFixedString('a', 3), 'Nullable(FixedString(3))'))) FROM t_fixedstring_key;
SELECT count() FROM t_fixedstring_key WHERE a IN (SELECT CAST(toFixedString('a', 3), 'Nullable(FixedString(3))')) SETTINGS use_partition_pruning = 0;
SELECT count() FROM t_fixedstring_key WHERE a IN (SELECT CAST(toFixedString('a', 3), 'Nullable(FixedString(3))')) SETTINGS use_primary_key = 0;

DROP TABLE t_fixedstring_key;

SELECT 'nullable key';
-- `nullIf` keeps the bytes `'ab\0'` under its NULL, and that NULL matches the NULL keys with `transform_null_in = 1`.
DROP TABLE IF EXISTS t_nullable_fixedstring_key;
CREATE TABLE t_nullable_fixedstring_key (a Nullable(FixedString(1))) ENGINE = MergeTree ORDER BY a SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_nullable_fixedstring_key SELECT if(number % 3 = 2, NULL, toFixedString(char(97 + number % 3), 1)) FROM numbers(30);
SELECT countIf(a IN (SELECT nullIf(toFixedString('ab', 3), toFixedString('ab', 3)))) FROM t_nullable_fixedstring_key SETTINGS transform_null_in = 1;
SELECT count() FROM t_nullable_fixedstring_key WHERE a IN (SELECT nullIf(toFixedString('ab', 3), toFixedString('ab', 3))) SETTINGS transform_null_in = 1;
DROP TABLE t_nullable_fixedstring_key;
