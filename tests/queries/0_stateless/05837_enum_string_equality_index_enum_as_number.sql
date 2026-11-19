-- `=`, `!=` and `<=>` read a string as an `Enum` number under `input_format_tsv_enum_as_number`, an ordering reads it as a name.
-- the primary key, the partition key and the skip indexes must not prune the rows that match.

DROP TABLE IF EXISTS t_enum_pk;
DROP TABLE IF EXISTS t_enum_minmax;
DROP TABLE IF EXISTS t_enum_bloom;
DROP TABLE IF EXISTS t_enum_partition;

CREATE TABLE t_enum_pk (e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4)) ENGINE = MergeTree ORDER BY e SETTINGS index_granularity = 1, index_granularity_bytes = 0;
CREATE TABLE t_enum_minmax (k UInt8, e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4), INDEX i e TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, index_granularity_bytes = 0;
CREATE TABLE t_enum_bloom (k UInt8, e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4), INDEX i e TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, index_granularity_bytes = 0;
CREATE TABLE t_enum_partition (e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4)) ENGINE = MergeTree PARTITION BY e ORDER BY tuple();

INSERT INTO t_enum_pk VALUES ('z'), ('a'), ('1'), ('b');
INSERT INTO t_enum_minmax VALUES (1, 'z'), (2, 'a'), (3, '1'), (4, 'b');
INSERT INTO t_enum_bloom VALUES (1, 'z'), (2, 'a'), (3, '1'), (4, 'b');
INSERT INTO t_enum_partition VALUES ('z'), ('a'), ('1'), ('b');

SET input_format_tsv_enum_as_number = 1, optimize_use_implicit_projections = 0;

SELECT 'primary key, equals', arraySort(groupArray(e)) FROM t_enum_pk WHERE e = '1';
SELECT 'primary key, equals reversed', arraySort(groupArray(e)) FROM t_enum_pk WHERE '1' = e;
SELECT 'primary key, equals and less', arraySort(groupArray(e)) FROM t_enum_pk WHERE e = '1' AND e < '1';
SELECT 'primary key, not equals', arraySort(groupArray(e)) FROM t_enum_pk WHERE e != '1';
SELECT 'primary key, is not distinct from', arraySort(groupArray(e)) FROM t_enum_pk WHERE e <=> '1';
SELECT 'primary key, tuple equals', arraySort(groupArray(e)) FROM t_enum_pk WHERE (e, 1) = ('1', 1);
SELECT 'primary key, LowCardinality', arraySort(groupArray(e)) FROM t_enum_pk WHERE e = toLowCardinality('1');
SELECT 'minmax, equals', arraySort(groupArray(e)) FROM t_enum_minmax WHERE e = '1';
SELECT 'minmax, equals and less', arraySort(groupArray(e)) FROM t_enum_minmax WHERE e = '1' AND e < '1';
SELECT 'bloom_filter, equals', arraySort(groupArray(e)) FROM t_enum_bloom WHERE e = '1';
SELECT 'bloom_filter, equals reversed', arraySort(groupArray(e)) FROM t_enum_bloom WHERE '1' = e;
SELECT 'bloom_filter, equals and less', arraySort(groupArray(e)) FROM t_enum_bloom WHERE e = '1' AND e < '1';
SELECT 'bloom_filter, tuple equals', arraySort(groupArray(e)) FROM t_enum_bloom WHERE (e, k) = ('1', 1);
SELECT 'partition, equals', arraySort(groupArray(e)) FROM t_enum_partition WHERE e = '1';
SELECT 'partition, equals and less', arraySort(groupArray(e)) FROM t_enum_partition WHERE e = '1' AND e < '1';

-- without the setting a string is an `Enum` name for both.
SET input_format_tsv_enum_as_number = 0;

SELECT 'setting off, primary key', arraySort(groupArray(e)) FROM t_enum_pk WHERE e = '1';
SELECT 'setting off, minmax', arraySort(groupArray(e)) FROM t_enum_minmax WHERE e = '1';
SELECT 'setting off, bloom_filter', arraySort(groupArray(e)) FROM t_enum_bloom WHERE e = '1';
SELECT 'setting off, partition', arraySort(groupArray(e)) FROM t_enum_partition WHERE e = '1';

DROP TABLE t_enum_pk;
DROP TABLE t_enum_minmax;
DROP TABLE t_enum_bloom;
DROP TABLE t_enum_partition;
