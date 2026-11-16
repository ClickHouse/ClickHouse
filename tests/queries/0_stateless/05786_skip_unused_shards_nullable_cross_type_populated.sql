-- Tags: shard, no-parallel
-- Populated two-shard regression for Nullable empty-string pruning and cross-type
-- multi-key comparisons (String/FixedString zero padding, Decimal/Float coercion,
-- and Float signed-zero hashing).

SET prefer_localhost_replica = 0;
SET allow_suspicious_low_cardinality_types = 1;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

DROP TABLE IF EXISTS dist_05786_null_str;
DROP TABLE IF EXISTS shard_0.t_05786_null_str;
DROP TABLE IF EXISTS shard_1.t_05786_null_str;

CREATE TABLE shard_0.t_05786_null_str (id UInt32, s Nullable(String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_null_str (id UInt32, s Nullable(String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_null_str AS shard_0.t_05786_null_str
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_null_str, cityHash64(assumeNotNull(s)));

INSERT INTO dist_05786_null_str SETTINGS distributed_foreground_insert = 1
VALUES (1, ''), (2, '\0'), (3, '\0\0'), (4, '\0\0\0'), (5, 'alpha'), (6, NULL);

SELECT '--- nullable string shards populated';
SELECT count() > 0 FROM shard_0.t_05786_null_str;
SELECT count() > 0 FROM shard_1.t_05786_null_str;

SELECT '--- nullable string empty-string pruning';
SELECT id FROM dist_05786_null_str WHERE s = '' ORDER BY id
SETTINGS optimize_skip_unused_shards = 0, optimize_empty_string_comparisons = 1;
SELECT id FROM dist_05786_null_str WHERE s = '' ORDER BY id
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 2, optimize_empty_string_comparisons = 1;
SELECT id FROM dist_05786_null_str WHERE s = '' ORDER BY id
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 2, optimize_empty_string_comparisons = 0;
SELECT id FROM dist_05786_null_str WHERE s IN ('') ORDER BY id
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 2;

SELECT '--- nullable string vs FixedString';
SELECT id, concat('[', hex(s), ']') FROM dist_05786_null_str WHERE s = toFixedString('', 1) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, concat('[', hex(s), ']') FROM dist_05786_null_str WHERE s = toFixedString('', 1) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;
SELECT id, concat('[', hex(s), ']') FROM dist_05786_null_str WHERE s IN (toFixedString('', 1)) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, concat('[', hex(s), ']') FROM dist_05786_null_str WHERE s IN (toFixedString('', 1)) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_null_str;
DROP TABLE shard_0.t_05786_null_str;
DROP TABLE shard_1.t_05786_null_str;

DROP TABLE IF EXISTS dist_05786_lc_null_str;
DROP TABLE IF EXISTS shard_0.t_05786_lc_null_str;
DROP TABLE IF EXISTS shard_1.t_05786_lc_null_str;

CREATE TABLE shard_0.t_05786_lc_null_str (id UInt32, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_lc_null_str (id UInt32, s LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_lc_null_str AS shard_0.t_05786_lc_null_str
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_lc_null_str, cityHash64(assumeNotNull(s)));

INSERT INTO dist_05786_lc_null_str SETTINGS distributed_foreground_insert = 1
VALUES (1, ''), (2, '\0'), (3, '\0\0'), (4, '\0\0\0'), (5, 'alpha'), (6, NULL);

SELECT '--- lc nullable string empty-string pruning and FixedString';
SELECT id FROM dist_05786_lc_null_str WHERE s = '' ORDER BY id
SETTINGS optimize_skip_unused_shards = 0, optimize_empty_string_comparisons = 1;
SELECT id FROM dist_05786_lc_null_str WHERE s = '' ORDER BY id
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 2, optimize_empty_string_comparisons = 1;
SELECT id FROM dist_05786_lc_null_str WHERE s = '' ORDER BY id
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 2, optimize_empty_string_comparisons = 0;
SELECT id FROM dist_05786_lc_null_str WHERE s IN ('') ORDER BY id
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 2;
SELECT id, concat('[', hex(s), ']') FROM dist_05786_lc_null_str WHERE s = toFixedString('', 1) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, concat('[', hex(s), ']') FROM dist_05786_lc_null_str WHERE s = toFixedString('', 1) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_lc_null_str;
DROP TABLE shard_0.t_05786_lc_null_str;
DROP TABLE shard_1.t_05786_lc_null_str;

DROP TABLE IF EXISTS dist_05786_str;
DROP TABLE IF EXISTS shard_0.t_05786_str;
DROP TABLE IF EXISTS shard_1.t_05786_str;

CREATE TABLE shard_0.t_05786_str (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_str (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_str AS shard_0.t_05786_str
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_str, cityHash64(s));

INSERT INTO dist_05786_str SETTINGS distributed_foreground_insert = 1
VALUES (1, ''), (2, '\0'), (3, '\0\0'), (4, '\0\0\0');

SELECT '--- non-nullable string vs FixedString';
SELECT id, concat('[', hex(s), ']') FROM dist_05786_str WHERE s = toFixedString('', 1) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, concat('[', hex(s), ']') FROM dist_05786_str WHERE s = toFixedString('', 1) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_str;
DROP TABLE shard_0.t_05786_str;
DROP TABLE shard_1.t_05786_str;

DROP TABLE IF EXISTS dist_05786_null_dec;
DROP TABLE IF EXISTS shard_0.t_05786_null_dec;
DROP TABLE IF EXISTS shard_1.t_05786_null_dec;

CREATE TABLE shard_0.t_05786_null_dec (id UInt32, d Nullable(Decimal128(18))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_null_dec (id UInt32, d Nullable(Decimal128(18))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_null_dec AS shard_0.t_05786_null_dec
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_null_dec, cityHash64(assumeNotNull(d)));

INSERT INTO dist_05786_null_dec SETTINGS distributed_foreground_insert = 1
VALUES (1, toDecimal128('1', 18)), (2, toDecimal128('1.000000000000000001', 18)), (3, toDecimal128('2', 18)), (4, NULL);

SELECT '--- nullable decimal128 shards populated and vs Float64';
SELECT count() > 0 FROM shard_0.t_05786_null_dec;
SELECT count() > 0 FROM shard_1.t_05786_null_dec;
SELECT id, toString(d) FROM dist_05786_null_dec WHERE d = 1.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, toString(d) FROM dist_05786_null_dec WHERE d = 1.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;
SELECT id, toString(d) FROM dist_05786_null_dec WHERE d IN (1.0) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, toString(d) FROM dist_05786_null_dec WHERE d IN (1.0) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_null_dec;
DROP TABLE shard_0.t_05786_null_dec;
DROP TABLE shard_1.t_05786_null_dec;

DROP TABLE IF EXISTS dist_05786_dec;
DROP TABLE IF EXISTS shard_0.t_05786_dec;
DROP TABLE IF EXISTS shard_1.t_05786_dec;

CREATE TABLE shard_0.t_05786_dec (id UInt32, d Decimal128(18)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_dec (id UInt32, d Decimal128(18)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_dec AS shard_0.t_05786_dec
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_dec, cityHash64(d));

INSERT INTO dist_05786_dec SETTINGS distributed_foreground_insert = 1
VALUES (1, toDecimal128('1', 18)), (2, toDecimal128('1.000000000000000001', 18)), (3, toDecimal128('2', 18));

SELECT '--- non-nullable decimal128 vs Float64';
SELECT id, toString(d) FROM dist_05786_dec WHERE d = 1.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, toString(d) FROM dist_05786_dec WHERE d = 1.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_dec;
DROP TABLE shard_0.t_05786_dec;
DROP TABLE shard_1.t_05786_dec;

DROP TABLE IF EXISTS dist_05786_null_flt;
DROP TABLE IF EXISTS shard_0.t_05786_null_flt;
DROP TABLE IF EXISTS shard_1.t_05786_null_flt;

CREATE TABLE shard_0.t_05786_null_flt (id UInt32, f Nullable(Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_null_flt (id UInt32, f Nullable(Float64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_null_flt AS shard_0.t_05786_null_flt
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_null_flt, cityHash64(assumeNotNull(f)));

INSERT INTO dist_05786_null_flt SETTINGS distributed_foreground_insert = 1
VALUES (1, 0.0), (2, -0.0), (3, 1.5);

SELECT '--- nullable float64 signed zero shards populated and vs zero';
SELECT count() > 0 FROM shard_0.t_05786_null_flt;
SELECT count() > 0 FROM shard_1.t_05786_null_flt;
SELECT id FROM dist_05786_null_flt WHERE f = 0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id FROM dist_05786_null_flt WHERE f = 0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;
SELECT id FROM dist_05786_null_flt WHERE f = 0.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id FROM dist_05786_null_flt WHERE f = 0.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;
SELECT id, reinterpretAsUInt64(assumeNotNull(f)) FROM dist_05786_null_flt WHERE f = toNullable(0.0) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, reinterpretAsUInt64(assumeNotNull(f)) FROM dist_05786_null_flt WHERE f = toNullable(0.0) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;
SELECT id, reinterpretAsUInt64(assumeNotNull(f)) FROM dist_05786_null_flt WHERE f = toNullable(-0.0) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, reinterpretAsUInt64(assumeNotNull(f)) FROM dist_05786_null_flt WHERE f = toNullable(-0.0) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_null_flt;
DROP TABLE shard_0.t_05786_null_flt;
DROP TABLE shard_1.t_05786_null_flt;

DROP TABLE IF EXISTS dist_05786_flt;
DROP TABLE IF EXISTS shard_0.t_05786_flt;
DROP TABLE IF EXISTS shard_1.t_05786_flt;

CREATE TABLE shard_0.t_05786_flt (id UInt32, f Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_flt (id UInt32, f Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_flt AS shard_0.t_05786_flt
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_flt, cityHash64(f));

INSERT INTO dist_05786_flt SETTINGS distributed_foreground_insert = 1
VALUES (1, 0.0), (2, -0.0), (3, 1.5);

SELECT '--- non-nullable float64 same-type and cross-type signed zero';
SELECT count() > 0 FROM shard_0.t_05786_flt;
SELECT count() > 0 FROM shard_1.t_05786_flt;
SELECT id, reinterpretAsUInt64(f) FROM dist_05786_flt WHERE f = 0.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id, reinterpretAsUInt64(f) FROM dist_05786_flt WHERE f = 0.0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;
SELECT id FROM dist_05786_flt WHERE f = 0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id FROM dist_05786_flt WHERE f = 0 ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_flt;
DROP TABLE shard_0.t_05786_flt;
DROP TABLE shard_1.t_05786_flt;

DROP TABLE IF EXISTS dist_05786_tuple_flt;
DROP TABLE IF EXISTS shard_0.t_05786_tuple_flt;
DROP TABLE IF EXISTS shard_1.t_05786_tuple_flt;

CREATE TABLE shard_0.t_05786_tuple_flt (id UInt32, t Tuple(Float64, UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_05786_tuple_flt (id UInt32, t Tuple(Float64, UInt32)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dist_05786_tuple_flt AS shard_0.t_05786_tuple_flt
ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05786_tuple_flt, cityHash64(t.1));

INSERT INTO dist_05786_tuple_flt SETTINGS distributed_foreground_insert = 1
VALUES (1, (0.0, 7)), (2, (-0.0, 7)), (3, (1.5, 7));

SELECT '--- tuple containing float64 same-type signed zero';
SELECT count() > 0 FROM shard_0.t_05786_tuple_flt;
SELECT count() > 0 FROM shard_1.t_05786_tuple_flt;
SELECT id FROM dist_05786_tuple_flt WHERE t = (0.0, toUInt32(7)) ORDER BY id
SETTINGS optimize_skip_unused_shards = 0;
SELECT id FROM dist_05786_tuple_flt WHERE t = (0.0, toUInt32(7)) ORDER BY id
SETTINGS optimize_skip_unused_shards = 1;

DROP TABLE dist_05786_tuple_flt;
DROP TABLE shard_0.t_05786_tuple_flt;
DROP TABLE shard_1.t_05786_tuple_flt;
