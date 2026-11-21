-- Tags: shard
-- `s = ''` is rewritten to `empty(s)`. Shard pruning must still see that constant.
-- With default weights, shard_num = cityHash64(value) % 2 + 1.
-- cityHash64('') % 2 + 1 = 2 and cityHash64('a') % 2 + 1 = 1 for String,
-- LowCardinality(String) and FixedString(2).
-- Shard 1 (shard_0) holds ids 10 and 11. Shard 2 (shard_1) holds ids 20 and 21.
-- A single value is read from the shard that owns it.
-- `s = '' OR s = 'a'` needs both shards, and each shard still applies the whole OR,
-- so every inserted row is returned.
-- `cityHash64` of a Nullable column is Nullable(UInt64) and is not a legal sharding key.
-- Nullable is covered by 05484_empty_string_read_in_order.

SET enable_analyzer = 1;
SET optimize_empty_string_comparisons = 1;
SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 2;

DROP TABLE IF EXISTS shard_0.t_128595_str;
DROP TABLE IF EXISTS shard_1.t_128595_str;
DROP TABLE IF EXISTS t_128595_str_dist;
DROP TABLE IF EXISTS shard_0.t_128595_lc;
DROP TABLE IF EXISTS shard_1.t_128595_lc;
DROP TABLE IF EXISTS t_128595_lc_dist;
DROP TABLE IF EXISTS shard_0.t_128595_fs;
DROP TABLE IF EXISTS shard_1.t_128595_fs;
DROP TABLE IF EXISTS t_128595_fs_dist;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

CREATE TABLE shard_0.t_128595_str (id UInt8, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_128595_str (id UInt8, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO shard_0.t_128595_str VALUES (10, ''), (11, 'a');
INSERT INTO shard_1.t_128595_str VALUES (20, ''), (21, 'a');
CREATE TABLE t_128595_str_dist AS shard_0.t_128595_str ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_128595_str, cityHash64(s));

CREATE TABLE shard_0.t_128595_lc (id UInt8, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_128595_lc (id UInt8, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO shard_0.t_128595_lc VALUES (10, ''), (11, 'a');
INSERT INTO shard_1.t_128595_lc VALUES (20, ''), (21, 'a');
CREATE TABLE t_128595_lc_dist AS shard_0.t_128595_lc ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_128595_lc, cityHash64(s));

CREATE TABLE shard_0.t_128595_fs (id UInt8, s FixedString(2)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE shard_1.t_128595_fs (id UInt8, s FixedString(2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO shard_0.t_128595_fs VALUES (10, ''), (11, 'a');
INSERT INTO shard_1.t_128595_fs VALUES (20, ''), (21, 'a');
CREATE TABLE t_128595_fs_dist AS shard_0.t_128595_fs ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_128595_fs, cityHash64(s));

SELECT 'string =';
SELECT id FROM t_128595_str_dist WHERE s = '' ORDER BY id;
SELECT 'string empty()';
SELECT id FROM t_128595_str_dist WHERE empty(s) ORDER BY id;
SELECT 'string reversed';
SELECT id FROM t_128595_str_dist WHERE '' = s ORDER BY id;
SELECT 'string a';
SELECT id FROM t_128595_str_dist WHERE s = 'a' ORDER BY id;
SELECT 'string or';
SELECT id FROM t_128595_str_dist WHERE s = '' OR s = 'a' ORDER BY id;

SELECT 'lc =';
SELECT id FROM t_128595_lc_dist WHERE s = '' ORDER BY id;
SELECT 'lc empty()';
SELECT id FROM t_128595_lc_dist WHERE empty(s) ORDER BY id;
SELECT 'lc reversed';
SELECT id FROM t_128595_lc_dist WHERE '' = s ORDER BY id;
SELECT 'lc a';
SELECT id FROM t_128595_lc_dist WHERE s = 'a' ORDER BY id;
SELECT 'lc or';
SELECT id FROM t_128595_lc_dist WHERE s = '' OR s = 'a' ORDER BY id;

SELECT 'fixed =';
SELECT id FROM t_128595_fs_dist WHERE s = '' ORDER BY id;
SELECT 'fixed empty()';
SELECT id FROM t_128595_fs_dist WHERE empty(s) ORDER BY id;
SELECT 'fixed a';
SELECT id FROM t_128595_fs_dist WHERE s = 'a' ORDER BY id;

-- `!= ''` is `notEmpty` and does not pin one shard.
SELECT id FROM t_128595_str_dist WHERE s != '' ORDER BY id; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }
SELECT id FROM t_128595_lc_dist WHERE s != '' ORDER BY id; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

DROP TABLE t_128595_str_dist;
DROP TABLE shard_0.t_128595_str;
DROP TABLE shard_1.t_128595_str;
DROP TABLE t_128595_lc_dist;
DROP TABLE shard_0.t_128595_lc;
DROP TABLE shard_1.t_128595_lc;
DROP TABLE t_128595_fs_dist;
DROP TABLE shard_0.t_128595_fs;
DROP TABLE shard_1.t_128595_fs;
