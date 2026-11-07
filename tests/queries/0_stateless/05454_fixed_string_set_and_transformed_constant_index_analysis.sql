-- A `FixedString` in a set or a constant is compared zero-padded, while its conversion to the type of an index keeps
-- the padding. The index analysis must not prune the granules that match.

DROP TABLE IF EXISTS t_bloom_string;
DROP TABLE IF EXISTS t_bloom_array;
DROP TABLE IF EXISTS t_partition_hash;
DROP TABLE IF EXISTS t_order_hash;
DROP TABLE IF EXISTS t_partition_array;

SELECT 'bloom filter, IN with a FixedString set over a String column';
CREATE TABLE t_bloom_string (id UInt64, s String, INDEX idx s TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_bloom_string VALUES (1, 'a'), (2, 'b'), (3, 'a\0');
SELECT count() FROM t_bloom_string WHERE s IN (SELECT toFixedString('a', 2)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bloom_string WHERE s IN (SELECT toFixedString('a', 2)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bloom_string WHERE s IN (SELECT 'a') SETTINGS force_data_skipping_indices = 'idx';

SELECT 'bloom filter, IN with an Array(FixedString) set over an Array(String) column';
CREATE TABLE t_bloom_array (id UInt64, s Array(String), INDEX idx s TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_bloom_array VALUES (1, ['a']), (2, ['b']);
SELECT count() FROM t_bloom_array WHERE s IN (SELECT [toFixedString('a', 2)]) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_bloom_array WHERE s IN (SELECT [toFixedString('a', 2)]) SETTINGS use_skip_indexes = 1;

SELECT 'partition key over a hash of an array, a constant with a FixedString';
CREATE TABLE t_partition_hash (a Array(String)) ENGINE = MergeTree PARTITION BY cityHash64(a) ORDER BY tuple();
INSERT INTO t_partition_hash VALUES (['a']), (['b']);
SELECT count() FROM t_partition_hash WHERE a = [toFixedString('a', 2)]
SETTINGS use_partition_pruning = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_partition_hash WHERE a = [toFixedString('a', 2)];
SELECT count() FROM t_partition_hash WHERE a = ['a'];

SELECT 'primary key over a hash of an array, a constant with a FixedString';
CREATE TABLE t_order_hash (a Array(String)) ENGINE = MergeTree ORDER BY cityHash64(a) SETTINGS index_granularity = 1;
INSERT INTO t_order_hash VALUES (['a']), (['b']);
SELECT count() FROM t_order_hash WHERE a = [toFixedString('a', 2)] SETTINGS use_primary_key = 0;
SELECT count() FROM t_order_hash WHERE a = [toFixedString('a', 2)] SETTINGS use_primary_key = 1;

SELECT 'partition key over an array, IN with an Array(FixedString) set';
CREATE TABLE t_partition_array (a Array(String)) ENGINE = MergeTree PARTITION BY a ORDER BY tuple();
INSERT INTO t_partition_array VALUES (['a']), (['b']);
SELECT count() FROM t_partition_array WHERE a IN (SELECT [toFixedString('a', 2)])
SETTINGS use_partition_pruning = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_partition_array WHERE a IN (SELECT [toFixedString('a', 2)]);
SELECT count() FROM t_partition_array WHERE a IN (SELECT ['a']);

DROP TABLE t_bloom_string;
DROP TABLE t_bloom_array;
DROP TABLE t_partition_hash;
DROP TABLE t_order_hash;
DROP TABLE t_partition_array;
