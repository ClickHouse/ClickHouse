-- A full scan of a simple-key `hashed_array` dictionary returns every key once with its own attributes, in the same order
-- for any number of streams and block size, and stops early on LIMIT.

DROP DICTIONARY IF EXISTS dict_hashed_array;
DROP DICTIONARY IF EXISTS dict_sharded_hashed_array;
DROP DICTIONARY IF EXISTS dict_empty_hashed_array;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_empty;
DROP DICTIONARY IF EXISTS dict_big_hashed_array;
DROP TABLE IF EXISTS src_big;

CREATE TABLE src (id UInt64, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO src SELECT number * 7, number % 13 + 1 FROM numbers(1000);
CREATE TABLE src_empty (id UInt64, value UInt32) ENGINE = MergeTree ORDER BY id;

CREATE DICTIONARY dict_hashed_array (id UInt64, value UInt32)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(HASHED_ARRAY());

CREATE DICTIONARY dict_sharded_hashed_array (id UInt64, value UInt32)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src')) LIFETIME(0) LAYOUT(HASHED_ARRAY(SHARDS 4));

CREATE DICTIONARY dict_empty_hashed_array (id UInt64, value UInt32)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_empty')) LIFETIME(0) LAYOUT(HASHED_ARRAY());

SELECT 'source', count(), sum(cityHash64(id, value)), countIf(id = 0) FROM src;
SELECT 'hashed_array', count(), sum(cityHash64(id, value)), countIf(id = 0) FROM dict_hashed_array SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'block size 1', count(), sum(cityHash64(id, value)), countIf(id = 0) FROM dict_hashed_array SETTINGS max_block_size = 1, max_threads = 8;
SELECT 'sharded', count(), sum(cityHash64(id, value)), countIf(id = 0) FROM dict_sharded_hashed_array SETTINGS max_block_size = 7, max_threads = 4;

-- Splitting into blocks does not reorder the single-stream scan.
SELECT 'order', (SELECT groupArray(id) FROM dict_hashed_array SETTINGS max_threads = 1, max_block_size = 7)
    = (SELECT groupArray(id) FROM dict_hashed_array SETTINGS max_threads = 1, max_block_size = 1000000);
SELECT 'sharded order', (SELECT groupArray(id) FROM dict_sharded_hashed_array SETTINGS max_threads = 1, max_block_size = 7)
    = (SELECT groupArray(id) FROM dict_sharded_hashed_array SETTINGS max_threads = 1, max_block_size = 1000000);

-- Several streams, ordered by block number, give the single-stream order.
SELECT 'dictGetKeys order', dictGetKeys('dict_hashed_array', 'value', 3)
    = (SELECT groupArray(id) FROM dict_hashed_array WHERE value = 3 SETTINGS max_threads = 1)
SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'dictGetKeys', length(dictGetKeys('dict_sharded_hashed_array', 'value', 3)) SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'inverse lookup', count() FROM src WHERE dictGet('dict_hashed_array', 'value', id) = 3 SETTINGS max_block_size = 7, max_threads = 4, optimize_inverse_dictionary_lookup = 1;

SELECT 'limit', count() FROM (SELECT * FROM dict_hashed_array LIMIT 10) SETTINGS max_block_size = 7;

SELECT 'empty', count() FROM dict_empty_hashed_array SETTINGS max_block_size = 7, max_threads = 4;
SELECT 'empty dictGetKeys', dictGetKeys('dict_empty_hashed_array', 'value', 3);

DROP DICTIONARY dict_hashed_array;
DROP DICTIONARY dict_sharded_hashed_array;
DROP DICTIONARY dict_empty_hashed_array;
DROP TABLE src;
DROP TABLE src_empty;

-- A full scan does not keep all keys of the dictionary in memory at once.
DROP DICTIONARY IF EXISTS dict_big_hashed_array;
DROP TABLE IF EXISTS src_big;
CREATE TABLE src_big (id UInt64, value UInt32) ENGINE = Memory;
INSERT INTO src_big SELECT number, number % 13 FROM numbers(2000000);
CREATE DICTIONARY dict_big_hashed_array (id UInt64, value UInt32)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_big')) LIFETIME(0) LAYOUT(HASHED_ARRAY());
SYSTEM RELOAD DICTIONARY dict_big_hashed_array;
SELECT 'memory', count(), sum(id), sum(value) FROM dict_big_hashed_array SETTINGS max_block_size = 8192, max_threads = 4, max_memory_usage = '8Mi';
SELECT 'memory limit', count() FROM (SELECT * FROM dict_big_hashed_array LIMIT 10) SETTINGS max_block_size = 8192, max_threads = 4, max_memory_usage = '8Mi';
DROP DICTIONARY dict_big_hashed_array;
DROP TABLE src_big;
