-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- Tag no-ordinary-database: KeeperMap doesn't support Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

-- `-0.` and `0.` are equal but are stored as different keys, so a key lookup by a floating-point zero must find both rows.

DROP TABLE IF EXISTS kv_float;
CREATE TABLE kv_float (k Float64, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_float VALUES (-0., 0), (0., 1), (5., 2);

SELECT 'k = 0', arraySort(groupArray(id)) FROM kv_float WHERE k = 0;
SELECT 'k = -0.', arraySort(groupArray(id)) FROM kv_float WHERE k = -0.;
SELECT 'k = 0 OR k = 5', arraySort(groupArray(id)) FROM kv_float WHERE k = 0 OR k = 5;
SELECT 'k IN (0., -0.)', arraySort(groupArray(id)) FROM kv_float WHERE k IN (0., -0.);

SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_float WHERE k = 0) WHERE explain ILIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_float WHERE k = 5) WHERE explain ILIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_float WHERE k IN (1., 5.)) WHERE explain ILIKE '%ReadType%';

ALTER TABLE kv_float DELETE WHERE k = 0 SETTINGS mutations_sync = 2;
SELECT 'ALTER DELETE', arraySort(groupArray(id)) FROM kv_float;

TRUNCATE TABLE kv_float;
INSERT INTO kv_float VALUES (-0., 0), (0., 1), (5., 2);
ALTER TABLE kv_float UPDATE id = id + 100 WHERE k = 0 SETTINGS mutations_sync = 2;
SELECT 'ALTER UPDATE', arraySort(groupArray(id)) FROM kv_float;

DROP TABLE kv_float;

DROP TABLE IF EXISTS kv_float32;
CREATE TABLE kv_float32 (k Float32, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_float32 VALUES (-0., 0), (0., 1);
SELECT 'Float32', arraySort(groupArray(id)) FROM kv_float32 WHERE k = 0;
DROP TABLE kv_float32;

DROP TABLE IF EXISTS kv_multi;
CREATE TABLE kv_multi (k1 Float64, k2 UInt8, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (k1, k2);
INSERT INTO kv_multi VALUES (-0., 1, 0), (0., 1, 1);
SELECT 'k1 = 0 AND k2 = 1', arraySort(groupArray(id)) FROM kv_multi WHERE k1 = 0 AND k2 = 1;
SELECT '(k1, k2) = (0., 1)', arraySort(groupArray(id)) FROM kv_multi WHERE (k1, k2) = (0., 1);
SELECT '(k1, k2) IN ((0., 1), (-0., 1))', arraySort(groupArray(id)) FROM kv_multi WHERE (k1, k2) IN ((0., 1), (-0., 1));
DROP TABLE kv_multi;

DROP TABLE IF EXISTS kv_tuple;
CREATE TABLE kv_tuple (k Tuple(Float64, UInt8), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_tuple VALUES ((-0., 1), 0), ((0., 1), 1);
SELECT 'Tuple', arraySort(groupArray(id)) FROM kv_tuple WHERE k = (0., 1);
DROP TABLE kv_tuple;

DROP TABLE IF EXISTS kv_array;
CREATE TABLE kv_array (k Array(Float64), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_array VALUES ([-0.], 0), ([0.], 1);
SELECT 'Array', arraySort(groupArray(id)) FROM kv_array WHERE k = [0.];
DROP TABLE kv_array;

DROP TABLE IF EXISTS kv_map;
CREATE TABLE kv_map (k Map(String, Float64), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_map VALUES (map('a', -0.), 0), (map('a', 0.), 1);
SELECT 'Map', arraySort(groupArray(id)) FROM kv_map WHERE k = map('a', 0.);
DROP TABLE kv_map;

DROP TABLE IF EXISTS kv_keeper_map;
CREATE TABLE kv_keeper_map (k Float64, id UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/05454_kv_keeper_map') PRIMARY KEY k;
INSERT INTO kv_keeper_map VALUES (-0., 0), (0., 1), (5., 2);
SELECT 'KeeperMap k = 0', arraySort(groupArray(id)) FROM kv_keeper_map WHERE k = 0;
ALTER TABLE kv_keeper_map DELETE WHERE k = 0 SETTINGS mutations_sync = 2;
SELECT 'KeeperMap ALTER DELETE', arraySort(groupArray(id)) FROM kv_keeper_map;
DROP TABLE kv_keeper_map;
