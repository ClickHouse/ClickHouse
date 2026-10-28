-- Tags: no-ordinary-database, no-fasttest, use-rocksdb
-- Tag no-ordinary-database: KeeperMap doesn't support Ordinary database
-- Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so rocksdb engine is not enabled by default

-- A float equals a `Decimal`-like key when they compare equal as `Float64`, so a key lookup must find every key whose image equals the float.

DROP TABLE IF EXISTS kv_dec;
CREATE TABLE kv_dec (k Decimal(9, 2), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_dec VALUES (19.99, 1), (-19.99, 2), (12.5, 3), (0.29, 4);

SELECT 'k = 19.99', arraySort(groupArray(id)) FROM kv_dec WHERE k = 19.99;
SELECT 'k = -19.99', arraySort(groupArray(id)) FROM kv_dec WHERE k = -19.99;
SELECT 'k = 0.29', arraySort(groupArray(id)) FROM kv_dec WHERE k = 0.29;
SELECT 'k = 19.99 OR k = 12.5', arraySort(groupArray(id)) FROM kv_dec WHERE k = 19.99 OR k = 12.5;
SELECT 'k IN (SELECT 19.99)', arraySort(groupArray(id)) FROM kv_dec WHERE k IN (SELECT 19.99);
SELECT 'k = CAST(19.99 AS Dynamic)', arraySort(groupArray(id)) FROM kv_dec WHERE k = CAST(19.99 AS Dynamic);

SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_dec WHERE k = 12.5) WHERE explain ILIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_dec WHERE k IN (SELECT toFloat32(12.5))) WHERE explain ILIKE '%ReadType%';
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_dec WHERE k = 19.99) WHERE explain ILIKE '%ReadType%';

ALTER TABLE kv_dec DELETE WHERE k = 19.99 SETTINGS mutations_sync = 2;
SELECT 'ALTER DELETE', arraySort(groupArray(id)) FROM kv_dec;
DROP TABLE kv_dec;

DROP TABLE IF EXISTS kv_dec38;
CREATE TABLE kv_dec38 (k Decimal(38, 30), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_dec38 VALUES ('0.1', 1), ('0.1000000000000000055511151231257827', 2);
SELECT 'Decimal(38, 30)', arraySort(groupArray(id)) FROM kv_dec38 WHERE k = 0.1;
DROP TABLE kv_dec38;

DROP TABLE IF EXISTS kv_dt64;
CREATE TABLE kv_dt64 (k DateTime64(9, 'UTC'), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_dt64 VALUES ('2020-01-01 00:00:00.123456789', 1), ('2020-01-01 00:00:00.123456790', 2), ('2020-01-01 00:00:00.123456700', 3);
SELECT 'DateTime64(9)', arraySort(groupArray(id)) FROM kv_dt64 WHERE k = 1577836800.123456789;
DROP TABLE kv_dt64;

DROP TABLE IF EXISTS kv_tuple;
CREATE TABLE kv_tuple (k Tuple(Decimal(10, 2), UInt8), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_tuple VALUES ((19.99, 1), 1);
SELECT 'Tuple key =', arraySort(groupArray(id)) FROM kv_tuple WHERE k = (19.99, 1);
SELECT 'Tuple key IN', arraySort(groupArray(id)) FROM kv_tuple WHERE k IN (SELECT (19.99, 1));
DROP TABLE kv_tuple;

DROP TABLE IF EXISTS kv_array;
CREATE TABLE kv_array (k Array(Decimal(10, 2)), id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO kv_array VALUES ([19.99], 1);
SELECT 'Array key', arraySort(groupArray(id)) FROM kv_array WHERE k = [19.99];
DROP TABLE kv_array;

DROP TABLE IF EXISTS kv_multi;
CREATE TABLE kv_multi (a Decimal(10, 2), b UInt8, id UInt64) ENGINE = EmbeddedRocksDB PRIMARY KEY (a, b);
INSERT INTO kv_multi VALUES (19.99, 1, 1), (12.5, 1, 2);
SELECT 'a = 19.99 AND b = 1', arraySort(groupArray(id)) FROM kv_multi WHERE a = 19.99 AND b = 1;
SELECT '(a, b) = (19.99, 1)', arraySort(groupArray(id)) FROM kv_multi WHERE (a, b) = (19.99, 1);
SELECT '(a, b) IN (SELECT 19.99, 1)', arraySort(groupArray(id)) FROM kv_multi WHERE (a, b) IN (SELECT 19.99, 1);
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_multi WHERE (a, b) = (12.5, 1)) WHERE explain ILIKE '%ReadType%';
SELECT '(a, b) = (SELECT (19.99, 1))', arraySort(groupArray(id)) FROM kv_multi WHERE (a, b) = (SELECT (19.99, 1));
SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT id FROM kv_multi WHERE (a, b) = (SELECT (12.5, 1))) WHERE explain ILIKE '%ReadType%';
DROP TABLE kv_multi;

DROP TABLE IF EXISTS kv_keeper;
CREATE TABLE kv_keeper (k Decimal(9, 2), id UInt64) ENGINE = KeeperMap('/' || currentDatabase() || '/05457_kv_key_lookup_decimal_float_constant') PRIMARY KEY k;
INSERT INTO kv_keeper VALUES (19.99, 1);
SELECT 'KeeperMap', arraySort(groupArray(id)) FROM kv_keeper WHERE k = 19.99;
DROP TABLE kv_keeper;
