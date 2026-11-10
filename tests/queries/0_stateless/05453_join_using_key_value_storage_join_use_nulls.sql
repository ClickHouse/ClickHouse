-- Tags: use-rocksdb
-- JOIN USING with join_use_nulls onto a key-value table whose USING column type differs from the left one.

SET allow_suspicious_low_cardinality_types = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS l_i64;
DROP TABLE IF EXISTS l_str;
DROP TABLE IF EXISTS r_u32;
DROP TABLE IF EXISTS r_str;
DROP TABLE IF EXISTS r_str_mt;

CREATE TABLE l_i64 (k Int64) ENGINE = Memory;
INSERT INTO l_i64 VALUES (1), (2), (5);
CREATE TABLE l_str (k String) ENGINE = Memory;
INSERT INTO l_str VALUES ('a'), ('b'), ('x');

CREATE TABLE r_u32 (k LowCardinality(UInt32), v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO r_u32 VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE TABLE r_str (k LowCardinality(String), v UInt32) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO r_str VALUES ('a', 1), ('b', 2), ('c', 3);
CREATE TABLE r_str_mt (k LowCardinality(String), v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO r_str_mt VALUES ('a', 1), ('b', 2), ('c', 3);

SELECT k, toTypeName(k) FROM l_i64 FULL JOIN r_u32 USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT k, r.k, toTypeName(r.k), r.v FROM l_i64 AS l LEFT JOIN r_u32 AS r USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT k, toTypeName(k) FROM l_str FULL JOIN r_str USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT k, r.k, toTypeName(r.k), r.v FROM l_str AS l LEFT JOIN r_str AS r USING (k) ORDER BY k SETTINGS join_algorithm = 'hash';
SELECT k, r.k, toTypeName(r.k), r.v FROM l_str AS l LEFT JOIN r_str AS r USING (k) ORDER BY k SETTINGS join_algorithm = 'direct';
SELECT k, r.k, toTypeName(r.k), r.v FROM l_str AS l LEFT JOIN r_str_mt AS r USING (k) ORDER BY k SETTINGS join_algorithm = 'direct';

DROP TABLE l_i64;
DROP TABLE l_str;
DROP TABLE r_u32;
DROP TABLE r_str;
DROP TABLE r_str_mt;
