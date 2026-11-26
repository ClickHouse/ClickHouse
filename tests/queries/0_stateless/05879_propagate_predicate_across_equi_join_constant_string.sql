-- a comparison of a join key with a constant string is copied to the other table

SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS prop_cs_ts;
DROP TABLE IF EXISTS prop_cs_ts64;
DROP TABLE IF EXISTS prop_cs_date;
DROP TABLE IF EXISTS prop_cs_enum;
DROP TABLE IF EXISTS prop_cs_utc;
DROP TABLE IF EXISTS prop_cs_msk;

CREATE TABLE prop_cs_ts (k DateTime('UTC'), v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_cs_ts64 (k DateTime64(3, 'UTC'), v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_cs_date (k Date, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_cs_enum (k Enum8('x' = 1, 'y' = 2, 'z' = 3), v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_cs_utc (k DateTime('UTC'), v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE prop_cs_msk (k DateTime('Europe/Moscow'), v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO prop_cs_ts SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600, number FROM numbers(2000);
INSERT INTO prop_cs_ts64 SELECT toDateTime64('2020-01-01 00:00:00', 3, 'UTC') + number * 3600, number FROM numbers(2000);
INSERT INTO prop_cs_date SELECT toDate('2020-01-01') + number, number FROM numbers(2000);
INSERT INTO prop_cs_enum SELECT number % 3 + 1, number FROM numbers(2000);
INSERT INTO prop_cs_utc SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600, number FROM numbers(2000);
INSERT INTO prop_cs_msk SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + number * 3600, number FROM numbers(2000);

-- the second number is the count of tables that are read without a key condition: 0 = the filter is on both tables

SELECT 'DateTime',
    (SELECT count() FROM (SELECT * FROM prop_cs_ts WHERE k >= '2020-03-01 00:00:00') AS a INNER JOIN prop_cs_ts AS b ON a.k = b.k),
    (SELECT countIf(explain LIKE '%Condition: true%') FROM (EXPLAIN indexes = 1
        SELECT count() FROM (SELECT * FROM prop_cs_ts WHERE k >= '2020-03-01 00:00:00') AS a INNER JOIN prop_cs_ts AS b ON a.k = b.k));

SELECT 'DateTime64',
    (SELECT count() FROM (SELECT * FROM prop_cs_ts64 WHERE k >= '2020-03-01 00:00:00.000') AS a INNER JOIN prop_cs_ts64 AS b ON a.k = b.k),
    (SELECT countIf(explain LIKE '%Condition: true%') FROM (EXPLAIN indexes = 1
        SELECT count() FROM (SELECT * FROM prop_cs_ts64 WHERE k >= '2020-03-01 00:00:00.000') AS a INNER JOIN prop_cs_ts64 AS b ON a.k = b.k));

SELECT 'Date',
    (SELECT count() FROM (SELECT * FROM prop_cs_date WHERE k < '2020-03-01') AS a INNER JOIN prop_cs_date AS b ON a.k = b.k),
    (SELECT countIf(explain LIKE '%Condition: true%') FROM (EXPLAIN indexes = 1
        SELECT count() FROM (SELECT * FROM prop_cs_date WHERE k < '2020-03-01') AS a INNER JOIN prop_cs_date AS b ON a.k = b.k));

SELECT 'Enum',
    (SELECT count() FROM (SELECT * FROM prop_cs_enum WHERE k = 'y' AND v < 10) AS a INNER JOIN prop_cs_enum AS b ON a.k = b.k),
    (SELECT countIf(explain LIKE '%Condition: true%') FROM (EXPLAIN indexes = 1
        SELECT count() FROM (SELECT * FROM prop_cs_enum WHERE k = 'y' AND v < 10) AS a INNER JOIN prop_cs_enum AS b ON a.k = b.k));

SELECT 'LEFT JOIN',
    (SELECT count() FROM (SELECT * FROM prop_cs_ts WHERE k >= '2020-03-01 00:00:00') AS a LEFT JOIN prop_cs_ts AS b ON a.k = b.k WHERE b.v != 5000),
    (SELECT countIf(explain LIKE '%Condition: true%') FROM (EXPLAIN indexes = 1
        SELECT count() FROM (SELECT * FROM prop_cs_ts WHERE k >= '2020-03-01 00:00:00') AS a LEFT JOIN prop_cs_ts AS b ON a.k = b.k WHERE b.v != 5000));

-- the two keys read the string in different time zones, so it is not copied: one day in Moscow is 24 rows
SELECT 'time zones',
    (SELECT count() FROM (SELECT * FROM prop_cs_msk WHERE k >= '2020-03-02 00:00:00' AND k < '2020-03-03 00:00:00') AS a INNER JOIN prop_cs_utc AS b ON a.k = b.k),
    (SELECT countIf(explain LIKE '%Condition: true%') FROM (EXPLAIN indexes = 1
        SELECT count() FROM (SELECT * FROM prop_cs_msk WHERE k >= '2020-03-02 00:00:00' AND k < '2020-03-03 00:00:00') AS a INNER JOIN prop_cs_utc AS b ON a.k = b.k));

DROP TABLE prop_cs_ts;
DROP TABLE prop_cs_ts64;
DROP TABLE prop_cs_date;
DROP TABLE prop_cs_enum;
DROP TABLE prop_cs_utc;
DROP TABLE prop_cs_msk;
