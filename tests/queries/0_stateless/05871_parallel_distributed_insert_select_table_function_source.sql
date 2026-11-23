-- Tags: distributed

SET distributed_foreground_insert = 1;

CREATE TABLE dst (x UInt64) ENGINE = Memory;
CREATE TABLE src (number UInt64) ENGINE = Remote('127.0.0.1', numbers(10));
CREATE TABLE dst_remote (x UInt64) ENGINE = Remote('127.0.0.1', currentDatabase(), dst);

SELECT 'Remote over numbers';
INSERT INTO dst_remote SELECT number * 10 AS projected FROM src WHERE number % 2 = 0 SETTINGS parallel_distributed_insert_select = 0;
SELECT 0, arraySort(groupArray(x)) FROM dst;
TRUNCATE TABLE dst;
INSERT INTO dst_remote SELECT number * 10 AS projected FROM src WHERE number % 2 = 0 SETTINGS parallel_distributed_insert_select = 1;
SELECT 1, arraySort(groupArray(x)) FROM dst;
TRUNCATE TABLE dst;
INSERT INTO dst_remote SELECT number * 10 AS projected FROM src WHERE number % 2 = 0 SETTINGS parallel_distributed_insert_select = 2;
SELECT 2, arraySort(groupArray(x)) FROM dst;
TRUNCATE TABLE dst;

SELECT 'alias';
INSERT INTO dst_remote SELECT s.number + 100 FROM src AS s WHERE s.number < 3 SETTINGS parallel_distributed_insert_select = 2;
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), dst) SELECT r.number + 200 FROM remote('127.0.0.1', numbers(10)) AS r WHERE r.number < 3 SETTINGS parallel_distributed_insert_select = 2;
INSERT INTO dst_remote SELECT src.number + 300 FROM src WHERE src.number < 2 SETTINGS parallel_distributed_insert_select = 2;
INSERT INTO dst_remote SELECT src.* FROM src WHERE number = 5 SETTINGS parallel_distributed_insert_select = 2;
SELECT arraySort(groupArray(x)) FROM dst;
TRUNCATE TABLE dst;

SELECT 'view';
INSERT INTO FUNCTION remote('127.0.0.1', currentDatabase(), dst) SELECT number * 2 FROM remote('127.0.0.1', view(SELECT number FROM numbers(10))) WHERE number > 6 SETTINGS parallel_distributed_insert_select = 2;
SELECT arraySort(groupArray(x)) FROM dst;
TRUNCATE TABLE dst;

SELECT 'two shards';
INSERT INTO FUNCTION remote('127.0.0.{1,2}', currentDatabase(), dst, x) SELECT number * 10 AS x FROM remote('127.0.0.{1,2}', numbers(10)) WHERE number % 2 = 0 SETTINGS parallel_distributed_insert_select = 2;
SELECT arraySort(groupArray(x)) FROM dst;

DROP TABLE dst_remote;
DROP TABLE src;
DROP TABLE dst;
