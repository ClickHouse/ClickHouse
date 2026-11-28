-- Files with equal size and checksum are stored once. In an incremental backup they can still differ in how
-- much of them the base backup has: `a*` and `z*` below get the same contents, but only one table of each
-- pair is in the base backup, so only that table's files are written as a suffix of their base version.

CREATE TABLE z1 (x UInt64) ENGINE = Log;
CREATE TABLE a2 (x UInt64) ENGINE = Log;
CREATE TABLE z3 (x UInt64) ENGINE = Log;
CREATE TABLE a4 (x UInt64) ENGINE = Log;
INSERT INTO z1 SELECT number + 1000 FROM numbers(50);
INSERT INTO a2 SELECT number + 2000 FROM numbers(50);
INSERT INTO z3 SELECT number + 3000 FROM numbers(50);
INSERT INTO a4 SELECT number + 4000 FROM numbers(50);
BACKUP TABLE z1, TABLE a2, TABLE z3, TABLE a4 TO Disk('backups', {CLICKHOUSE_DATABASE_1:String}) FORMAT Null;

INSERT INTO z1 SELECT number + 1000 FROM numbers(50, 50);
INSERT INTO a2 SELECT number + 2000 FROM numbers(50, 50);
INSERT INTO z3 SELECT number + 3000 FROM numbers(50, 50);
INSERT INTO a4 SELECT number + 4000 FROM numbers(50, 50);

CREATE TABLE a1 (x UInt64) ENGINE = Log;
CREATE TABLE z2 (x UInt64) ENGINE = Log;
CREATE TABLE a3 (x UInt64) ENGINE = Log;
CREATE TABLE z4 (x UInt64) ENGINE = Log;
INSERT INTO a1 SELECT number + 1000 FROM numbers(50);
INSERT INTO a1 SELECT number + 1000 FROM numbers(50, 50);
INSERT INTO z2 SELECT number + 2000 FROM numbers(50);
INSERT INTO z2 SELECT number + 2000 FROM numbers(50, 50);
INSERT INTO a3 SELECT number + 3000 FROM numbers(50);
INSERT INTO a3 SELECT number + 3000 FROM numbers(50, 50);
INSERT INTO z4 SELECT number + 4000 FROM numbers(50);
INSERT INTO z4 SELECT number + 4000 FROM numbers(50, 50);

BACKUP TABLE a1, TABLE z1, TABLE a2, TABLE z2, TABLE a3, TABLE z3, TABLE a4, TABLE z4 TO Disk('backups', {CLICKHOUSE_DATABASE_2:String})
    SETTINGS base_backup = Disk('backups', {CLICKHOUSE_DATABASE_1:String}) FORMAT Null;

RESTORE TABLE a1 AS a1_r, TABLE z1 AS z1_r, TABLE a2 AS a2_r, TABLE z2 AS z2_r, TABLE a3 AS a3_r, TABLE z3 AS z3_r, TABLE a4 AS a4_r, TABLE z4 AS z4_r
    FROM Disk('backups', {CLICKHOUSE_DATABASE_2:String}) FORMAT Null;

SELECT 'a1', count(), sum(x) FROM a1_r;
SELECT 'z1', count(), sum(x) FROM z1_r;
SELECT 'a2', count(), sum(x) FROM a2_r;
SELECT 'z2', count(), sum(x) FROM z2_r;
SELECT 'a3', count(), sum(x) FROM a3_r;
SELECT 'z3', count(), sum(x) FROM z3_r;
SELECT 'a4', count(), sum(x) FROM a4_r;
SELECT 'z4', count(), sum(x) FROM z4_r;
