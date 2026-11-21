-- A `UUID2` key of a nested map in `SummingMergeTree` is summed the same way as a `UUID` key:
-- both with `FINAL` and after a physical merge.

DROP TABLE IF EXISTS t_uuid2_summing;
DROP TABLE IF EXISTS t_uuid1_summing;

CREATE TABLE t_uuid2_summing (k UInt64, n UInt64, SomeMap Nested(ID UUID2, Num UInt64)) ENGINE = SummingMergeTree ORDER BY k;
CREATE TABLE t_uuid1_summing (k UInt64, n UInt64, SomeMap Nested(ID UUID1, Num UInt64)) ENGINE = SummingMergeTree ORDER BY k;

SYSTEM STOP MERGES t_uuid2_summing;
SYSTEM STOP MERGES t_uuid1_summing;

INSERT INTO t_uuid2_summing VALUES (0, 1, ['00000000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000000'], [100, 1]);
INSERT INTO t_uuid2_summing VALUES (0, 1, ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002'], [200, 10]);
INSERT INTO t_uuid1_summing VALUES (0, 1, ['00000000-0000-0000-0000-000000000001', '10000000-0000-0000-0000-000000000000'], [100, 1]);
INSERT INTO t_uuid1_summing VALUES (0, 1, ['00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002'], [200, 10]);

SELECT 'FINAL';
SELECT n, SomeMap.ID, SomeMap.Num FROM t_uuid2_summing FINAL;
SELECT n, arraySort(x -> toString(x.1), arrayZip(SomeMap.ID, SomeMap.Num)) FROM t_uuid1_summing FINAL;

SYSTEM START MERGES t_uuid2_summing;
SYSTEM START MERGES t_uuid1_summing;
OPTIMIZE TABLE t_uuid2_summing FINAL;
OPTIMIZE TABLE t_uuid1_summing FINAL;

SELECT 'merged';
SELECT n, SomeMap.ID, SomeMap.Num FROM t_uuid2_summing;
SELECT n, arraySort(x -> toString(x.1), arrayZip(SomeMap.ID, SomeMap.Num)) FROM t_uuid1_summing;

DROP TABLE t_uuid2_summing;
DROP TABLE t_uuid1_summing;
