-- Tags: no-replicated-database, no-shared-merge-tree
DROP TABLE IF EXISTS t_detach_empty_parts_cleanup;
CREATE TABLE t_detach_empty_parts_cleanup (p UInt8, k UInt32)
ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime = 0;

INSERT INTO t_detach_empty_parts_cleanup SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3);
ALTER TABLE t_detach_empty_parts_cleanup DETACH PART '1_1_1_0';
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_detach_empty_parts_cleanup' AND rows = 0;
ALTER TABLE t_detach_empty_parts_cleanup ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_detach_empty_parts_cleanup;

ALTER TABLE t_detach_empty_parts_cleanup DETACH PARTITION 1;
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_detach_empty_parts_cleanup' AND rows = 0;
ALTER TABLE t_detach_empty_parts_cleanup ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_detach_empty_parts_cleanup;

DROP TABLE t_detach_empty_parts_cleanup;
