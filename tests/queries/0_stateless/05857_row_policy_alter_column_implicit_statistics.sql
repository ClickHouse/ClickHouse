-- an explicit statistics type replaces the implicit one of the same type, also in the row policy check

CREATE TABLE t (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';
CREATE TABLE u (id UInt32, x UInt32) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';
INSERT INTO t VALUES (1, 1), (2, 0);
CREATE ROW POLICY p_05857 ON t FOR SELECT USING x > 0 TO CURRENT_USER;

ALTER TABLE t MODIFY COLUMN x UInt64, ADD STATISTICS x TYPE uniq_v2 SETTINGS mutations_sync = 2;
ALTER TABLE u MODIFY COLUMN x UInt64, ADD STATISTICS x TYPE uniq_v2 SETTINGS mutations_sync = 2;
SELECT name, type, statistics FROM system.columns WHERE database = currentDatabase() AND table IN ('t', 'u') AND name = 'x' ORDER BY table;
SELECT id FROM t;

DROP ROW POLICY p_05857 ON t;
DROP TABLE t;
DROP TABLE u;
