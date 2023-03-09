-- Tags: no-parallel

DROP TABLE IF EXISTS 25400_marked_dropped_tables;

CREATE TABLE 25400_marked_dropped_tables (id Int32) Engine=MergeTree() ORDER BY id;
DROP TABLE 25400_marked_dropped_tables;

SELECT table, engine FROM system.marked_dropped_tables LIMIT 1;
DESCRIBE TABLE system.marked_dropped_tables;
