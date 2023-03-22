-- Tags: no-ordinary-database

SET database_atomic_wait_for_drop_and_detach_synchronously = 0;
DROP TABLE IF EXISTS 25400_marked_dropped_tables;

CREATE TABLE 25400_marked_dropped_tables (id Int32) Engine=MergeTree() ORDER BY id;
DROP TABLE 25400_marked_dropped_tables;

SELECT table, engine FROM system.marked_dropped_tables WHERE database = currentDatabase() LIMIT 1;
DESCRIBE TABLE system.marked_dropped_tables;
 
