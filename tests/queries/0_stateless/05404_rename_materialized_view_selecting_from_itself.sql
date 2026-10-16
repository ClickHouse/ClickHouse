-- Renaming a materialized view that selects from itself and then pointing its query at the new name used to read freed memory.

CREATE TABLE src (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY tuple() AS SELECT c0 FROM src;

ALTER TABLE mv MODIFY QUERY SELECT c0 FROM mv;
SELECT dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'mv';

RENAME TABLE mv TO mv2;
-- This ALTER resolves a name conflict in the view dependency graph, which logs a warning.
SET send_logs_level = 'error';
ALTER TABLE mv2 MODIFY QUERY SELECT c0 FROM mv2;
SELECT dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'mv2';

DROP TABLE mv2;
DROP TABLE src;
