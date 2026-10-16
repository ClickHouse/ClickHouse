CREATE TABLE src (c0 Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY tuple() AS SELECT c0 FROM src;

ALTER TABLE mv MODIFY QUERY SELECT c0 FROM mv; -- { serverError BAD_ARGUMENTS }
ALTER TABLE mv MODIFY QUERY SELECT c0 FROM (SELECT c0 FROM mv); -- { serverError BAD_ARGUMENTS }
SELECT dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'src';

DROP TABLE mv;
DROP TABLE src;
