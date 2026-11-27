-- A persistent table over `loop` would hold the source table's storage object forever,
-- so `DROP TABLE s_05493 SYNC` would never return. https://github.com/ClickHouse/ClickHouse/issues/119647

DROP TABLE IF EXISTS s_05493;

CREATE TABLE s_05493 (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO s_05493 VALUES (1), (2);

-- The transient form keeps working.
SELECT * FROM loop(currentDatabase(), s_05493) LIMIT 3;
SELECT * FROM loop(s_05493) LIMIT 3;

CREATE TABLE p_05493 AS loop(currentDatabase(), s_05493); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_05493 AS loop(s_05493); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_05493 AS loop(numbers(3)); -- { serverError BAD_ARGUMENTS }

DROP TABLE s_05493 SYNC;
