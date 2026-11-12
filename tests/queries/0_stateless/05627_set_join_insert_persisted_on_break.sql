-- An INSERT into a persistent Set or Join table broken off by `timeout_overflow_mode = 'break'` succeeds,
-- so the rows it made visible must survive DETACH/ATTACH.
-- The source never ends, so the INSERT is always broken off.

DROP TABLE IF EXISTS s_05627;
DROP TABLE IF EXISTS j_05627;
DROP TABLE IF EXISTS counts_05627;
CREATE TABLE s_05627 (x UInt64) ENGINE = Set;
CREATE TABLE j_05627 (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
CREATE TABLE counts_05627 (name String, c UInt64) ENGINE = Memory;

INSERT INTO s_05627 SELECT number FROM system.numbers WHERE sleepEachRow(0.001) = 0
SETTINGS max_block_size = 100, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
    max_execution_time = 1, timeout_overflow_mode = 'break', max_rows_to_read = 0, max_bytes_to_read = 0;

INSERT INTO j_05627 SELECT number, number FROM system.numbers WHERE sleepEachRow(0.001) = 0
SETTINGS max_block_size = 100, max_threads = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
    max_execution_time = 1, timeout_overflow_mode = 'break', max_rows_to_read = 0, max_bytes_to_read = 0;

INSERT INTO counts_05627 SELECT 'set', count() FROM numbers(1000000) WHERE number IN s_05627;
INSERT INTO counts_05627 SELECT 'join', count() FROM j_05627;

DETACH TABLE s_05627;
ATTACH TABLE s_05627;
DETACH TABLE j_05627;
ATTACH TABLE j_05627;

SELECT name, c > 0, c = (SELECT count() FROM numbers(1000000) WHERE number IN s_05627) FROM counts_05627 WHERE name = 'set';
SELECT name, c > 0, c = (SELECT count() FROM j_05627) FROM counts_05627 WHERE name = 'join';

DROP TABLE s_05627;
DROP TABLE j_05627;
DROP TABLE counts_05627;
