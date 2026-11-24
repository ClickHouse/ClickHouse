-- Tags: atomic-database
-- An incremental refresh resumes from the cursor that the bounded read reports at its end. A step of the query may
-- stop reading before that, when it needs no more rows (`DISTINCT` over constants passes one row and closes its input).
-- The source is read to the end anyway, so the cursor advances and the next refresh does not append the same rows again.

DROP TABLE IF EXISTS early_src;
DROP TABLE IF EXISTS early_tgt;
DROP TABLE IF EXISTS early_mv;

CREATE TABLE early_src (k UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE early_tgt (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO early_src SELECT number FROM numbers(100000);

CREATE MATERIALIZED VIEW early_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO early_tgt EMPTY
    AS SELECT DISTINCT toUInt64(1) AS k FROM early_src
    SETTINGS max_threads = 1, max_block_size = 1;

SYSTEM REFRESH VIEW early_mv;
SYSTEM WAIT VIEW early_mv;
SELECT 'first refresh', count(), groupArray(k) FROM early_tgt;

-- The source has not changed: nothing is appended.
SYSTEM REFRESH VIEW early_mv;
SYSTEM WAIT VIEW early_mv;
SELECT 'unchanged source', count(), groupArray(k) FROM early_tgt;

-- Only the new rows are read.
INSERT INTO early_src SELECT number FROM numbers(100000, 1000);
SYSTEM REFRESH VIEW early_mv;
SYSTEM WAIT VIEW early_mv;
SELECT 'new rows', count(), groupArray(k) FROM early_tgt;

SYSTEM REFRESH VIEW early_mv;
SYSTEM WAIT VIEW early_mv;
SELECT 'unchanged source', count(), groupArray(k) FROM early_tgt;

DROP TABLE early_mv;
DROP TABLE early_tgt;
DROP TABLE early_src;
