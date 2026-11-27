-- Tags: atomic-database
-- The bounded read of an incremental refresh has a stream per subscription, with the partitions of the table divided
-- between them. A query that stops the read early (a `DISTINCT` by a constant) gets its row from one of the streams and
-- closes all of them, possibly before the round of another stream starts. Every stream still reports the cursor after
-- the rows of its snapshot, so a refresh without new rows in the source appends nothing.

DROP TABLE IF EXISTS esp_src;
DROP TABLE IF EXISTS esp_tgt;
DROP TABLE IF EXISTS esp_mv;

CREATE TABLE esp_src (p UInt64, k UInt64)
ENGINE = MergeTree ORDER BY k PARTITION BY p
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE esp_tgt (c UInt8) ENGINE = MergeTree ORDER BY c;

INSERT INTO esp_src SELECT number % 16, number FROM numbers(160000);

CREATE MATERIALIZED VIEW esp_mv REFRESH EVERY 10 YEAR APPEND INCREMENTAL TO esp_tgt EMPTY
    AS SELECT DISTINCT 1 AS c FROM esp_src
    SETTINGS max_block_size = 1, max_threads = 16;

SYSTEM REFRESH VIEW esp_mv;
SYSTEM WAIT VIEW esp_mv;
SELECT 'first', count() FROM esp_tgt;

-- No new rows in the source: nothing is appended, whichever streams were stopped before their rounds.
SYSTEM REFRESH VIEW esp_mv;
SYSTEM WAIT VIEW esp_mv;
SYSTEM REFRESH VIEW esp_mv;
SYSTEM WAIT VIEW esp_mv;
SYSTEM REFRESH VIEW esp_mv;
SYSTEM WAIT VIEW esp_mv;
SELECT 'idle', count() FROM esp_tgt;

-- New rows in one partition: one more row.
INSERT INTO esp_src VALUES (3, 1000000);
SYSTEM REFRESH VIEW esp_mv;
SYSTEM WAIT VIEW esp_mv;
SELECT 'new', count() FROM esp_tgt;
SYSTEM REFRESH VIEW esp_mv;
SYSTEM WAIT VIEW esp_mv;
SELECT 'idle', count() FROM esp_tgt;

DROP TABLE esp_mv;
DROP TABLE esp_tgt;
DROP TABLE esp_src;
