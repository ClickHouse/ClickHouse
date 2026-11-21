-- Rows-TTL bounds that a merge carries over without a rescan keep the fingerprint of the expression they
-- were computed under, which may differ from the current rows TTL. A column TTL that resets a column of
-- that older expression must drop their provenance, or a later MODIFY TTL back to that expression would
-- shift bounds that no longer describe the rows and delete live data.
SET alter_sync = 2, mutations_sync = 2;
SET allow_suspicious_ttl_expressions = 1;

DROP TABLE IF EXISTS materialize_ttl_column_ttl_old_expression;
CREATE TABLE materialize_ttl_column_ttl_old_expression
(
    id UInt32,
    d DateTime('UTC') DEFAULT now('UTC') + INTERVAL 1000 DAY TTL d + INTERVAL 1 DAY,
    e DateTime('UTC')
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 300 DAY
SETTINGS min_bytes_for_full_part_storage = 0, min_bytes_for_wide_part = 1000000000, materialize_ttl_recalculate_only = 0;

SYSTEM STOP TTL MERGES materialize_ttl_column_ttl_old_expression;
INSERT INTO materialize_ttl_column_ttl_old_expression VALUES (1, now('UTC') - INTERVAL 100 DAY, now('UTC') + INTERVAL 1000 DAY);
ALTER TABLE materialize_ttl_column_ttl_old_expression MODIFY TTL e + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START TTL MERGES materialize_ttl_column_ttl_old_expression;
-- Resets `d` of the row to its future default; the rows-TTL bounds of `d + INTERVAL 300 DAY` are kept.
OPTIMIZE TABLE materialize_ttl_column_ttl_old_expression FINAL;

SYSTEM STOP MERGES materialize_ttl_column_ttl_old_expression;
ALTER TABLE materialize_ttl_column_ttl_old_expression MODIFY TTL d + INTERVAL 300 DAY SETTINGS materialize_ttl_after_modify = 0;
INSERT INTO materialize_ttl_column_ttl_old_expression VALUES (2, now('UTC') - INTERVAL 100 DAY, now('UTC') + INTERVAL 1000 DAY);
ALTER TABLE materialize_ttl_column_ttl_old_expression MODIFY COLUMN d REMOVE TTL SETTINGS materialize_ttl_after_modify = 0;
SYSTEM START MERGES materialize_ttl_column_ttl_old_expression;
OPTIMIZE TABLE materialize_ttl_column_ttl_old_expression FINAL;

-- The row with id = 1 now has `d` in the future and must survive.
ALTER TABLE materialize_ttl_column_ttl_old_expression MODIFY TTL d + INTERVAL 10 DAY;
SELECT id FROM materialize_ttl_column_ttl_old_expression ORDER BY id;

DROP TABLE materialize_ttl_column_ttl_old_expression;
