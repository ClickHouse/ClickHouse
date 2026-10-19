-- https://github.com/ClickHouse/ClickHouse/pull/126018#discussion_r4307283951

DROP TABLE IF EXISTS projection_null_check_drop_readd;
CREATE TABLE projection_null_check_drop_readd
(
    id UInt64,
    x Nullable(UInt8),
    PROJECTION p_null (SELECT countIf(isNull(x)), countIf(isNotNull(x))),
    PROJECTION p_id (SELECT sum(id))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO projection_null_check_drop_readd VALUES (1, NULL), (2, 1);
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS force_optimize_projection = 1, optimize_functions_to_subcolumns = 0;

-- Keep the old nullable states while the reading schema already has the new non-nullable column.
SYSTEM STOP MERGES projection_null_check_drop_readd;
ALTER TABLE projection_null_check_drop_readd DROP COLUMN x, ADD COLUMN x UInt8
SETTINGS alter_sync = 0;
SELECT count() > 0 FROM system.mutations
WHERE database = currentDatabase() AND table = 'projection_null_check_drop_readd' AND NOT is_done;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS optimize_use_projections = 0;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS prefer_optimize_projection = 1;
SYSTEM START MERGES projection_null_check_drop_readd;
DROP TABLE projection_null_check_drop_readd;

-- After synchronous completion the old states must be gone, not just rejected while the mutation is pending.
CREATE TABLE projection_null_check_drop_readd
(
    id UInt64,
    x Nullable(UInt8),
    PROJECTION p_null (SELECT countIf(isNull(x)), countIf(isNotNull(x))),
    PROJECTION p_id (SELECT sum(id))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO projection_null_check_drop_readd VALUES (1, NULL), (2, 1);
ALTER TABLE projection_null_check_drop_readd DROP COLUMN x, ADD COLUMN x UInt8
SETTINGS alter_sync = 2;
SELECT name, count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'projection_null_check_drop_readd' AND active
GROUP BY name ORDER BY name;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS optimize_use_projections = 0;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS prefer_optimize_projection = 1;

-- The declaration survives clearing: new inserts still build the projection.
SYSTEM STOP MERGES projection_null_check_drop_readd;
INSERT INTO projection_null_check_drop_readd VALUES (3, 7);
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'projection_null_check_drop_readd' AND active AND name = 'p_null';
SYSTEM START MERGES projection_null_check_drop_readd;
OPTIMIZE TABLE projection_null_check_drop_readd FINAL;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS optimize_use_projections = 0;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS prefer_optimize_projection = 1;
DROP TABLE projection_null_check_drop_readd;

-- Also invalidate dependencies that were already folded in the original non-nullable schema.
CREATE TABLE projection_null_check_drop_readd
(
    id UInt64,
    x UInt8,
    PROJECTION p_null (SELECT countIf(isNull(x)), countIf(isNotNull(x)))
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO projection_null_check_drop_readd VALUES (1, 1), (2, 2);
ALTER TABLE projection_null_check_drop_readd DROP COLUMN x, ADD COLUMN x Nullable(UInt8)
SETTINGS alter_sync = 2;
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'projection_null_check_drop_readd' AND active;
SELECT countIf(isNull(x)), countIf(isNotNull(x)) FROM projection_null_check_drop_readd
SETTINGS prefer_optimize_projection = 1;
DROP TABLE projection_null_check_drop_readd;
