-- Random settings limits: query_plan_merge_filters=(1, 1)

-- A bare column in a WHERE AND chain guards the conjuncts written after it, also when the chain is split
-- into several filters: intDiv(1, x) must not be evaluated on the row the column rejects.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_sparse;

CREATE TABLE tab (g UInt8, n Nullable(Bool), l LowCardinality(UInt8), i Int32, f Float64, x Int32, b String) ENGINE = Log;
INSERT INTO tab VALUES (0, NULL, 0, 0, 0, 0, 'true'), (1, true, 3, -2, 0.5, 1, 'true');

SELECT g FROM tab WHERE g AND intDiv(1, x) > 0 AND toBool(b);
SELECT n FROM tab WHERE n AND intDiv(1, x) > 0 AND toBool(b);
SELECT l FROM tab WHERE l AND intDiv(1, x) > 0 AND toBool(b);
SELECT i FROM tab WHERE i AND intDiv(1, x) > 0 AND toBool(b);
SELECT f FROM tab WHERE f AND intDiv(1, x) > 0 AND toBool(b);

SELECT count() FROM tab WHERE x >= 0 AND g AND intDiv(1, x) > 0 AND toBool(b);
SELECT count() FROM (SELECT * FROM tab WHERE g) WHERE intDiv(1, x) > 0 AND toBool(b);

CREATE TABLE tab_sparse (g UInt8, x Int32, b String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO tab_sparse SELECT number = 9, number = 9, 'true' FROM numbers(10);
SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab_sparse' AND column = 'g';
SELECT count() FROM tab_sparse WHERE g AND intDiv(1, x) > 0 AND toBool(b)
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

SELECT trimLeft(explain) FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM tab WHERE g AND intDiv(1, x) > 0 AND toBool(b))
WHERE explain ILIKE '%AND column:%';

SELECT count() FROM tab WHERE intDiv(1, x) > 0 AND toBool(b) AND g; -- { serverError ILLEGAL_DIVISION }

DROP TABLE tab;
DROP TABLE tab_sparse;
