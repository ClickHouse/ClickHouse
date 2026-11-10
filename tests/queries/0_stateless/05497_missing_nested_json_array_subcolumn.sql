-- A Nested member missing from a part is filled using the offsets of a member that is present.
-- Subcolumns with their own array dimensions must keep the declared structure, also in merges
-- of columns with dynamic structure (ORDER BY across parts and FINAL).

DROP TABLE IF EXISTS t_missing_nested_wide;
DROP TABLE IF EXISTS t_missing_nested_compact;

CREATE TABLE t_missing_nested_wide (id UInt64, raw Array(String), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_missing_nested_compact (id UInt64, raw Array(String), v UInt64)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;

SELECT 'Wide';

INSERT INTO t_missing_nested_wide VALUES (1, ['a', 'b'], 1);

ALTER TABLE t_missing_nested_wide ADD COLUMN `n.json` Array(JSON);
ALTER TABLE t_missing_nested_wide ADD COLUMN `n.typed` Array(JSON(a Array(UInt64)));
ALTER TABLE t_missing_nested_wide ADD COLUMN `n.variant` Array(Variant(String, Array(UInt64)));
ALTER TABLE t_missing_nested_wide ADD COLUMN `n.arr` Array(Array(UInt64));
ALTER TABLE t_missing_nested_wide ADD COLUMN `n.name` Array(String) MATERIALIZED raw;
ALTER TABLE t_missing_nested_wide MATERIALIZE COLUMN `n.name` SETTINGS mutations_sync = 2;

INSERT INTO t_missing_nested_wide (id, raw, `n.json`, `n.typed`, `n.variant`, `n.arr`, v)
SELECT 1, ['c'], ['{"a":[1,2]}'::JSON], ['{"a":[3]}'::JSON(a Array(UInt64))], [[4]::Array(UInt64)::Variant(String, Array(UInt64))], [[5]], 2;

SELECT v, n.json.a.:`Array(Dynamic)`, n.json.a.:`Array(Nullable(Int64))`, n.typed.a, n.variant.`Array(UInt64)`, n.arr
FROM t_missing_nested_wide ORDER BY v;

SELECT v, n.json.a.:`Array(Dynamic)`, n.json.a.:`Array(Nullable(Int64))`
FROM t_missing_nested_wide FINAL;

SELECT 'Compact';

INSERT INTO t_missing_nested_compact VALUES (1, ['a', 'b'], 1);

ALTER TABLE t_missing_nested_compact ADD COLUMN `n.json` Array(JSON);
ALTER TABLE t_missing_nested_compact ADD COLUMN `n.typed` Array(JSON(a Array(UInt64)));
ALTER TABLE t_missing_nested_compact ADD COLUMN `n.variant` Array(Variant(String, Array(UInt64)));
ALTER TABLE t_missing_nested_compact ADD COLUMN `n.arr` Array(Array(UInt64));
ALTER TABLE t_missing_nested_compact ADD COLUMN `n.name` Array(String) MATERIALIZED raw;
ALTER TABLE t_missing_nested_compact MATERIALIZE COLUMN `n.name` SETTINGS mutations_sync = 2;

INSERT INTO t_missing_nested_compact (id, raw, `n.json`, `n.typed`, `n.variant`, `n.arr`, v)
SELECT 1, ['c'], ['{"a":[1,2]}'::JSON], ['{"a":[3]}'::JSON(a Array(UInt64))], [[4]::Array(UInt64)::Variant(String, Array(UInt64))], [[5]], 2;

SELECT v, n.json.a.:`Array(Dynamic)`, n.json.a.:`Array(Nullable(Int64))`, n.typed.a, n.variant.`Array(UInt64)`, n.arr
FROM t_missing_nested_compact ORDER BY v;

SELECT v, n.json.a.:`Array(Dynamic)`, n.json.a.:`Array(Nullable(Int64))`
FROM t_missing_nested_compact FINAL;

DROP TABLE t_missing_nested_wide;
DROP TABLE t_missing_nested_compact;
