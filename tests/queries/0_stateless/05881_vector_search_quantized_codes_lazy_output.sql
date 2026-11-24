-- Tags: no-parallel-replicas
-- (the two-stage codes rewrite is disabled under parallel replicas, so the plan-shape assertions cannot hold there.)
-- With `vector_search_use_quantized_codes`, the shortlist reads the vector for its rows, and the other columns the query
-- returns are read for the final LIMIT rows only (`vector_search_lazy_materialization_after_rescoring`).

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;
SET vector_search_index_fetch_multiplier = 8;

DROP TABLE IF EXISTS tab_wide;
DROP TABLE IF EXISTS tab_compact;
DROP TABLE IF EXISTS tab_name_taken;

-- The columns are read lazily for the final rows only if the parts measure them as wide, which Compact parts do not.
CREATE TABLE tab_wide (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)), text String)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE tab_compact (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)), text String)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128, min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000;
-- A column named like the row index of the lazy read of the final rows.
CREATE TABLE tab_name_taken (id UInt32, __vector_search_row_index UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)), text String)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

-- Three parts each.
INSERT INTO tab_wide SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), concat('row-', toString(number), '-', repeat('x', 100)) FROM numbers(0, 1500);
INSERT INTO tab_wide SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), concat('row-', toString(number), '-', repeat('x', 100)) FROM numbers(1500, 1500);
INSERT INTO tab_wide SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)), concat('row-', toString(number), '-', repeat('x', 100)) FROM numbers(3000, 1000);
INSERT INTO tab_compact SELECT * FROM tab_wide WHERE id < 1500;
INSERT INTO tab_compact SELECT * FROM tab_wide WHERE id >= 1500 AND id < 3000;
INSERT INTO tab_compact SELECT * FROM tab_wide WHERE id >= 3000;
INSERT INTO tab_name_taken SELECT id, id, vec, text FROM tab_wide WHERE id < 1500;
INSERT INTO tab_name_taken SELECT id, id, vec, text FROM tab_wide WHERE id >= 1500 AND id < 3000;
INSERT INTO tab_name_taken SELECT id, id, vec, text FROM tab_wide WHERE id >= 3000;

SELECT 'Wide parts';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1
    SELECT id, left(text, 8) FROM tab_wide ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 5)
WHERE explain ILIKE '%quantized shortlist limit%' OR explain ILIKE '%Lazily read columns%';

SELECT 'Wide parts, setting disabled';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1
    SELECT id, left(text, 8) FROM tab_wide ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 5
    SETTINGS vector_search_lazy_materialization_after_rescoring = 0)
WHERE explain ILIKE '%Lazily read columns%';

SELECT 'Wide parts, shortlist of LIMIT rows';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1
    SELECT id, left(text, 8) FROM tab_wide ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 5
    SETTINGS vector_search_index_fetch_multiplier = 1)
WHERE explain ILIKE '%Lazily read columns%';

SELECT 'Wide parts, narrow returned columns';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1
    SELECT id FROM tab_wide ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 5)
WHERE explain ILIKE '%Lazily read columns%';

SELECT 'Compact parts';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1
    SELECT id, left(text, 8) FROM tab_compact ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 5)
WHERE explain ILIKE '%Lazily read columns%';

SELECT 'Name of the row index taken';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN actions = 1
    SELECT id, __vector_search_row_index, left(text, 8) FROM tab_name_taken ORDER BY L2Distance(vec, (SELECT vec FROM tab_name_taken WHERE id = 123)) LIMIT 5)
WHERE explain ILIKE '%Lazily read columns%';

SELECT 'Rows';
SELECT id, left(text, 8), round(L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)), 3) AS d FROM tab_wide
ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 5;
SELECT id, left(text, 8) FROM tab_wide WHERE id % 3 = 0 ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 3;
SELECT id, left(text, 8) FROM tab_wide ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 3 OFFSET 2;
SELECT id, left(text, 8), round(L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)), 3) AS d FROM tab_compact
ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 3;
SELECT id, left(text, 8) FROM tab_compact WHERE id % 3 = 0 ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 3;
SELECT id, left(text, 8) FROM tab_compact ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 3 OFFSET 2;
SELECT id, __vector_search_row_index, left(text, 8) FROM tab_name_taken ORDER BY L2Distance(vec, (SELECT vec FROM tab_name_taken WHERE id = 123)) LIMIT 3;

SELECT 'Rows, setting disabled';
SET vector_search_lazy_materialization_after_rescoring = 0;
SELECT id, left(text, 8), round(L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)), 3) AS d FROM tab_wide
ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 5;
SELECT id, left(text, 8) FROM tab_wide WHERE id % 3 = 0 ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 3;
SELECT id, left(text, 8) FROM tab_wide ORDER BY L2Distance(vec, (SELECT vec FROM tab_wide WHERE id = 123)) LIMIT 3 OFFSET 2;
SELECT id, left(text, 8), round(L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)), 3) AS d FROM tab_compact
ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 3;
SELECT id, left(text, 8) FROM tab_compact WHERE id % 3 = 0 ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 3;
SELECT id, left(text, 8) FROM tab_compact ORDER BY L2Distance(vec, (SELECT vec FROM tab_compact WHERE id = 123)) LIMIT 3 OFFSET 2;
SELECT id, __vector_search_row_index, left(text, 8) FROM tab_name_taken ORDER BY L2Distance(vec, (SELECT vec FROM tab_name_taken WHERE id = 123)) LIMIT 3;

DROP TABLE tab_wide;
DROP TABLE tab_compact;
DROP TABLE tab_name_taken;
