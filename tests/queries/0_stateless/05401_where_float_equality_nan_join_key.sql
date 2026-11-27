-- Random settings limits: query_plan_merge_filter_into_join_condition=(1, None)
-- `nan = nan` is false, so a WHERE equality drops a pair of NaNs, also when it is used as a JOIN key.
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS fa;
DROP TABLE IF EXISTS fb;
DROP TABLE IF EXISTS fc;

CREATE TABLE fa (k UInt8, f Float64, s Float32, n Nullable(Float64), t Tuple(UInt8, Float64), b BFloat16, lc LowCardinality(Float64), lcn LowCardinality(Nullable(Float64)), arr Array(Float64), mp Map(UInt8, Float64)) ENGINE = Memory;
CREATE TABLE fb AS fa;
CREATE TABLE fc AS fa;
INSERT INTO fa VALUES (1, 1, 1, 1, (1, 1), 1, 1, 1, [1], map(1, 1)), (2, nan, nan, nan, (2, nan), nan, nan, nan, [nan], map(1, nan)), (3, 3, 3, NULL, (3, 3), 3, 3, NULL, [3], map(1, 3));
INSERT INTO fb VALUES (1, 1, 1, 1, (1, 1), 1, 1, 1, [1], map(1, 1)), (2, nan, nan, nan, (2, nan), nan, nan, nan, [nan], map(1, nan)), (3, 4, 4, NULL, (3, 4), 4, 4, NULL, [4], map(1, 4));
INSERT INTO fc SELECT * FROM fa;

SELECT '-- comma and CROSS JOIN';
SELECT fa.k, fb.k FROM fa, fb WHERE fa.f = fb.f ORDER BY ALL;
SELECT fa.k, fb.k FROM fa, fb WHERE fa.f = fb.f ORDER BY ALL SETTINGS cross_to_inner_join_rewrite = 0;
SELECT fa.k, fb.k FROM fa CROSS JOIN fb WHERE fa.f = fb.f ORDER BY ALL;

SELECT '-- INNER JOIN with a WHERE equality';
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.f = fb.f ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.s = fb.s ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.n = fb.n ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.t = fb.t ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.b = fb.b ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.lc = fb.lc ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.lcn = fb.lcn ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.s = fb.f ORDER BY ALL;
SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.f = fb.f ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';

SELECT '-- correlated EXISTS';
SELECT k FROM fa WHERE EXISTS (SELECT 1 FROM fb WHERE fb.f = fa.f) ORDER BY ALL;

SELECT '-- three tables';
SELECT fa.k, fb.k, fc.k FROM fa, fb, fc WHERE fa.f = fb.f AND fb.k = fc.k ORDER BY ALL;

SELECT '-- the equality is a JOIN key and also stays a filter';
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain ILIKE '%filter column:%f = f%') > 0 AS has_equality_filter
FROM (EXPLAIN actions = 1 SELECT fa.k, fb.k FROM fa, fb WHERE fa.f = fb.f);
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain ILIKE '%filter column:%f = f%') > 0 AS has_equality_filter
FROM (EXPLAIN actions = 1 SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.f = fb.f);
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain ILIKE '%filter column:%f = f%') > 0 AS has_equality_filter
FROM (EXPLAIN actions = 1 SELECT fa.k, fb.k FROM fa, fb WHERE fa.k = fb.k);
SELECT
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Type: (\\w+)') AS join_kind,
    extract(arrayStringConcat(groupArray(explain), '\n'), 'Join conditions: ([^\n]*)') AS join_conditions,
    countIf(explain ILIKE '%filter column:%arr = arr%' OR explain ILIKE '%filter column:%mp = mp%') > 0 AS has_equality_filter
FROM (EXPLAIN actions = 1 SELECT fa.k, fb.k FROM fa INNER JOIN fb ON fa.k = fb.k WHERE fa.arr = fb.arr AND fa.mp = fb.mp);

DROP TABLE fa;
DROP TABLE fb;
DROP TABLE fc;
