-- A filter on `a UInt8` must not reuse the query condition cache entry of a filter on `aU Int8`.
SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (k UInt64, a UInt8, aU Int8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100;
INSERT INTO tab SELECT number, intDiv(number, 100) = 9, intDiv(number, 100) = 0 FROM numbers(1000);

SELECT count(), min(k) FROM tab WHERE aU;
SELECT count(), min(k) FROM tab WHERE a;
SELECT count(), min(k) FROM tab PREWHERE aU;
SELECT count(), min(k) FROM tab PREWHERE a;

DROP TABLE tab;
