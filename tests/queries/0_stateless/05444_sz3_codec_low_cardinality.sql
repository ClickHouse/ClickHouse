-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

-- Test that a LowCardinality column cannot be compressed by a lossy codec like SZ3.

SET enable_sz3_codec = 1, allow_suspicious_low_cardinality_types = 1, enable_subcolumn_codecs = 1;
-- skip the "float codec on a non-float column" sanity check, which is not what is tested here
SET allow_suspicious_codecs = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_ephemeral;

CREATE TABLE tab (k UInt64, x LowCardinality(Float64) CODEC(SZ3)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (k UInt64, x LowCardinality(Float64) CODEC(SZ3, LZ4)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (k UInt64, x Array(LowCardinality(Float32)) CODEC(SZ3)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (k UInt64, x Tuple(a LowCardinality(Float64), b Float64) CODEC(SZ3)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (k UInt64, n Nested(a LowCardinality(Float64), b Float64) CODEC(SZ3)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE tab (k UInt64, x Tuple(a LowCardinality(Float64), b Float64), CODEC x.a (SZ3)) ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }

-- A lossy codec on a non-LowCardinality stream of the same column, and a lossless codec on LowCardinality, are allowed
CREATE TABLE tab (k UInt64, x Tuple(a LowCardinality(Float64), b Float64), y LowCardinality(Float64) CODEC(ZSTD), z Float64 CODEC(SZ3), CODEC x.b (SZ3)) ENGINE = MergeTree ORDER BY k;
INSERT INTO tab SELECT number, (number % 10, number), number % 10, number FROM numbers(1000);
SELECT count(), uniqExact(x.a), uniqExact(y) FROM tab;

ALTER TABLE tab MODIFY COLUMN z LowCardinality(Float64); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab MODIFY COLUMN y CODEC(SZ3); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab MODIFY COLUMN x.a CODEC(SZ3); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab ADD COLUMN w Array(LowCardinality(Float64)) CODEC(SZ3); -- { serverError BAD_ARGUMENTS }
SELECT count(), uniqExact(x.a), uniqExact(y) FROM tab;

DROP TABLE tab;

-- An EPHEMERAL column is not stored, so a lossy codec on it is allowed
CREATE TABLE tab_ephemeral (k UInt64, e LowCardinality(Float64) EPHEMERAL 0 CODEC(SZ3)) ENGINE = MergeTree ORDER BY k;
ALTER TABLE tab_ephemeral ADD COLUMN f LowCardinality(Float64) EPHEMERAL 0 CODEC(SZ3);
DROP TABLE tab_ephemeral;
