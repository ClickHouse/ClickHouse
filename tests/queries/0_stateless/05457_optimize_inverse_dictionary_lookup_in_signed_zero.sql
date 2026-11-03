-- `IN` sets compare floating-point values by their binary representation, so `-0.0` does not match
-- the attribute `DEFAULT` `+0.0`. The rewrite must classify the `DEFAULT` the same way.

DROP DICTIONARY IF EXISTS d_float_signed_zero;
DROP TABLE IF EXISTS t_float_signed_zero;

CREATE TABLE t_float_signed_zero (id UInt64, v Float64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_float_signed_zero VALUES (1, 2.5), (2, 1.0);

CREATE DICTIONARY d_float_signed_zero (id UInt64, v Float64 DEFAULT 0)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 't_float_signed_zero'))
LAYOUT(HASHED())
LIFETIME(0);

SELECT 'IN';
SELECT number, dictGet('d_float_signed_zero', 'v', number) IN (-0.0, 1.0) AS r
FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT number, dictGet('d_float_signed_zero', 'v', number) IN (-0.0, 1.0) AS r
FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'NOT IN';
SELECT number, dictGet('d_float_signed_zero', 'v', number) NOT IN (-0.0, 1.0) AS r
FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT number, dictGet('d_float_signed_zero', 'v', number) NOT IN (-0.0, 1.0) AS r
FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'IN with +0.0';
SELECT number, dictGet('d_float_signed_zero', 'v', number) IN (0.0, 1.0) AS r
FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT number, dictGet('d_float_signed_zero', 'v', number) IN (0.0, 1.0) AS r
FROM numbers(3) ORDER BY number SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP DICTIONARY d_float_signed_zero;
DROP TABLE t_float_signed_zero;

DROP DICTIONARY IF EXISTS d_lc_signed_zero;
DROP TABLE IF EXISTS src_lc_signed_zero;
CREATE TABLE src_lc_signed_zero (id UInt64, val Float64) ENGINE = Memory AS SELECT 1, -0.0 UNION ALL SELECT 2, 0.0;
CREATE DICTIONARY d_lc_signed_zero (id UInt64, val Float64) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_lc_signed_zero')) LAYOUT(FLAT()) LIFETIME(0);
SELECT id FROM (SELECT toUInt64(1) AS id UNION ALL SELECT toUInt64(2)) WHERE CAST(dictGet('d_lc_signed_zero', 'val', id), 'LowCardinality(Float64)') = 0.0 ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;
DROP DICTIONARY d_lc_signed_zero;
DROP TABLE src_lc_signed_zero;
