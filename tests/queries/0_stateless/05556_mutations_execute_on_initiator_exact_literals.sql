SET mutations_sync = 2, enable_time_time64_type = 1;
SET mutations_execute_subqueries_on_initiator = 1, mutations_execute_nondeterministic_on_initiator = 1;

DROP TABLE IF EXISTS t_exact_literals;
DROP DICTIONARY IF EXISTS t_exact_literals_dict;
DROP TABLE IF EXISTS t_exact_literals_dict_src;
DROP TABLE IF EXISTS t_exact_literals_variant;
DROP TABLE IF EXISTS t_exact_literals_json;
DROP TABLE IF EXISTS t_exact_literals_join;
DROP TABLE IF EXISTS t_exact_literals_join_dst;

CREATE TABLE t_exact_literals (k UInt8, d DateTime64(3, 'UTC'), n Nullable(DateTime64(9, 'UTC')), t Time64(1), x Decimal(9, 2), y Decimal(76, 0), v UInt64 DEFAULT 0)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exact_literals (k, d, n, t, x, y) VALUES (1, '1950-01-01 00:00:00', '1900-01-01 00:00:00.000000001', -3600.5, 1, 1),
    (2, '2290-01-01 00:00:00', NULL, 10, 2, 2), (3, '2024-01-01 00:00:00', '2024-01-01 00:00:00', 20, 3, 3);

ALTER TABLE t_exact_literals UPDATE v = v + 1 WHERE d = (SELECT toDateTime64('1950-01-01 00:00:00', 3, 'UTC'));
ALTER TABLE t_exact_literals UPDATE v = v + 2 WHERE d = (SELECT toDateTime64('2290-01-01 00:00:00', 3, 'UTC'));
ALTER TABLE t_exact_literals UPDATE v = v + 4 WHERE n = (SELECT toDateTime64('1900-01-01 00:00:00.000000001', 9, 'UTC'));
ALTER TABLE t_exact_literals UPDATE v = v + 8 WHERE t = (SELECT toTime64(-3600.5, 1));
ALTER TABLE t_exact_literals UPDATE v = v + 16 WHERE (k, d) = (SELECT 1, toDateTime64('1950-01-01 00:00:00', 3, 'UTC'));
ALTER TABLE t_exact_literals UPDATE v = v + 32 WHERE has((SELECT [toDateTime64('2290-01-01 00:00:00', 3, 'UTC')]), d);
ALTER TABLE t_exact_literals UPDATE v = v + 64 WHERE (SELECT map('a', toDateTime64('1950-01-01 00:00:00', 3, 'UTC')))['a'] = d;
ALTER TABLE t_exact_literals UPDATE v = v + 128 WHERE ((SELECT toDateTime64('1950-01-01 00:00:00', 3, 'UTC')) AS s) = d AND s < '1960-01-01';
ALTER TABLE t_exact_literals UPDATE x = (SELECT toDecimal32(9999999.99, 2) + toDecimal32(9999999.99, 2)) WHERE k = 2;
ALTER TABLE t_exact_literals UPDATE d = (SELECT toDateTime64('1960-05-05 01:02:03.456', 3, 'UTC')) WHERE k = 3;
-- 77 digits, more than the precision of the type
ALTER TABLE t_exact_literals UPDATE y = (SELECT toDecimal256('9999999999999999999999999999999999999999999999999999999999999999999999999999', 0) * toDecimal32(5, 0)) WHERE k = 3;

CREATE TABLE t_exact_literals_dict_src (k UInt64, d DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exact_literals_dict_src VALUES (1, '1950-01-01 00:00:00');
CREATE DICTIONARY t_exact_literals_dict (k UInt64, d DateTime64(3, 'UTC')) PRIMARY KEY k
    SOURCE(CLICKHOUSE(TABLE 't_exact_literals_dict_src')) LAYOUT(FLAT()) LIFETIME(0);
ALTER TABLE t_exact_literals UPDATE v = v + 256 WHERE d = dictGet('t_exact_literals_dict', 'd', toUInt64(1));

-- Variant and Dynamic keep the type of their value
CREATE TABLE t_exact_literals_variant (k UInt8, x Variant(DateTime64(3, 'UTC'), String), u Variant(UInt64, String), y Dynamic, v UInt64 DEFAULT 0)
    ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exact_literals_variant (k, x, u, y) SELECT 1, toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), 42::UInt64, 1;
INSERT INTO t_exact_literals_variant (k, x, u, y) SELECT 2, toDateTime64('1950-01-01 00:00:00', 3, 'UTC'), 43::UInt64, 2;
ALTER TABLE t_exact_literals_variant UPDATE v = v + 1 WHERE x = (SELECT toDateTime64('2024-01-01 00:00:00', 3, 'UTC')::Variant(DateTime64(3, 'UTC'), String));
ALTER TABLE t_exact_literals_variant UPDATE v = v + 2 WHERE x = (SELECT toDateTime64('1950-01-01 00:00:00', 3, 'UTC')::Variant(DateTime64(3, 'UTC'), String));
ALTER TABLE t_exact_literals_variant UPDATE v = v + 4 WHERE u = (SELECT 42::UInt64::Variant(UInt64, String));
ALTER TABLE t_exact_literals_variant UPDATE y = (SELECT toDateTime64('1950-01-01 00:00:00', 3, 'UTC')::Dynamic) WHERE k = 2;

-- a date in a dynamic JSON path is stored as text, so it is inferred as a date again
CREATE TABLE t_exact_literals_json (k UInt8, j JSON) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exact_literals_json VALUES (1, '{}');
ALTER TABLE t_exact_literals_json UPDATE j = (SELECT '{"a":"2024-01-01"}'::JSON) WHERE k = 1;

-- a nondeterministic function keeps the member type of its value: 42 also fits the Date member
CREATE TABLE t_exact_literals_join (k UInt64, x Variant(Date, UInt64)) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_exact_literals_join SELECT 1, 42::UInt64::Variant(Date, UInt64);
CREATE TABLE t_exact_literals_join_dst (k UInt8, x Variant(Date, UInt64)) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exact_literals_join_dst SELECT 1, 1::UInt64::Variant(Date, UInt64);
ALTER TABLE t_exact_literals_join_dst UPDATE x = joinGet('t_exact_literals_join', 'x', toUInt64(1)) WHERE k = 1;

SELECT k, d, n, t, x, y, v FROM t_exact_literals ORDER BY k;
SELECT k, x, variantType(x), v, y, dynamicType(y) FROM t_exact_literals_variant ORDER BY k;
SELECT k, j.a, dynamicType(j.a) FROM t_exact_literals_json ORDER BY k;
SELECT countIf(command LIKE '%SELECT%'), countIf(command LIKE '%dictGet%') FROM system.mutations
    WHERE database = currentDatabase() AND table IN ('t_exact_literals', 't_exact_literals_variant', 't_exact_literals_json');
-- an array is carried as one literal
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_exact_literals' AND command LIKE '%has(%';
SELECT k, x, variantType(x) FROM t_exact_literals_join_dst ORDER BY k;
SELECT countIf(command LIKE '%joinGet%') FROM system.mutations WHERE database = currentDatabase() AND table = 't_exact_literals_join_dst';

DROP TABLE t_exact_literals_join_dst;
DROP TABLE t_exact_literals_join;
DROP TABLE t_exact_literals_json;
DROP TABLE t_exact_literals_variant;
DROP DICTIONARY t_exact_literals_dict;
DROP TABLE t_exact_literals_dict_src;
DROP TABLE t_exact_literals;
