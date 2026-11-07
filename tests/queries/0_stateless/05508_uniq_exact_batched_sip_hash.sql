-- `uniqExact` over one `String`, `FixedString` or `IPv6` argument hashes the values of a block several at a time. The
-- states store the hashes, so they must not change: the state checksums in the reference were computed by the code that
-- hashes one value at a time.

DROP VIEW IF EXISTS uniq_exact_values;
DROP TABLE IF EXISTS uniq_exact_states;
DROP TABLE IF EXISTS uniq_exact_if_states;
DROP TABLE IF EXISTS uniq_exact_nullable_states;
DROP TABLE IF EXISTS uniq_exact_nullable_if_states;

-- Every string length from 0 to 300, and every 25th string 512 to 4100 bytes long. The `*r` columns repeat each value
-- 4 times in a row, and `kr` keeps 12 rows in a row in one group, so consecutive rows of a group repeat values. The keys
-- are `UInt16`: a `UInt8` key (`k8`) is aggregated one row at a time.
CREATE VIEW uniq_exact_values AS
SELECT
    toUInt16(number % 3) AS k,
    toUInt16(intDiv(number, 12) % 3) AS kr,
    toUInt8(number % 3) AS k8,
    number % 7 != 0 AS c,
    substring(repeat(hex(cityHash64(number)), 260), 1, if(number % 25 = 7, 512 + number * 37 % 3589, number % 301)) AS s,
    toFixedString(substring(hex(cityHash64(number)), 1, 5), 5) AS f5,
    toFixedString(hex(cityHash64(number)), 16) AS f16,
    toFixedString(substring(repeat(hex(cityHash64(number)), 4), 1, 64), 64) AS f64,
    CAST(toUInt128(cityHash64(number)) * 18446744073709551557 + number AS IPv6) AS ip,
    substring(repeat(hex(cityHash64(intDiv(number, 4))), 260), 1, if(intDiv(number, 4) % 25 = 7, 512 + intDiv(number, 4) * 37 % 3589, intDiv(number, 4) % 301)) AS sr,
    toFixedString(substring(hex(cityHash64(intDiv(number, 4))), 1, 5), 5) AS f5r,
    toFixedString(hex(cityHash64(intDiv(number, 4))), 16) AS f16r,
    toFixedString(substring(repeat(hex(cityHash64(intDiv(number, 4))), 4), 1, 64), 64) AS f64r,
    CAST(toUInt128(cityHash64(intDiv(number, 4))) * 18446744073709551557 + intDiv(number, 4) AS IPv6) AS ipr,
    if(number % 5 = 0, NULL, s) AS ns,
    if(number % 5 = 0, NULL, f5) AS nf5,
    if(number % 5 = 0, NULL, f16) AS nf16,
    if(number % 5 = 0, NULL, f64) AS nf64,
    if(number % 5 = 0, NULL, ip) AS nip
FROM numbers(3000);

-- The counts, with any number of threads.
SELECT 'counts, no key', uniqExact(s), uniqExact(f5), uniqExact(f16), uniqExact(f64), uniqExact(ip) FROM uniq_exact_values;
SELECT 'counts, no key, repeats', uniqExact(sr), uniqExact(f5r), uniqExact(f16r), uniqExact(f64r), uniqExact(ipr) FROM uniq_exact_values;
SELECT 'counts, no key, If', uniqExactIf(s, c), uniqExactIf(f5, c), uniqExactIf(f16, c), uniqExactIf(f64, c), uniqExactIf(ip, c) FROM uniq_exact_values;
SELECT 'counts, no key, Nullable', uniqExact(ns), uniqExact(nf5), uniqExact(nf16), uniqExact(nf64), uniqExact(nip) FROM uniq_exact_values;
SELECT 'counts, no key, Nullable, If', uniqExactIf(ns, c), uniqExactIf(nf5, c), uniqExactIf(nf16, c), uniqExactIf(nf64, c), uniqExactIf(nip, c) FROM uniq_exact_values;
SELECT 'counts, GROUP BY', k, uniqExact(s), uniqExact(f5), uniqExact(f16), uniqExact(f64), uniqExact(ip) FROM uniq_exact_values GROUP BY k ORDER BY k;
SELECT 'counts, GROUP BY, repeats', kr, uniqExact(sr), uniqExact(f5r), uniqExact(f16r), uniqExact(f64r), uniqExact(ipr) FROM uniq_exact_values GROUP BY kr ORDER BY kr;
SELECT 'counts, GROUP BY, If', k, uniqExactIf(s, c), uniqExactIf(f5, c), uniqExactIf(f16, c), uniqExactIf(f64, c), uniqExactIf(ip, c) FROM uniq_exact_values GROUP BY k ORDER BY k;
SELECT 'counts, GROUP BY, repeats, If', kr, uniqExactIf(sr, c), uniqExactIf(f5r, c), uniqExactIf(f16r, c), uniqExactIf(f64r, c), uniqExactIf(ipr, c) FROM uniq_exact_values GROUP BY kr ORDER BY kr;
SELECT 'counts, GROUP BY, Nullable', k, uniqExact(ns), uniqExact(nf5), uniqExact(nf16), uniqExact(nf64), uniqExact(nip) FROM uniq_exact_values GROUP BY k ORDER BY k;

-- The checksums of the states. One thread inserts the rows in order, and nothing merges states, so the same hashes give
-- the same bytes. `-IfState` keeps the batched `-If` path; `-StateIf` would add one row at a time.
SET max_threads = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'no key', cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values SETTINGS max_block_size = 7;
SELECT 'no key', cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values SETTINGS max_block_size = 29;
SELECT 'no key', cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values SETTINGS max_block_size = 257;
SELECT 'no key', cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values;

SELECT 'no key, repeats', cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values SETTINGS max_block_size = 7;
SELECT 'no key, repeats', cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values SETTINGS max_block_size = 29;
SELECT 'no key, repeats', cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values SETTINGS max_block_size = 257;
SELECT 'no key, repeats', cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values;

SELECT 'no key, If', cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values SETTINGS max_block_size = 7;
SELECT 'no key, If', cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values SETTINGS max_block_size = 29;
SELECT 'no key, If', cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values SETTINGS max_block_size = 257;
SELECT 'no key, If', cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values;

SELECT 'no key, Nullable', cityHash64(hex(uniqExactState(ns))), cityHash64(hex(uniqExactState(nf5))), cityHash64(hex(uniqExactState(nf16))), cityHash64(hex(uniqExactState(nf64))), cityHash64(hex(uniqExactState(nip))) FROM uniq_exact_values SETTINGS max_block_size = 7;
SELECT 'no key, Nullable', cityHash64(hex(uniqExactState(ns))), cityHash64(hex(uniqExactState(nf5))), cityHash64(hex(uniqExactState(nf16))), cityHash64(hex(uniqExactState(nf64))), cityHash64(hex(uniqExactState(nip))) FROM uniq_exact_values SETTINGS max_block_size = 29;
SELECT 'no key, Nullable', cityHash64(hex(uniqExactState(ns))), cityHash64(hex(uniqExactState(nf5))), cityHash64(hex(uniqExactState(nf16))), cityHash64(hex(uniqExactState(nf64))), cityHash64(hex(uniqExactState(nip))) FROM uniq_exact_values SETTINGS max_block_size = 257;
SELECT 'no key, Nullable', cityHash64(hex(uniqExactState(ns))), cityHash64(hex(uniqExactState(nf5))), cityHash64(hex(uniqExactState(nf16))), cityHash64(hex(uniqExactState(nf64))), cityHash64(hex(uniqExactState(nip))) FROM uniq_exact_values;

SELECT 'no key, Nullable, If', cityHash64(hex(uniqExactIfState(ns, c))), cityHash64(hex(uniqExactIfState(nf5, c))), cityHash64(hex(uniqExactIfState(nf16, c))), cityHash64(hex(uniqExactIfState(nf64, c))), cityHash64(hex(uniqExactIfState(nip, c))) FROM uniq_exact_values SETTINGS max_block_size = 7;
SELECT 'no key, Nullable, If', cityHash64(hex(uniqExactIfState(ns, c))), cityHash64(hex(uniqExactIfState(nf5, c))), cityHash64(hex(uniqExactIfState(nf16, c))), cityHash64(hex(uniqExactIfState(nf64, c))), cityHash64(hex(uniqExactIfState(nip, c))) FROM uniq_exact_values SETTINGS max_block_size = 29;
SELECT 'no key, Nullable, If', cityHash64(hex(uniqExactIfState(ns, c))), cityHash64(hex(uniqExactIfState(nf5, c))), cityHash64(hex(uniqExactIfState(nf16, c))), cityHash64(hex(uniqExactIfState(nf64, c))), cityHash64(hex(uniqExactIfState(nip, c))) FROM uniq_exact_values SETTINGS max_block_size = 257;
SELECT 'no key, Nullable, If', cityHash64(hex(uniqExactIfState(ns, c))), cityHash64(hex(uniqExactIfState(nf5, c))), cityHash64(hex(uniqExactIfState(nf16, c))), cityHash64(hex(uniqExactIfState(nf64, c))), cityHash64(hex(uniqExactIfState(nip, c))) FROM uniq_exact_values;

SELECT 'GROUP BY', k, cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values GROUP BY k ORDER BY k SETTINGS max_block_size = 7;
SELECT 'GROUP BY', k, cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values GROUP BY k ORDER BY k SETTINGS max_block_size = 29;
SELECT 'GROUP BY', k, cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values GROUP BY k ORDER BY k SETTINGS max_block_size = 257;
SELECT 'GROUP BY', k, cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values GROUP BY k ORDER BY k;

SELECT 'GROUP BY, repeats', kr, cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values GROUP BY kr ORDER BY kr SETTINGS max_block_size = 7;
SELECT 'GROUP BY, repeats', kr, cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values GROUP BY kr ORDER BY kr SETTINGS max_block_size = 29;
SELECT 'GROUP BY, repeats', kr, cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values GROUP BY kr ORDER BY kr SETTINGS max_block_size = 257;
SELECT 'GROUP BY, repeats', kr, cityHash64(hex(uniqExactState(sr))), cityHash64(hex(uniqExactState(f5r))), cityHash64(hex(uniqExactState(f16r))), cityHash64(hex(uniqExactState(f64r))), cityHash64(hex(uniqExactState(ipr))) FROM uniq_exact_values GROUP BY kr ORDER BY kr;

SELECT 'GROUP BY, If', k, cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values GROUP BY k ORDER BY k SETTINGS max_block_size = 7;
SELECT 'GROUP BY, If', k, cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values GROUP BY k ORDER BY k SETTINGS max_block_size = 29;
SELECT 'GROUP BY, If', k, cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values GROUP BY k ORDER BY k SETTINGS max_block_size = 257;
SELECT 'GROUP BY, If', k, cityHash64(hex(uniqExactIfState(s, c))), cityHash64(hex(uniqExactIfState(f5, c))), cityHash64(hex(uniqExactIfState(f16, c))), cityHash64(hex(uniqExactIfState(f64, c))), cityHash64(hex(uniqExactIfState(ip, c))) FROM uniq_exact_values GROUP BY k ORDER BY k;

SELECT 'GROUP BY, repeats, If', kr, cityHash64(hex(uniqExactIfState(sr, c))), cityHash64(hex(uniqExactIfState(f5r, c))), cityHash64(hex(uniqExactIfState(f16r, c))), cityHash64(hex(uniqExactIfState(f64r, c))), cityHash64(hex(uniqExactIfState(ipr, c))) FROM uniq_exact_values GROUP BY kr ORDER BY kr SETTINGS max_block_size = 7;
SELECT 'GROUP BY, repeats, If', kr, cityHash64(hex(uniqExactIfState(sr, c))), cityHash64(hex(uniqExactIfState(f5r, c))), cityHash64(hex(uniqExactIfState(f16r, c))), cityHash64(hex(uniqExactIfState(f64r, c))), cityHash64(hex(uniqExactIfState(ipr, c))) FROM uniq_exact_values GROUP BY kr ORDER BY kr SETTINGS max_block_size = 29;
SELECT 'GROUP BY, repeats, If', kr, cityHash64(hex(uniqExactIfState(sr, c))), cityHash64(hex(uniqExactIfState(f5r, c))), cityHash64(hex(uniqExactIfState(f16r, c))), cityHash64(hex(uniqExactIfState(f64r, c))), cityHash64(hex(uniqExactIfState(ipr, c))) FROM uniq_exact_values GROUP BY kr ORDER BY kr SETTINGS max_block_size = 257;
SELECT 'GROUP BY, repeats, If', kr, cityHash64(hex(uniqExactIfState(sr, c))), cityHash64(hex(uniqExactIfState(f5r, c))), cityHash64(hex(uniqExactIfState(f16r, c))), cityHash64(hex(uniqExactIfState(f64r, c))), cityHash64(hex(uniqExactIfState(ipr, c))) FROM uniq_exact_values GROUP BY kr ORDER BY kr;

-- Once `max_rows_to_group_by` is reached, the rows of new keys have no place.
SELECT 'GROUP BY, overflow any', k, kr, cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values GROUP BY k, kr ORDER BY k, kr SETTINGS max_block_size = 7, max_rows_to_group_by = 2, group_by_overflow_mode = 'any';

-- A `UInt8` key, and a `Nullable` argument with a key, insert one row at a time.
SELECT 'GROUP BY UInt8', k8, cityHash64(hex(uniqExactState(s))), cityHash64(hex(uniqExactState(f5))), cityHash64(hex(uniqExactState(f16))), cityHash64(hex(uniqExactState(f64))), cityHash64(hex(uniqExactState(ip))) FROM uniq_exact_values GROUP BY k8 ORDER BY k8;
SELECT 'GROUP BY, Nullable', k, cityHash64(hex(uniqExactState(ns))), cityHash64(hex(uniqExactState(nf5))), cityHash64(hex(uniqExactState(nf16))), cityHash64(hex(uniqExactState(nf64))), cityHash64(hex(uniqExactState(nip))) FROM uniq_exact_values GROUP BY k ORDER BY k;

-- The states of the shapes above, with one and with several threads, merged with states built one row at a time (a
-- `UInt8` key, a `Nullable` argument with a key) that hold every value: a hash that differs from the one of the
-- per-row path is counted twice.
CREATE TABLE uniq_exact_states
(
    s AggregateFunction(uniqExact, String),
    f5 AggregateFunction(uniqExact, FixedString(5)),
    f16 AggregateFunction(uniqExact, FixedString(16)),
    f64 AggregateFunction(uniqExact, FixedString(64)),
    ip AggregateFunction(uniqExact, IPv6)
)
ENGINE = Memory;

INSERT INTO uniq_exact_states SELECT uniqExactState(s), uniqExactState(f5), uniqExactState(f16), uniqExactState(f64), uniqExactState(ip) FROM uniq_exact_values SETTINGS max_threads = 1;
INSERT INTO uniq_exact_states SELECT uniqExactState(s), uniqExactState(f5), uniqExactState(f16), uniqExactState(f64), uniqExactState(ip) FROM uniq_exact_values SETTINGS max_threads = 4;
INSERT INTO uniq_exact_states SELECT uniqExactState(sr), uniqExactState(f5r), uniqExactState(f16r), uniqExactState(f64r), uniqExactState(ipr) FROM uniq_exact_values SETTINGS max_threads = 1;
INSERT INTO uniq_exact_states SELECT uniqExactState(sr), uniqExactState(f5r), uniqExactState(f16r), uniqExactState(f64r), uniqExactState(ipr) FROM uniq_exact_values SETTINGS max_threads = 4;
INSERT INTO uniq_exact_states SELECT uniqExactState(s), uniqExactState(f5), uniqExactState(f16), uniqExactState(f64), uniqExactState(ip) FROM uniq_exact_values GROUP BY k SETTINGS max_threads = 1;
INSERT INTO uniq_exact_states SELECT uniqExactState(s), uniqExactState(f5), uniqExactState(f16), uniqExactState(f64), uniqExactState(ip) FROM uniq_exact_values GROUP BY k SETTINGS max_threads = 4;
INSERT INTO uniq_exact_states SELECT uniqExactState(sr), uniqExactState(f5r), uniqExactState(f16r), uniqExactState(f64r), uniqExactState(ipr) FROM uniq_exact_values GROUP BY kr SETTINGS max_threads = 1;
INSERT INTO uniq_exact_states SELECT uniqExactState(sr), uniqExactState(f5r), uniqExactState(f16r), uniqExactState(f64r), uniqExactState(ipr) FROM uniq_exact_values GROUP BY kr SETTINGS max_threads = 4;
INSERT INTO uniq_exact_states SELECT uniqExactState(s), uniqExactState(f5), uniqExactState(f16), uniqExactState(f64), uniqExactState(ip) FROM uniq_exact_values GROUP BY k8;
SELECT 'merged', uniqExactMerge(s), uniqExactMerge(f5), uniqExactMerge(f16), uniqExactMerge(f64), uniqExactMerge(ip) FROM uniq_exact_states;

CREATE TABLE uniq_exact_if_states
(
    s AggregateFunction(uniqExactIf, String, UInt8),
    f5 AggregateFunction(uniqExactIf, FixedString(5), UInt8),
    f16 AggregateFunction(uniqExactIf, FixedString(16), UInt8),
    f64 AggregateFunction(uniqExactIf, FixedString(64), UInt8),
    ip AggregateFunction(uniqExactIf, IPv6, UInt8)
)
ENGINE = Memory;

INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(s, c), uniqExactIfState(f5, c), uniqExactIfState(f16, c), uniqExactIfState(f64, c), uniqExactIfState(ip, c) FROM uniq_exact_values SETTINGS max_threads = 1;
INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(s, c), uniqExactIfState(f5, c), uniqExactIfState(f16, c), uniqExactIfState(f64, c), uniqExactIfState(ip, c) FROM uniq_exact_values SETTINGS max_threads = 4;
INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(s, c), uniqExactIfState(f5, c), uniqExactIfState(f16, c), uniqExactIfState(f64, c), uniqExactIfState(ip, c) FROM uniq_exact_values GROUP BY k SETTINGS max_threads = 1;
INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(s, c), uniqExactIfState(f5, c), uniqExactIfState(f16, c), uniqExactIfState(f64, c), uniqExactIfState(ip, c) FROM uniq_exact_values GROUP BY k SETTINGS max_threads = 4;
INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(sr, c), uniqExactIfState(f5r, c), uniqExactIfState(f16r, c), uniqExactIfState(f64r, c), uniqExactIfState(ipr, c) FROM uniq_exact_values GROUP BY kr SETTINGS max_threads = 1;
INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(sr, c), uniqExactIfState(f5r, c), uniqExactIfState(f16r, c), uniqExactIfState(f64r, c), uniqExactIfState(ipr, c) FROM uniq_exact_values GROUP BY kr SETTINGS max_threads = 4;
INSERT INTO uniq_exact_if_states SELECT uniqExactIfState(s, k8 < 3), uniqExactIfState(f5, k8 < 3), uniqExactIfState(f16, k8 < 3), uniqExactIfState(f64, k8 < 3), uniqExactIfState(ip, k8 < 3) FROM uniq_exact_values GROUP BY k8;
SELECT 'merged, If', uniqExactIfMerge(s), uniqExactIfMerge(f5), uniqExactIfMerge(f16), uniqExactIfMerge(f64), uniqExactIfMerge(ip) FROM uniq_exact_if_states;

CREATE TABLE uniq_exact_nullable_states
(
    s AggregateFunction(uniqExact, Nullable(String)),
    f5 AggregateFunction(uniqExact, Nullable(FixedString(5))),
    f16 AggregateFunction(uniqExact, Nullable(FixedString(16))),
    f64 AggregateFunction(uniqExact, Nullable(FixedString(64))),
    ip AggregateFunction(uniqExact, Nullable(IPv6))
)
ENGINE = Memory;

INSERT INTO uniq_exact_nullable_states SELECT uniqExactState(ns), uniqExactState(nf5), uniqExactState(nf16), uniqExactState(nf64), uniqExactState(nip) FROM uniq_exact_values SETTINGS max_threads = 1;
INSERT INTO uniq_exact_nullable_states SELECT uniqExactState(ns), uniqExactState(nf5), uniqExactState(nf16), uniqExactState(nf64), uniqExactState(nip) FROM uniq_exact_values SETTINGS max_threads = 4;
INSERT INTO uniq_exact_nullable_states SELECT uniqExactState(ns), uniqExactState(nf5), uniqExactState(nf16), uniqExactState(nf64), uniqExactState(nip) FROM uniq_exact_values GROUP BY k;
SELECT 'merged, Nullable', uniqExactMerge(s), uniqExactMerge(f5), uniqExactMerge(f16), uniqExactMerge(f64), uniqExactMerge(ip) FROM uniq_exact_nullable_states;

CREATE TABLE uniq_exact_nullable_if_states
(
    s AggregateFunction(uniqExactIf, Nullable(String), UInt8),
    f5 AggregateFunction(uniqExactIf, Nullable(FixedString(5)), UInt8),
    f16 AggregateFunction(uniqExactIf, Nullable(FixedString(16)), UInt8),
    f64 AggregateFunction(uniqExactIf, Nullable(FixedString(64)), UInt8),
    ip AggregateFunction(uniqExactIf, Nullable(IPv6), UInt8)
)
ENGINE = Memory;

INSERT INTO uniq_exact_nullable_if_states SELECT uniqExactIfState(ns, c), uniqExactIfState(nf5, c), uniqExactIfState(nf16, c), uniqExactIfState(nf64, c), uniqExactIfState(nip, c) FROM uniq_exact_values SETTINGS max_threads = 1;
INSERT INTO uniq_exact_nullable_if_states SELECT uniqExactIfState(ns, c), uniqExactIfState(nf5, c), uniqExactIfState(nf16, c), uniqExactIfState(nf64, c), uniqExactIfState(nip, c) FROM uniq_exact_values SETTINGS max_threads = 4;
INSERT INTO uniq_exact_nullable_if_states SELECT uniqExactIfState(ns, k8 < 3), uniqExactIfState(nf5, k8 < 3), uniqExactIfState(nf16, k8 < 3), uniqExactIfState(nf64, k8 < 3), uniqExactIfState(nip, k8 < 3) FROM uniq_exact_values GROUP BY k;
SELECT 'merged, Nullable, If', uniqExactIfMerge(s), uniqExactIfMerge(f5), uniqExactIfMerge(f16), uniqExactIfMerge(f64), uniqExactIfMerge(ip) FROM uniq_exact_nullable_if_states;

DROP TABLE uniq_exact_nullable_if_states;
DROP TABLE uniq_exact_nullable_states;
DROP TABLE uniq_exact_if_states;
DROP TABLE uniq_exact_states;
DROP VIEW uniq_exact_values;
