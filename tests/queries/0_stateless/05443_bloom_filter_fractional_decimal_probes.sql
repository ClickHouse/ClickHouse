-- Fractional `Decimal` probes must not alias an inserted integer after truncation.
WITH (SELECT groupBloomFilterState(toUInt64(42))) AS bf
SELECT
    bloomFilterContains(bf, toDecimal32('42.5', 1)),
    bloomFilterContains(bf, toDecimal32('42.0', 1)),
    bloomFilterContains(bf, toDecimal64('42.5', 1)),
    bloomFilterContains(bf, toDecimal64('42.0', 1)),
    bloomFilterContains(bf, toDecimal128('42.5', 1)),
    bloomFilterContains(bf, toDecimal128('42.0', 1)),
    bloomFilterContains(bf, toDecimal256('42.5', 1)),
    bloomFilterContains(bf, toDecimal256('42.0', 1)),
    bloomFilterContains(bf, toDecimal64('42', 0));

-- Negative fractions must not alias signed integers or unsigned zero.
WITH
    (SELECT groupBloomFilterState(toInt64(-42))) AS signed_bf,
    (SELECT groupBloomFilterState(toUInt64(0))) AS unsigned_bf
SELECT
    bloomFilterContains(signed_bf, toDecimal64('-42.5', 1)),
    bloomFilterContains(signed_bf, toDecimal64('-42.0', 1)),
    bloomFilterContains(unsigned_bf, toDecimal64('-0.5', 1)),
    bloomFilterContains(unsigned_bf, toDecimal64('0.0', 1));

-- Exact wide integers must remain valid without an integer-to-`Decimal` round trip.
WITH
    (SELECT groupBloomFilterState(toInt128('-18446744073709551616'))) AS int128_bf,
    (SELECT groupBloomFilterState(toUInt128('18446744073709551616'))) AS uint128_bf,
    (SELECT groupBloomFilterState(toInt256('-170141183460469231731687303715884105728'))) AS int256_bf,
    (SELECT groupBloomFilterState(toUInt256('170141183460469231731687303715884105728'))) AS uint256_bf
SELECT
    bloomFilterContains(int128_bf, toDecimal128('-18446744073709551616.5', 1)),
    bloomFilterContains(int128_bf, toDecimal128('-18446744073709551616.0', 1)),
    bloomFilterContains(uint128_bf, toDecimal128('18446744073709551616.5', 1)),
    bloomFilterContains(uint128_bf, toDecimal128('18446744073709551616.0', 1)),
    bloomFilterContains(int256_bf, toDecimal256('-170141183460469231731687303715884105728.5', 1)),
    bloomFilterContains(int256_bf, toDecimal256('-170141183460469231731687303715884105728.0', 1)),
    bloomFilterContains(uint256_bf, toDecimal256('170141183460469231731687303715884105728.5', 1)),
    bloomFilterContains(uint256_bf, toDecimal256('170141183460469231731687303715884105728.0', 1));

-- Exercise vector probes and preserve `NULL` propagation for each `Decimal` width.
WITH
    (SELECT groupBloomFilterState(value) FROM (SELECT arrayJoin([toUInt64(0), 42]) AS value)) AS bf
SELECT
    probe,
    bloomFilterContains(bf, toDecimal32(probe, 1)),
    bloomFilterContains(bf, toDecimal64(probe, 1)),
    bloomFilterContains(bf, toDecimal128(probe, 1)),
    bloomFilterContains(bf, toDecimal256(probe, 1))
FROM
(
    SELECT arrayJoin(CAST(['-0.5', '0.0', '42.0', '42.5', NULL], 'Array(Nullable(String))')) AS probe
)
ORDER BY probe NULLS LAST;
