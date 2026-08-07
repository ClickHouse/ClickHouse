SET optimize_trivial_insert_select = 0;

-------------------------------------------------------------------------------------------
-- Case 1: Verify that GCD compression works for signed integers and decimals with negative values.
-- GCD compression applies to any fixed-width integer storage type (1, 2, 4, 8, 16, 32 bytes).
-- This table deliberately uses values of ~10^15 (multiples of 10^15) so the GCD divisor is
-- large enough to produce a clearly measurable compression ratio.  Those values overflow
-- Int8/Int16/Int32, so the smaller signed widths are omitted here and covered by the
-- round-trip test (use case 2) below.
DROP TABLE IF EXISTS table_gcd_codec_on_negative_integers;

CREATE TABLE table_gcd_codec_on_negative_integers
(
    symbol LowCardinality(String),
    ts DateTime64(6, 'UTC') CODEC(Delta, ZSTD),

    /* Signed integers */
    col_int64_plain       Int64 CODEC(ZSTD),
    col_int64_gcd         Int64 CODEC(GCD, ZSTD),
    col_int64_abs_plain   Int64 CODEC(ZSTD),
    col_int64_abs_gcd     Int64 CODEC(GCD, ZSTD),

    col_int128_plain      Int128 CODEC(ZSTD),
    col_int128_gcd        Int128 CODEC(GCD, ZSTD),
    col_int128_abs_plain  Int128 CODEC(ZSTD),
    col_int128_abs_gcd    Int128 CODEC(GCD, ZSTD),

    col_int256_plain      Int256 CODEC(ZSTD),
    col_int256_gcd        Int256 CODEC(GCD, ZSTD),
    col_int256_abs_plain  Int256 CODEC(ZSTD),
    col_int256_abs_gcd    Int256 CODEC(GCD, ZSTD),

    /* Unsigned integers */
    col_uint64_plain      UInt64 CODEC(ZSTD),
    col_uint64_gcd        UInt64 CODEC(GCD, ZSTD),

    col_uint128_plain     UInt128 CODEC(ZSTD),
    col_uint128_gcd       UInt128 CODEC(GCD, ZSTD),

    col_uint256_plain     UInt256 CODEC(ZSTD),
    col_uint256_gcd       UInt256 CODEC(GCD, ZSTD),

    /* Decimals (scale=3, safe for ~10^6 magnitude) */
    col_dec64_plain       Decimal64(3) CODEC(ZSTD),
    col_dec64_gcd         Decimal64(3) CODEC(GCD, ZSTD),
    col_dec64_abs_plain   Decimal64(3) CODEC(ZSTD),
    col_dec64_abs_gcd     Decimal64(3) CODEC(GCD, ZSTD),

    col_dec128_plain      Decimal128(3) CODEC(ZSTD),
    col_dec128_gcd        Decimal128(3) CODEC(GCD, ZSTD),
    col_dec128_abs_plain  Decimal128(3) CODEC(ZSTD),
    col_dec128_abs_gcd    Decimal128(3) CODEC(GCD, ZSTD),

    col_dec256_plain      Decimal256(3) CODEC(ZSTD),
    col_dec256_gcd        Decimal256(3) CODEC(GCD, ZSTD),
    col_dec256_abs_plain  Decimal256(3) CODEC(ZSTD),
    col_dec256_abs_gcd    Decimal256(3) CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY (symbol, ts)
/* Force wide parts so system.columns reports per-column compressed/uncompressed bytes. */
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO table_gcd_codec_on_negative_integers
SELECT
    'binance:spot:SOLUSDT',
    now64(6),

    /* SIGNED INTEGERS (~10^15) */
    x_big, x_big, abs(x_big), abs(x_big),

    toInt128(x_big), toInt128(x_big), toInt128(abs(x_big)), toInt128(abs(x_big)),
    toInt256(x_big), toInt256(x_big), toInt256(abs(x_big)), toInt256(abs(x_big)),

    /* UNSIGNED INTEGERS (~10^15, positive only) */
    reinterpretAsUInt64(abs(x_big)),
    reinterpretAsUInt64(abs(x_big)),

    reinterpretAsUInt128(toInt128(abs(x_big))),
    reinterpretAsUInt128(toInt128(abs(x_big))),

    reinterpretAsUInt256(toInt256(abs(x_big))),
    reinterpretAsUInt256(toInt256(abs(x_big))),

    /* DECIMALS (~10^6, safe for Decimal64/128/256(3)) */
    toDecimal64(x_small, 3),  toDecimal64(x_small, 3),
    toDecimal64(abs(x_small), 3), toDecimal64(abs(x_small), 3),

    toDecimal128(x_small, 3), toDecimal128(x_small, 3),
    toDecimal128(abs(x_small), 3), toDecimal128(abs(x_small), 3),

    toDecimal256(x_small, 3), toDecimal256(x_small, 3),
    toDecimal256(abs(x_small), 3), toDecimal256(abs(x_small), 3)
FROM
(
    SELECT
        /* Large magnitude for integer types */
        ((reinterpretAsInt64(rand64()) % 2001) - 1000) * 1000000000000000 AS x_big,
        /* Small magnitude for decimals */
        ((reinterpretAsInt64(rand64()) % 2001) - 1000) * 1000 AS x_small
    FROM numbers(100000)
);

/* Get compressed/uncompressed bytes for each column, compute compression ratio, and compare GCD vs plain. */
-- SELECT
--     c.database,
--     c.table,
--     c.name AS column,
--     formatReadableSize(c.data_compressed_bytes) AS compressed,
--     formatReadableSize(c.data_uncompressed_bytes) AS uncompressed,
--     round(c.data_uncompressed_bytes / c.data_compressed_bytes, 2) AS compr_ratio,
--     p.rows AS rows_cnt,
--     round(c.data_compressed_bytes / p.rows, 2) AS avg_row_size
-- FROM system.columns AS c
-- LEFT JOIN
-- (
--     SELECT
--         database,
--         table,
--         sum(rows) AS rows
--     FROM system.parts
--     WHERE active AND table = 'table_gcd_codec_on_negative_integers'
--     GROUP BY database, table
-- ) AS p
-- ON c.database = p.database AND c.table = p.table
-- WHERE c.table = 'table_gcd_codec_on_negative_integers'
-- ORDER BY column;

/* Compare GCD vs plain compression ratios for each type. */
WITH cols AS
(
    SELECT
        name,
        data_uncompressed_bytes,
        data_compressed_bytes,
        if(data_compressed_bytes = 0, 0.,
           data_uncompressed_bytes / data_compressed_bytes) AS ratio
        FROM system.columns
        WHERE `database` = currentDatabase()
            AND `table` = 'table_gcd_codec_on_negative_integers'
),
r AS
(
    SELECT
        /* Signed ints */
        maxIf(ratio, name='col_int64_plain')      AS r_int64_plain,
        maxIf(ratio, name='col_int64_gcd')        AS r_int64_gcd,
        maxIf(ratio, name='col_int64_abs_plain')  AS r_int64_abs_plain,
        maxIf(ratio, name='col_int64_abs_gcd')    AS r_int64_abs_gcd,

        maxIf(ratio, name='col_int128_plain')     AS r_int128_plain,
        maxIf(ratio, name='col_int128_gcd')       AS r_int128_gcd,
        maxIf(ratio, name='col_int128_abs_plain') AS r_int128_abs_plain,
        maxIf(ratio, name='col_int128_abs_gcd')   AS r_int128_abs_gcd,

        maxIf(ratio, name='col_int256_plain')     AS r_int256_plain,
        maxIf(ratio, name='col_int256_gcd')       AS r_int256_gcd,
        maxIf(ratio, name='col_int256_abs_plain') AS r_int256_abs_plain,
        maxIf(ratio, name='col_int256_abs_gcd')   AS r_int256_abs_gcd,

        /* Unsigned ints */
        maxIf(ratio, name='col_uint64_plain')     AS r_uint64_plain,
        maxIf(ratio, name='col_uint64_gcd')       AS r_uint64_gcd,

        maxIf(ratio, name='col_uint128_plain')    AS r_uint128_plain,
        maxIf(ratio, name='col_uint128_gcd')      AS r_uint128_gcd,

        maxIf(ratio, name='col_uint256_plain')    AS r_uint256_plain,
        maxIf(ratio, name='col_uint256_gcd')      AS r_uint256_gcd,

        /* Decimals */
        maxIf(ratio, name='col_dec64_plain')      AS r_dec64_plain,
        maxIf(ratio, name='col_dec64_gcd')        AS r_dec64_gcd,
        maxIf(ratio, name='col_dec64_abs_plain')  AS r_dec64_abs_plain,
        maxIf(ratio, name='col_dec64_abs_gcd')    AS r_dec64_abs_gcd,

        maxIf(ratio, name='col_dec128_plain')     AS r_dec128_plain,
        maxIf(ratio, name='col_dec128_gcd')       AS r_dec128_gcd,
        maxIf(ratio, name='col_dec128_abs_plain') AS r_dec128_abs_plain,
        maxIf(ratio, name='col_dec128_abs_gcd')   AS r_dec128_abs_gcd,

        maxIf(ratio, name='col_dec256_plain')     AS r_dec256_plain,
        maxIf(ratio, name='col_dec256_gcd')       AS r_dec256_gcd,
        maxIf(ratio, name='col_dec256_abs_plain') AS r_dec256_abs_plain,
        maxIf(ratio, name='col_dec256_abs_gcd')   AS r_dec256_abs_gcd
    FROM cols
)
SELECT
    /* Signed ints — MUST PASS as bug is fixed */
    (r_int64_abs_gcd  > r_int64_abs_plain)  AS int64_abs_gcd_ok,
    (r_int64_gcd      > r_int64_plain)      AS int64_signed_gcd_better_than_plain,
    (r_int64_gcd      >= 0.5 * r_int64_abs_gcd) AS int64_signed_gcd_reasonable_vs_abs,

    (r_int128_abs_gcd > r_int128_abs_plain) AS int128_abs_gcd_ok,
    (r_int128_gcd     > r_int128_plain)     AS int128_signed_gcd_better_than_plain,
    (r_int128_gcd     >= 0.5 * r_int128_abs_gcd) AS int128_signed_gcd_reasonable_vs_abs,

    (r_int256_abs_gcd > r_int256_abs_plain) AS int256_abs_gcd_ok,
    (r_int256_gcd     > r_int256_plain)     AS int256_signed_gcd_better_than_plain,
    (r_int256_gcd     >= 0.5 * r_int256_abs_gcd) AS int256_signed_gcd_reasonable_vs_abs,

    /* Unsigned ints — no negative bug expected */
    (r_uint64_gcd     > r_uint64_plain)     AS uint64_gcd_better_than_plain,
    (r_uint128_gcd    > r_uint128_plain)    AS uint128_gcd_better_than_plain,
    (r_uint256_gcd    > r_uint256_plain)    AS uint256_gcd_better_than_plain,

    /* Decimals — MUST PASS as bug is fixed */
    (r_dec64_abs_gcd  > r_dec64_abs_plain)  AS dec64_abs_gcd_ok,
    (r_dec64_gcd      > r_dec64_plain)      AS dec64_signed_gcd_better_than_plain,
    (r_dec64_gcd      >= 0.5 * r_dec64_abs_gcd) AS dec64_signed_gcd_reasonable_vs_abs,

    (r_dec128_abs_gcd > r_dec128_abs_plain) AS dec128_abs_gcd_ok,
    (r_dec128_gcd     > r_dec128_plain)     AS dec128_signed_gcd_better_than_plain,
    (r_dec128_gcd     >= 0.5 * r_dec128_abs_gcd) AS dec128_signed_gcd_reasonable_vs_abs,

    (r_dec256_abs_gcd > r_dec256_abs_plain) AS dec256_abs_gcd_ok,
    (r_dec256_gcd     > r_dec256_plain)     AS dec256_signed_gcd_better_than_plain,
    (r_dec256_gcd     >= 0.5 * r_dec256_abs_gcd) AS dec256_signed_gcd_reasonable_vs_abs
FROM r;

DROP TABLE table_gcd_codec_on_negative_integers;

-------------------------------------------------------------------------------------------
-- Case 2: Verify that all signed integer widths round-trip correctly under GCD compression.
-- Each column uses INT_MIN for that width so the magnitude 2^(N-1) is unrepresentable
-- in the signed type — the exact edge case that [expr.unary.op] makes UB.
-- Int128/Int256 use simple multiples instead of their literal INT_MIN values.

DROP TABLE IF EXISTS table_gcd_codec_signed_min_roundtrip;
CREATE TABLE table_gcd_codec_signed_min_roundtrip
(
    a Int8   CODEC(GCD, ZSTD),
    b Int16  CODEC(GCD, ZSTD),
    c Int32  CODEC(GCD, ZSTD),
    d Int64  CODEC(GCD, ZSTD),
    e Int128 CODEC(GCD, ZSTD),
    f Int256 CODEC(GCD, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO table_gcd_codec_signed_min_roundtrip VALUES
    (-128,  -32768,       -2147483648,  -9223372036854775808,  toInt128(-200),  toInt256(-200)),
    (-64,   -16384,       -1073741824,  -4611686018427387904,  toInt128(-100),  toInt256(-100)),
    ( 64,    16384,        1073741824,   4611686018427387904,  toInt128( 100),  toInt256( 100));

SELECT min(a), max(a), min(b), max(b), min(c), max(c), min(d), max(d), min(e), max(e), min(f), max(f)
FROM table_gcd_codec_signed_min_roundtrip;

DROP TABLE table_gcd_codec_signed_min_roundtrip;
