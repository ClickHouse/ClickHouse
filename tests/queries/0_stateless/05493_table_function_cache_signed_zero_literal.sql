-- Table functions are cached per query by the hash of their AST, which hashes literals with
-- `FieldVisitorHash`. `-0.0` and `0.0` have different bits, so they must not share a cache entry.

SELECT bits FROM
(
    SELECT reinterpretAsUInt64(c1) AS bits FROM values(-0.)
    UNION ALL
    SELECT reinterpretAsUInt64(c1) AS bits FROM values(0.)
)
ORDER BY bits;
