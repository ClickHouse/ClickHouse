-- The constant `else` branch is a tuple holding a NULL `Dynamic` whose type list (`String`) cannot be merged
-- into the result's, which is already full after the first 64 rows. All 128 rows must still be produced.
SELECT count(), sum(length(toString(x)))
FROM
(
    SELECT if(number < 64,
        CAST(tuple(number::Dynamic(max_types = 1)) AS Variant(Tuple(Dynamic(max_types = 1)), UInt64)),
        CAST(tuple(NULL::Nullable(String)::Dynamic(max_types = 1)) AS Variant(Tuple(Dynamic(max_types = 1)), UInt64))) AS x
    FROM numbers(128)
);
