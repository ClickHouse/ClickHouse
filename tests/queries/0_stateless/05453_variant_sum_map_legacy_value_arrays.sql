-- The legacy form of -Map, `sumMap(keys, values1, values2, ...)` and the tuple of these arrays, converts every array
-- of `Variant` values independently. The result must be the same as for the explicit cast.

SET allow_suspicious_variant_types = 1;

SELECT sumMap(['a'], [v], [w]), sumMap((['a'], [v], [w])), minMap(['a'], [v], [w]), maxMap((['a'], [v], [w]))
FROM values('v Variant(UInt64), w Variant(UInt64)', (1, 2), (NULL, 3), (2, 4));

SELECT sumMap(['a'], [CAST(v AS Nullable(UInt64))], [CAST(w AS Nullable(UInt64))])
FROM values('v Variant(UInt64), w Variant(UInt64)', (1, 2), (NULL, 3), (2, 4));

-- Only a later value array is `Variant`.
SELECT sumMap(['a'], [v], [w]), sumMap((['a'], [v], [w])), toTypeName(sumMap(['a'], [v], [w]))
FROM values('v UInt64, w Variant(UInt8, Int16)', (1, 2), (2, NULL), (3, -4));

SELECT sumMap(['a'], [v], [CAST(w AS Nullable(Int16))]), toTypeName(sumMap(['a'], [v], [CAST(w AS Nullable(Int16))]))
FROM values('v UInt64, w Variant(UInt8, Int16)', (1, 2), (2, NULL), (3, -4));

-- With a combinator around.
SELECT sumMapIf(['a'], [v], [w], v > 1)
FROM values('v UInt64, w Variant(UInt8, Int16)', (1, 2), (2, NULL), (3, -4));
