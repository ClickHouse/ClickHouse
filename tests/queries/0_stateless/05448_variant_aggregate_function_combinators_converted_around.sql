-- Combinators that pass the rows to the nested function one by one (-SimpleState) or element by element (-ForEach, -Array, -Map)
-- convert the `Variant` arguments around themselves, for the whole block. The result must be the same as for the explicit cast.

SET allow_suspicious_variant_types = 1;

WITH number % 3 = 0 ? NULL::Variant(UInt8, Int16) : (number % 3 = 1 ? number::UInt8::Variant(UInt8, Int16) : (-number)::Int16::Variant(UInt8, Int16)) AS v,
    CAST(v AS Nullable(Int16)) AS c
SELECT
    sumSimpleState(v) = sumSimpleState(c), toTypeName(sumSimpleState(v)) = toTypeName(sumSimpleState(c)),
    sumForEach([v, v]) = sumForEach([c, c]), toTypeName(sumForEach([v, v])) = toTypeName(sumForEach([c, c])),
    sumArray([v]) = sumArray([c]), maxArrayForEach([[v]]) = maxArrayForEach([[c]]),
    sumForEachIf([v], number > 2) = sumForEachIf([c], number > 2), sumArrayIf([v], number > 2) = sumArrayIf([c], number > 2),
    avgForEachOrDefault([v]) = avgForEachOrDefault([c]), sumForEachArgMax([v], number) = sumForEachArgMax([c], number),
    sumMap(map('a', v)) = sumMap(map('a', c)), toTypeName(sumMap(map('a', v))) = toTypeName(sumMap(map('a', c))),
    sumMap(['a'], [v]) = sumMap(['a'], [c]), sumMap((['a'], [v])) = sumMap((['a'], [c]))
FROM numbers(10);

WITH number % 3 = 0 ? NULL::Variant(UInt8, Int16) : (number % 3 = 1 ? number::UInt8::Variant(UInt8, Int16) : (-number)::Int16::Variant(UInt8, Int16)) AS v
SELECT sumSimpleState(v), sumForEach([v, v]), sumArrayIf([v], number > 2), sumMap(map('a', v)), groupArrayForEach([v]), groupArrayMap(map('a', v), number % 2)
FROM numbers(10);
