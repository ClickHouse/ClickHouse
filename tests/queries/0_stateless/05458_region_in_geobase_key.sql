-- Tags: no-fasttest
-- Tag no-fasttest: needs the legacy geobase (config.d/legacy_geobase.xml with
-- regions_hierarchy.txt), which is not installed in fasttest, as in 02411_legacy_geobase.

-- regionIn(lhs, rhs[, geobase]) with the optional third "point of view" argument.
-- Expected behaviour: docs/reference/functions/regular-functions/embedded-dict-functions.mdx,
-- sections "regionIn" and "Multiple Geobases": the third argument is the dictionary key,
-- '' selects the default hierarchy, and the relation is reflexive.
-- tests/config/regions_hierarchy.txt: 5 < 4 < 3 < 2 < 10 < 9 < 1 and 8 < 7 < 6 < 12 < 11 < 1.
-- No setting influences the result, so nothing has to be pinned.

-- Both arguments constant, default geobase key: Boulder(5) is in World(1), not the other way round.
SELECT regionIn(5::UInt32, 1::UInt32, '');
SELECT regionIn(1::UInt32, 5::UInt32, '');
-- Reflexive, as documented.
SELECT regionIn(5::UInt32, 5::UInt32, '');

-- First argument a column, second constant: the regions inside USA(2) are 2, 3, 4 and 5.
SELECT number, regionIn(number::UInt32, 2::UInt32, '') FROM numbers(1, 12) ORDER BY number;

-- First argument constant, second a column: Boulder(5) is in 1, 2, 3, 4, 5, 9 and 10.
SELECT number, regionIn(5::UInt32, number::UInt32, '') FROM numbers(1, 12) ORDER BY number;

-- Both arguments columns: 48 of the 144 ordered pairs of 1..12 are in the relation.
SELECT count(), sum(regionIn(n1.number::UInt32, n2.number::UInt32, ''))
FROM numbers(1, 12) AS n1 CROSS JOIN numbers(1, 12) AS n2;

-- The explicit default key '' and the two-argument form agree on every pair.
SELECT countIf(regionIn(n1.number::UInt32, n2.number::UInt32, '') != regionIn(n1.number::UInt32, n2.number::UInt32))
FROM numbers(1, 12) AS n1 CROSS JOIN numbers(1, 12) AS n2;

-- An unknown dictionary key has no hierarchy.
SELECT regionIn(5::UInt32, 1::UInt32, 'xy'); -- { serverError POCO_EXCEPTION }
