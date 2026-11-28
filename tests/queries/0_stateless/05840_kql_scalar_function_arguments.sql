-- Argument-shape validation in the KQL scalar function table, src/Parsers/Kusto/KQLFunctions.cpp.
--
-- A builder in `scalarFunctions()` checks the shape of its arguments before it builds the
-- ClickHouse AST: the parity of a `case` list, whether an optional `kind` is a string literal it
-- knows, whether a capture group is an integer literal, whether a unit name is one it can map.
-- A shape it cannot translate yields a null AST, and `KQLParser::parseFunctionCall` reports that
-- as a parse error. So each rejected call below fails with SYNTAX_ERROR instead of being
-- mistranslated into a query that fails at execution time, or worse, quietly answers something
-- else. The error comes out of the parser, which runs in the client, hence `clientError`.
--
-- Expected behaviour is the Kusto reference for case(), countof(), extract(), datetime_add(),
-- datetime_part(), datetime_diff(), geo_distance_2points(), ipv4_is_match(), set_difference() and
-- array_iff(), as summarised in the comments next to each entry of `scalarFunctions()`.

SET enable_kusto_dialect = 1;
SET dialect = 'kusto';

-- `case(p1, v1, ..., pn, vn, else)` carries a mandatory final `else`, so a legal call always has
-- an odd number of arguments. Four is enough arguments, but the `else` is the one missing.
print '-- case --';
print case(1 > 2, 'a', 3 > 4, 'b');                                  -- { clientError SYNTAX_ERROR }

-- The optional `kind` of `countof` selects substring or regex counting at translation time, so it
-- has to be one of the two names, written as a literal.
print '-- countof --';
print countof('abc', 'b', 3);                                        -- { clientError SYNTAX_ERROR }
print countof('abc', 'b', 'fuzzy');                                  -- { clientError SYNTAX_ERROR }

-- `extract(regex, captureGroup, text [, typeof(T)])` indexes the capture group at translation
-- time, so the group has to be a non-negative integer literal, and the optional type a literal
-- too. A leading minus is an operator in KQL, so `-1` is an expression, not a literal; a hex
-- literal is the one form that reads as unsigned, so 0xFFFFFFFFFFFFFFFF does arrive as a literal
-- and is the negative long -1.
print '-- extract --';
print extract('(a)', 'x', 'abc');                                    -- { clientError SYNTAX_ERROR }
print extract('(a)', -1, 'abc');                                     -- { clientError SYNTAX_ERROR }
print extract('(a)', 0xFFFFFFFFFFFFFFFF, 'abc');                     -- { clientError SYNTAX_ERROR }
print extract('(a)', 1, 'abc', 5);                                   -- { clientError SYNTAX_ERROR }

-- The unit of `datetime_add`, `datetime_part` and `datetime_diff` picks the ClickHouse function,
-- so it must be a string literal, and one of the units that has a mapping.
print '-- datetime units --';
print datetime_add(1, 2, datetime(2026-01-01));                       -- { clientError SYNTAX_ERROR }
print datetime_add('fortnight', 1, datetime(2026-01-01));             -- { clientError SYNTAX_ERROR }
print datetime_part(1, datetime(2026-01-01));                         -- { clientError SYNTAX_ERROR }
print datetime_part('nanosec', datetime(2026-01-01));                 -- { clientError SYNTAX_ERROR }
print datetime_diff(1, datetime(2026-01-02), datetime(2026-01-01));   -- { clientError SYNTAX_ERROR }

-- The optional fifth argument of `geo_distance_2points` asks for the ellipsoid formula instead of
-- the sphere, which is a different ClickHouse function, so it must be a bool literal.
print '-- geo_distance_2points --';
print geo_distance_2points(0, 0, 1, 1, 5);                            -- { clientError SYNTAX_ERROR }

-- The shapes that do translate, for the same three checks seen from the other side.
print '-- accepted shapes --';
-- An explicit prefix narrows the one taken from the two addresses: at /24 these two match.
print ipv4_is_match('10.1.2.3', '10.1.2.255', 24);
-- `set_difference` subtracts every further array from the first one.
print set_difference(dynamic([1, 2, 3, 4]), dynamic([1]), dynamic([2]));
-- `array_iff` picks element-wise from the second or the third array.
print array_iff(dynamic([true, false]), dynamic([1, 2]), dynamic([10, 20]));

SET dialect = 'clickhouse';
