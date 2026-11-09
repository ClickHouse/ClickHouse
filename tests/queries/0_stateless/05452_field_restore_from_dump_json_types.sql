-- `Field::restoreFromDump` (src/Core/Field.cpp) rebuilds a Field from the debug text that
-- `FieldVisitorDump` produces. Its only SQL surface is `formatQueryFromJSON`: the JSON AST
-- reader `JSONObjectReader::readFieldFromObject` (src/Parsers/ASTJSONReadHelpers.cpp) handles
-- the JSON-native field types itself (Null, UInt64, Int64, Float64, Bool, String, Array,
-- Tuple, Map) and routes every other `field_type` through `Field::restoreFromDump`, so the
-- restored value becomes the re-formatted query text. After the restore the reader compares
-- `Field::getTypeName()` with the declared `field_type`, which is why an `Array_`/`Tuple_`/
-- `Map_` payload is fully parsed and only then rejected with BAD_ARGUMENTS.
-- Expected behaviour source: the `formatQueryFromJSON` FunctionDocumentation block in
-- src/Functions/formatQueryFromJSON.cpp ("Takes a JSON representation of a SQL AST ... and
-- formats it back into a SQL query string").
-- CI had zero hits on the wide-integer, Decimal, Array_/Tuple_/Map_, AggregateFunctionState_
-- and Number_ branches of the dump parser.

-- 1. Wide integers at their boundary values: the dump text is parsed back with the exact
--    width and re-emitted as the decoded number (Int128 max, UInt128 max, Int256 min,
--    UInt256 max). A truncating parse would change the printed digits.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"Int128","value":"Int128_170141183460469231731687303715884105727"}},{"name":"b","value":{"field_type":"UInt128","value":"UInt128_340282366920938463463374607431768211455"}},{"name":"c","value":{"field_type":"Int256","value":"Int256_-57896044618658097711785492504343953926634992332820282019728792003956564819968"}},{"name":"d","value":{"field_type":"UInt256","value":"UInt256_115792089237316195423570985008687907853269984665640564039457584007913129639935"}}]}$$);

-- 2. Decimals of all four widths. The `e0` exponent is required: the dump parser reads the
--    payload with `readQuoted(DecimalField)`, whose digit scanner stops at the exponent.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"Decimal32","value":"Decimal32_'1.25e0'"}},{"name":"b","value":{"field_type":"Decimal64","value":"Decimal64_'-2.5e0'"}},{"name":"c","value":{"field_type":"Decimal128","value":"Decimal128_'3.75e0'"}},{"name":"d","value":{"field_type":"Decimal256","value":"Decimal256_'4.125e0'"}}]}$$);

-- 3. An AggregateFunctionState dump carries the function name and the state blob separately;
--    only the blob survives into the formatted query.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"AggregateFunctionState","value":"AggregateFunctionState_('sum', 'ab')"}}]}$$);

-- 4. The same dump without the name/data separator is malformed.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"AggregateFunctionState","value":"AggregateFunctionState_('sum')"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }

-- 5. A `Number_` dump keeps the literal spelling of an unparsed numeric token.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"Number","value":"Number_1e5"}}]}$$);

-- 6. Well-formed Array_/Tuple_/Map_ dumps: the elements are parsed recursively and the
--    resulting Array/Tuple/Map is then rejected by the reader's type-mismatch guard, because
--    a declared `field_type` of `Array`/`Tuple`/`Map` never reaches the dump parser at all.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Array_[Int64_1, Int64_2]"}}]}$$); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Tuple_(Int64_1, UInt64_2)"}}]}$$); -- { serverError BAD_ARGUMENTS }
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Map_('k', Int64_2)"}}]}$$); -- { serverError BAD_ARGUMENTS }

-- 7. Unterminated Array_/Tuple_/Map_ dumps: no closing bracket, so the element scan runs out
--    of input.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Array_[Int64_1"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Tuple_(Int64_1"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Map_('k', Int64_2"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }

-- 8. Trailing garbage after the closing bracket must not be silently ignored.
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Array_[Int64_1] x"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Tuple_(Int64_1) x"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
SELECT formatQueryFromJSON($${"type":"SetQuery","changes":[{"name":"a","value":{"field_type":"UUID","value":"Map_('k', Int64_2) x"}}]}$$); -- { serverError CANNOT_RESTORE_FROM_FIELD_DUMP }
