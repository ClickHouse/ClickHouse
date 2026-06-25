-- Decimal512 array function regression coverage
SELECT arrayMap(x -> toString(x), arrayCumSum([toDecimal512('1.00', 2), toDecimal512('2.50', 2), toDecimal512('3.25', 2)]));
SELECT toTypeName(arrayCumSum([toDecimal512('1.00', 2), toDecimal512('2.50', 2)]));

SELECT arrayMap(x -> toString(x), arrayCumSumNonNegative([toDecimal512('1.00', 2), toDecimal512('-5.00', 2), toDecimal512('6.00', 2)]));
SELECT toTypeName(arrayCumSumNonNegative([toDecimal512('1.00', 2), toDecimal512('-5.00', 2), toDecimal512('6.00', 2)]));

SELECT arrayMap(x -> toString(x), arrayDifference([toDecimal512('1.00', 2), toDecimal512('1.50', 2), toDecimal512('2.10', 2)]));
SELECT toTypeName(arrayDifference([toDecimal512('1.00', 2), toDecimal512('1.50', 2), toDecimal512('2.10', 2)]));

SELECT arrayMap(x -> toString(x), arrayCompact([toDecimal512('1.00', 2) AS x, x, toDecimal512('2.50', 2) AS y, y, y, toDecimal512('3.00', 2)]));
SELECT toTypeName(arrayCompact([toDecimal512('1.00', 2) AS x, x, toDecimal512('2.50', 2) AS y, y, y, toDecimal512('3.00', 2)]));

SELECT toString(arrayElement([toDecimal512('1.00', 2), toDecimal512('2.00', 2), toDecimal512('3.00', 2)], 2));
SELECT toTypeName(arrayElement([toDecimal512('1.00', 2), toDecimal512('2.00', 2)], 1));
