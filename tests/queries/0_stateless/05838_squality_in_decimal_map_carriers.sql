SELECT
    map('a', toFloat64(0.1)) IN (materialize(map('a', toDecimal32('0.1', 1))), NULL),
    map('a', toFloat64(0.1)) IN (materialize(CAST(map('a', toDecimal32('0.1', 1)) AS Dynamic)), NULL),
    map('a', toFloat64(0.1)) IN
        (materialize(CAST(map('a', toDecimal32('0.1', 1)) AS Variant(Map(String, Decimal32(1)), String))), NULL)
SETTINGS in_null_as_unknown = 1;
