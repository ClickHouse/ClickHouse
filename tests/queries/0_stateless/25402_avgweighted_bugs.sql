SELECT avgWeighted(CAST(-398., 'Decimal(38, 10)'), CAST(0.05453896522521973, 'Decimal(38,10)')) as x;
SELECT avgWeighted(CAST(-398., 'Decimal(38, 10)'), CAST(0.05453896522521973, 'Float64')) as x;
SELECT avgWeighted(CAST(-398., 'Float64'), CAST(0.05453896522521973, 'Decimal(38,10)')) as x;
SELECT avgWeighted(CAST(-398., 'Float64'), CAST(0.05453896522521973, 'Float64')) as x;
SELECT avgWeighted(toDecimal128(1, 38), toDecimal128(1, 38)) as x;
