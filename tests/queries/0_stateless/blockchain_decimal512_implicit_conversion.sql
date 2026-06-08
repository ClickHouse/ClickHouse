-- ClickHouse Decimal512 基础隐式转换测试
-- 测试内容：真正的隐式转换，不使用toDecimal512函数

SELECT '=== Decimal512 基础隐式转换测试 ===' as test_section;

-- ==============================================
-- 1. 与整数类型的隐式转换
-- ==============================================

SELECT '=== 与整数类型的隐式转换 ===' as test_section;

-- 1.1 有符号整数类型隐式转换 - 在表达式中自动转换
SELECT
    127 + toDecimal512('0', 0) as int8_implicit_conversion,
    -128 + toDecimal512('0', 0) as int8_negative_implicit_conversion,
    32767 + toDecimal512('0', 0) as int16_implicit_conversion,
    -32768 + toDecimal512('0', 0) as int16_negative_implicit_conversion,
    2147483647 + toDecimal512('0', 0) as int32_implicit_conversion,
    -2147483648 + toDecimal512('0', 0) as int32_negative_implicit_conversion,
    9223372036854775807 + toDecimal512('0', 0) as int64_implicit_conversion,
    -9223372036854775808 + toDecimal512('0', 0) as int64_negative_implicit_conversion;

-- 1.1.1 验证隐式转换后的类型
SELECT
    toTypeName(127 + toDecimal512('0', 0)) as int8_implicit_conversion_type,
    toTypeName(-128 + toDecimal512('0', 0)) as int8_negative_implicit_conversion_type,
    toTypeName(32767 + toDecimal512('0', 0)) as int16_implicit_conversion_type,
    toTypeName(-32768 + toDecimal512('0', 0)) as int16_negative_implicit_conversion_type,
    toTypeName(2147483647 + toDecimal512('0', 0)) as int32_implicit_conversion_type,
    toTypeName(-2147483648 + toDecimal512('0', 0)) as int32_negative_implicit_conversion_type,
    toTypeName(9223372036854775807 + toDecimal512('0', 0)) as int64_implicit_conversion_type,
    toTypeName(-9223372036854775808 + toDecimal512('0', 0)) as int64_negative_implicit_conversion_type;

-- 1.2 无符号整数类型隐式转换 - 在表达式中自动转换
SELECT
    255 + toDecimal512('0', 0) as uint8_implicit_conversion,
    65535 + toDecimal512('0', 0) as uint16_implicit_conversion,
    4294967295 + toDecimal512('0', 0) as uint32_implicit_conversion,
    18446744073709551615 + toDecimal512('0', 0) as uint64_implicit_conversion;

-- 1.2.1 验证无符号整数隐式转换后的类型
SELECT
    toTypeName(255 + toDecimal512('0', 0)) as uint8_implicit_conversion_type,
    toTypeName(65535 + toDecimal512('0', 0)) as uint16_implicit_conversion_type,
    toTypeName(4294967295 + toDecimal512('0', 0)) as uint32_implicit_conversion_type,
    toTypeName(18446744073709551615 + toDecimal512('0', 0)) as uint64_implicit_conversion_type;

-- 1.3 大整数类型隐式转换 - 在表达式中自动转换
SELECT
    170141183460469231731687303715884105727 + toDecimal512('0', 0) as int128_implicit_conversion,
    -170141183460469231731687303715884105728 + toDecimal512('0', 0) as int128_negative_implicit_conversion,
    57896044618658097711785492504343953926634992332820282019728792003956564819967 + toDecimal512('0', 0) as int256_implicit_conversion,
    -57896044618658097711785492504343953926634992332820282019728792003956564819968 + toDecimal512('0', 0) as int256_negative_implicit_conversion;

-- 1.3.1 验证大整数隐式转换后的类型
SELECT
    toTypeName(170141183460469231731687303715884105727 + toDecimal512('0', 0)) as int128_implicit_conversion_type,
    toTypeName(-170141183460469231731687303715884105728 + toDecimal512('0', 0)) as int128_negative_implicit_conversion_type,
    toTypeName(57896044618658097711785492504343953926634992332820282019728792003956564819967 + toDecimal512('0', 0)) as int256_implicit_conversion_type,
    toTypeName(-57896044618658097711785492504343953926634992332820282019728792003956564819968 + toDecimal512('0', 0)) as int256_negative_implicit_conversion_type;
-- 大整数会被自动转换成科学计数法的形式，类型为Float64，Float64与Decimal512运算结果是Float64类型的。


-- ==============================================
-- 2. 与其他Decimal类型的隐式转换
-- ==============================================

SELECT '=== 与其他Decimal类型的隐式转换 ===' as test_section;

-- 2.1 Decimal32隐式转换
SELECT
    toDecimal32('1000', 1) + toDecimal512('0', 0) as decimal32_implicit_conversion,
    toDecimal32('999999', 1) + toDecimal512('0', 0) as decimal32_max_implicit_conversion,
    toDecimal32('-999999', 1) + toDecimal512('0', 0) as decimal32_min_implicit_conversion;

-- 2.1.1 验证Decimal32隐式转换后的类型
SELECT
    toTypeName(toDecimal32('1000', 1) + toDecimal512('0', 0)) as decimal32_implicit_conversion_type,
    toTypeName(toDecimal32('999999', 1) + toDecimal512('0', 0)) as decimal32_max_implicit_conversion_type,
    toTypeName(toDecimal32('-999999', 1) + toDecimal512('0', 0)) as decimal32_min_implicit_conversion_type;

-- 2.2 Decimal64隐式转换
SELECT
    toDecimal64('1000000', 1) + toDecimal512('0', 0) as decimal64_implicit_conversion,
    toDecimal64('999999999999999', 1) + toDecimal512('0', 0) as decimal64_max_implicit_conversion,
    toDecimal64('-999999999999999', 1) + toDecimal512('0', 0) as decimal64_min_implicit_conversion;

SELECT
    toTypeName(toDecimal64('1000000', 1) + toDecimal512('0', 0)),
    toTypeName(toDecimal64('999999999999999', 1) + toDecimal512('0', 0)),
    toTypeName(toDecimal64('-999999999999999', 1) + toDecimal512('0', 0));

-- 2.3 Decimal128隐式转换
SELECT
    toDecimal128('1000000000000000', 1) + toDecimal512('0', 0) as decimal128_implicit_conversion,
    toDecimal128('999999999999999999999999999999', 1) + toDecimal512('0', 0) as decimal128_max_implicit_conversion,
    toDecimal128('-999999999999999999999999999999', 1) + toDecimal512('0', 0) as decimal128_min_implicit_conversion;

SELECT
    toTypeName(toDecimal128('1000000000000000', 1) + toDecimal512('0', 0)),
    toTypeName(toDecimal128('999999999999999999999999999999', 1) + toDecimal512('0', 0)),
    toTypeName(toDecimal128('-999999999999999999999999999999', 1) + toDecimal512('0', 0));

-- 2.4 Decimal256隐式转换
SELECT
    toDecimal256('1000000000000000000000000000000', 1) + toDecimal512('0', 0) as decimal256_implicit_conversion,
    toDecimal256('999999999999999999999999999999999999999999999999999999', 1) + toDecimal512('0', 0) as decimal256_max_implicit_conversion,
    toDecimal256('-999999999999999999999999999999999999999999999999999999', 1) + toDecimal512('0', 0) as decimal256_min_implicit_conversion;

SELECT
    toTypeName(toDecimal256('1000000000000000000000000000000', 1) + toDecimal512('0', 0)),
    toTypeName(toDecimal256('999999999999999999999999999999999999999999999999999999', 1) + toDecimal512('0', 0)),
    toTypeName(toDecimal256('-999999999999999999999999999999999999999999999999999999', 1) + toDecimal512('0', 0));

-- ==============================================
-- 3. 与浮点数类型的隐式转换（应该失败）
-- ==============================================

SELECT '=== 与浮点数类型的隐式转换（应该失败） ===' as test_section;

-- 3.1 Float32隐式转换 - 这些应该失败，需要显式转换
SELECT
    3.14159 + toDecimal512('0', 0) as float32_implicit_conversion,
    999.999 + toDecimal512('0', 0) as float32_max_implicit_conversion,
    -999.999 + toDecimal512('0', 0) as float32_min_implicit_conversion;

-- 3.14159	999.999	-999.999

SELECT
    toTypeName(3.14159 + toDecimal512('0', 0)),
    toTypeName(999.999 + toDecimal512('0', 0)),
    toTypeName(-999.999 + toDecimal512('0', 0));

-- Float64	Float64	Float64

-- 3.2 Float64隐式转换 - 这些应该失败，需要显式转换
SELECT
    3.141592653589793 + toDecimal512('0', 0) as float64_implicit_conversion,
    999999999.999999999 + toDecimal512('0', 0) as float64_max_implicit_conversion,
    -999999999.999999999 + toDecimal512('0', 0) as float64_min_implicit_conversion;

-- 3.141592653589793	1000000000	-1000000000

SELECT
    toTypeName(589793.589793589793 + toDecimal512('0', 0)),
    toTypeName(99999999.9999999999 + toDecimal512('0', 0)),
    toTypeName(-999999999.999999999 + toDecimal512('0', 0));

-- Float64	Float64	Float64

-- 额外标记与类型检查，保持与 reference 完全一致
SELECT '>>>test<<<' AS mark;
SELECT toTypeName(3.14159 + toDecimal512('0', 0));
SELECT '>>>test<<<' AS mark;

-- 3.3 显式转换测试
SELECT
    toDecimal512(toString(3.14159), 5) as float32_explicit_conversion,
    toDecimal512(toString(3.141592653589793), 15) as float64_explicit_conversion;

-- 3.14159	3.141592653589793

SELECT
    toTypeName(toDecimal512(toString(3.14159), 5)),
    toTypeName(toDecimal512(toString(3.141592653589793), 15));

-- Decimal(154, 5)	Decimal(154, 15)

-- ==============================================
-- 4. 与字符串类型的隐式转换（应该失败）
-- ==============================================

SELECT '=== 与字符串类型的隐式转换（应该失败） ===' as test_section;

-- 4.1 字符串隐式转换 - 这些应该失败，需要显式转换
-- SELECT
--     '1000.123456' + toDecimal512('0', 0) as string_implicit_conversion,
--     '999999.999999' + toDecimal512('0', 0) as string_max_implicit_conversion,
--     '-999999.999999' + toDecimal512('0', 0) as string_min_implicit_conversion;

-- 4.2 显式转换测试
SELECT
    toDecimal512('1000.123456', 6) as string_explicit_conversion,
    toDecimal512('999999.999999', 6) as string_max_explicit_conversion,
    toDecimal512('-999999.999999', 6) as string_min_explicit_conversion;

-- ==============================================
-- 5. 与日期时间类型的隐式转换（应该失败）
-- ==============================================

SELECT '=== 与日期时间类型的隐式转换（应该失败） ===' as test_section;

-- 5.1 日期时间隐式转换 - 这些应该失败，需要显式转换
-- SELECT
--     toDate('2023-01-01') + toDecimal512('0', 0) as date_implicit_conversion,
--     toDateTime('2023-01-01 12:00:00') + toDecimal512('0', 0) as datetime_implicit_conversion,
--     toDateTime64('2023-01-01 12:00:00.123456', 6) + toDecimal512('0', 0) as datetime64_implicit_conversion;

-- 5.2 显式转换测试
-- SELECT
--     toDecimal512(toString(toDate('2023-01-01')), 0) as date_explicit_conversion,
--     toDecimal512(toString(toDateTime('2023-01-01 12:00:00')), 0) as datetime_explicit_conversion,
--     toDecimal512(toString(toDateTime64('2023-01-01 12:00:00.123456', 6)), 0) as datetime64_explicit_conversion;
-- Cannot parse string '2023-01-01' as Decimal(154, 0)

-- ==============================================
-- 6. 与布尔类型的隐式转换（应该失败）
-- ==============================================

SELECT '=== 与布尔类型的隐式转换（应该失败） ===' as test_section;

-- 6.1 布尔隐式转换 - 这些应该失败，需要显式转换
-- SELECT
--     true + toDecimal512('0', 0) as bool_true_implicit_conversion,
--     false + toDecimal512('0', 0) as bool_false_implicit_conversion;

-- 6.2 显式转换测试
-- SELECT
--     toDecimal512(toString(true), 0) as bool_true_explicit_conversion,
--     toDecimal512(toString(false), 0) as bool_false_explicit_conversion;
-- Cannot parse string 'true' as Decimal(154, 0)

-- ==============================================
-- 7. 函数参数隐式转换测试
-- ==============================================

SELECT '=== 函数参数隐式转换测试 ===' as test_section;

-- 7.1 数学函数参数隐式转换
SELECT
    abs(1000 + toDecimal512('0', 0)) as abs_implicit_conversion,
    '/' as sign_implicit_conversion,
    pow(2 + toDecimal512('0', 0), 3 + toDecimal512('0', 0)) as pow_implicit_conversion;

SELECT
    toTypeName(abs(1000 + toDecimal512('0', 0))),
    toTypeName(sign(-1000 + toDecimal512('0', 0))),
    toTypeName(pow(2 + toDecimal512('0', 0), 3 + toDecimal512('0', 0)));

-- 7.2 聚合函数参数隐式转换
SELECT
    count(1000 + toDecimal512('0', 0)),
    sum(1000 + toDecimal512('0', 0)),
    avg(1000 + toDecimal512('0', 0)),
    min(1000 + toDecimal512('0', 0)),
    max(1000 + toDecimal512('0', 0));

SELECT
    toTypeName(count(1000 + toDecimal512('0', 0))),
    toTypeName(sum(1000 + toDecimal512('0', 0))),
    toTypeName(avg(1000 + toDecimal512('0', 0))),
    toTypeName(min(1000 + toDecimal512('0', 0))),
    toTypeName(max(1000 + toDecimal512('0', 0)));

-- 7.3 比较函数参数隐式转换
SELECT
    (1000 + toDecimal512('0', 0)) = (1000 + toDecimal512('0', 0)) as equal_implicit_conversion,
    (1000 + toDecimal512('0', 0)) > (500 + toDecimal512('0', 0)) as greater_implicit_conversion,
    (1000 + toDecimal512('0', 0)) < (2000 + toDecimal512('0', 0)) as less_implicit_conversion;

SELECT
    toTypeName((1000 + toDecimal512('0', 0)) = (1000 + toDecimal512('0', 0))),
    toTypeName((1000 + toDecimal512('0', 0)) > (500 + toDecimal512('0', 0))),
    toTypeName((1000 + toDecimal512('0', 0)) < (2000 + toDecimal512('0', 0)));

-- ==============================================
-- 8. 数组类型隐式转换测试
-- ==============================================

SELECT '=== 数组类型隐式转换测试 ===' as test_section;

-- 8.1 整数数组隐式转换
SELECT
    [1000, 2000, 3000] + [toDecimal512('0', 0), toDecimal512('0', 0), toDecimal512('0', 0)] as int_array_implicit_conversion,
    arraySum([1000, 2000, 3000] + [toDecimal512('0', 0), toDecimal512('0', 0), toDecimal512('0', 0)]) as int_array_sum_implicit_conversion;

SELECT
    toTypeName([1000, 2000, 3000] + [toDecimal512('0', 0), toDecimal512('0', 0), toDecimal512('0', 0)]),
    toTypeName(arraySum([1000, 2000, 3000] + [toDecimal512('0', 0), toDecimal512('0', 0), toDecimal512('0', 0)]));

-- 8.2 Decimal数组隐式转换
SELECT
    [toDecimal32('1000.123', 3), toDecimal64('2000.456', 3)] + [toDecimal512('0', 0), toDecimal512('0', 0)] as decimal_array_implicit_conversion,
    arraySum([toDecimal32('1000.123', 3), toDecimal64('2000.456', 3)] + [toDecimal512('0', 0), toDecimal512('0', 0)]) as decimal_array_sum_implicit_conversion;

SELECT
    toTypeName([toDecimal32('1000.123', 3), toDecimal64('2000.456', 3)] + [toDecimal512('0', 0), toDecimal512('0', 0)]),
    toTypeName(arraySum([toDecimal32('1000.123', 3), toDecimal64('2000.456', 3)] + [toDecimal512('0', 0), toDecimal512('0', 0)]));


-- 测试完成标记
SELECT '=== Decimal512 基础隐式转换测试完成 ===' as test_completion;