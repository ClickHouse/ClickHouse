-- ClickHouse Decimal512 边界情况和异常隐式转换测试 - 区块链场景
-- 测试内容：边界情况、异常处理、类型推断等

SELECT '=== Decimal512 边界情况和异常隐式转换测试 - 区块链场景 ===' as test_section;

-- ==============================================
-- 1. 整数溢出隐式转换测试
-- ==============================================

SELECT '=== 整数溢出隐式转换测试 ===' as test_section;

-- 1.1 有符号整数最大值与Decimal512的隐式转换
SELECT
    127 + toDecimal512(0, 0) as int8_max_to_decimal512,
    32767 + toDecimal512(0, 0) as int16_max_to_decimal512,
    2147483647 + toDecimal512(0, 0) as int32_max_to_decimal512,
    9223372036854775807 + toDecimal512(0, 0) as int64_max_to_decimal512,
    toInt128('170141183460469231731687303715884105727') + toDecimal512(0, 0) as int128_max_to_decimal512,
    toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') + toDecimal512(0, 0) as int256_max_to_decimal512;

SELECT
    toTypeName(127 + toDecimal512(0, 0)) AS int8_max_to_decimal512,
    toTypeName(32767 + toDecimal512(0, 0)) AS int16_max_to_decimal512,
    toTypeName(2147483647 + toDecimal512(0, 0)) AS int32_max_to_decimal512,
    toTypeName(9223372036854775807 + toDecimal512(0, 0)) AS int64_max_to_decimal512,
    toTypeName(toInt128('170141183460469231731687303715884105727') + toDecimal512(0, 0)) AS int128_max_to_decimal512,
    toTypeName(toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') + toDecimal512(0, 0)) AS int256_max_to_decimal512;

-- 1.2 有符号整数最小值与Decimal512的隐式转换
SELECT
    -128 + toDecimal512(0, 0) as int8_min_to_decimal512,
    -32768 + toDecimal512(0, 0) as int16_min_to_decimal512,
    -2147483648 + toDecimal512(0, 0) as int32_min_to_decimal512,
    -9223372036854775808 + toDecimal512(0, 0) as int64_min_to_decimal512,
    toInt128('-170141183460469231731687303715884105728') + toDecimal512(0, 0) as int128_min_to_decimal512,
    toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') + toDecimal512(0, 0) as int256_min_to_decimal512;

SELECT
    toTypeName(-128 + toDecimal512(0, 0)) AS int8_min_to_decimal512,
    toTypeName(-32768 + toDecimal512(0, 0)) AS int16_min_to_decimal512,
    toTypeName(-2147483648 + toDecimal512(0, 0)) AS int32_min_to_decimal512,
    toTypeName(-9223372036854775808 + toDecimal512(0, 0)) AS int64_min_to_decimal512,
    toTypeName(toInt128('-170141183460469231731687303715884105728') + toDecimal512(0, 0)) AS int128_min_to_decimal512,
    toTypeName(toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') + toDecimal512(0, 0)) AS int256_min_to_decimal512;

-- 1.3 无符号整数最大值与Decimal512的隐式转换
SELECT
    255 + toDecimal512(0, 0) as uint8_max_to_decimal512,
    65535 + toDecimal512(0, 0) as uint16_max_to_decimal512,
    4294967295 + toDecimal512(0, 0) as uint32_max_to_decimal512,
    18446744073709551615 + toDecimal512(0, 0) as uint64_max_to_decimal512,
    toUInt128('340282366920938463463374607431768211455') + toDecimal512(0, 0) as uint128_max_to_decimal512,
    toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') + toDecimal512(0, 0) as uint256_max_to_decimal512;

SELECT
    toTypeName(255 + toDecimal512(0, 0)) AS uint8_max_to_decimal512,
    toTypeName(65535 + toDecimal512(0, 0)) AS uint16_max_to_decimal512,
    toTypeName(4294967295 + toDecimal512(0, 0)) AS uint32_max_to_decimal512,
    toTypeName(18446744073709551615 + toDecimal512(0, 0)) AS uint64_max_to_decimal512,
    toTypeName(toUInt128('340282366920938463463374607431768211455') + toDecimal512(0, 0)) AS uint128_max_to_decimal512,
    toTypeName(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') + toDecimal512(0, 0)) AS uint256_max_to_decimal512;

-- 1.4 整数溢出边界测试
SELECT
    9223372036854775807 + toDecimal512(1, 0) as int64_max_plus_one,
    -9223372036854775808 - toDecimal512(1, 0) as int64_min_minus_one,
    18446744073709551615 + toDecimal512(1, 0) as uint64_max_plus_one;

SELECT
    toTypeName(9223372036854775807 + toDecimal512(1, 0)) AS int64_max_plus_one,
    toTypeName(-9223372036854775808 - toDecimal512(1, 0)) AS int64_min_minus_one,
    toTypeName(18446744073709551615 + toDecimal512(1, 0)) AS uint64_max_plus_one;

-- ==============================================
-- 2. 精度丢失隐式转换测试
-- ==============================================

SELECT '=== 精度丢失隐式转换测试 ===' as test_section;

-- 2.1 高精度到低精度转换
SELECT
    toDecimal512('1000000.123456789012345678', 18) as original_18_precision,
    toDecimal512('1000000.123456789012345678', 10) as reduced_to_10_precision,
    toDecimal512('1000000.123456789012345678', 5) as reduced_to_5_precision,
    toDecimal512('1000000.123456789012345678', 0) as reduced_to_0_precision;

SELECT
    toTypeName(toDecimal512('1000000.123456789012345678', 18)) AS original_18_precision,
    toTypeName(toDecimal512('1000000.123456789012345678', 10)) AS reduced_to_10_precision,
    toTypeName(toDecimal512('1000000.123456789012345678', 5)) AS reduced_to_5_precision,
    toTypeName(toDecimal512('1000000.123456789012345678', 0)) AS reduced_to_0_precision;

-- 2.2 低精度到高精度转换
SELECT
    toDecimal512('1000000.123', 3) as original_3_precision,
    toDecimal512('1000000.123', 10) as extended_to_10_precision,
    toDecimal512('1000000.123', 18) as extended_to_18_precision;
    -- toDecimal512('1000000.123', 154) as extended_to_154_precision;

SELECT
    toTypeName(toDecimal512('1000000.123', 3)) AS original_3_precision,
    toTypeName(toDecimal512('1000000.123', 10)) AS extended_to_10_precision,
    toTypeName(toDecimal512('1000000.123', 18)) AS extended_to_18_precision;
    -- toTypeName(toDecimal512('1000000.123', 154)) AS extended_to_154_precision;

-- 2.3 精度截断测试
SELECT
    toDecimal512('999999999.999999999999999999', 18) as max_18_precision,
    toDecimal512('999999999.999999999999999999', 10) as truncated_to_10_precision,
    toDecimal512('999999999.999999999999999999', 5) as truncated_to_5_precision,
    toDecimal512('999999999.999999999999999999', 0) as truncated_to_0_precision;

SELECT
    toTypeName(toDecimal512('999999999.999999999999999999', 18)) AS max_18_precision,
    toTypeName(toDecimal512('999999999.999999999999999999', 10)) AS truncated_to_10_precision,
    toTypeName(toDecimal512('999999999.999999999999999999', 5)) AS truncated_to_5_precision,
    toTypeName(toDecimal512('999999999.999999999999999999', 0)) AS truncated_to_0_precision;

-- 2.4 精度舍入测试
SELECT
    round(toDecimal512('999999999.999999999999999999', 18), 10) as rounded_to_10_precision,
    round(toDecimal512('999999999.999999999999999999', 18), 5) as rounded_to_5_precision,
    round(toDecimal512('999999999.999999999999999999', 18), 0) as rounded_to_0_precision;

SELECT
    toTypeName(round(toDecimal512('999999999.999999999999999999', 18), 10)) AS rounded_to_10_precision,
    toTypeName(round(toDecimal512('999999999.999999999999999999', 18), 5)) AS rounded_to_5_precision,
    toTypeName(round(toDecimal512('999999999.999999999999999999', 18), 0)) AS rounded_to_0_precision;

-- ==============================================
-- 3. 科学计数法隐式转换测试
-- ==============================================

SELECT '=== 科学计数法隐式转换测试 ===' as test_section;

-- 3.1 科学计数法字符串显式转换
SELECT
    toDecimal512('1e18', 18) as scientific_1e18,
    toDecimal512('1e-18', 18) as scientific_1e_18,
    toDecimal512('1.5e10', 18) as scientific_1_5e10,
    toDecimal512('2.5e-5', 18) as scientific_2_5e_5;

SELECT
    toTypeName(toDecimal512('1e18', 18)) AS scientific_1e18,
    toTypeName(toDecimal512('1e-18', 18)) AS scientific_1e_18,
    toTypeName(toDecimal512('1.5e10', 18)) AS scientific_1_5e10,
    toTypeName(toDecimal512('2.5e-5', 18)) AS scientific_2_5e_5;

-- 3.2 大科学计数法转换
-- SELECT
--     toDecimal512('1e100', 18) as scientific_1e100,
--     toDecimal512('1e150', 18) as scientific_1e150,
--     toDecimal512('1e154', 18) as scientific_1e154;
    -- toDecimal512('1e154', 18) as scientific_1e154_overflow


-- 3.3 小科学计数法转换
-- SELECT
--     toDecimal512('1e-100', 18) as scientific_1e_100,
--     toDecimal512('1e-150', 18) as scientific_1e_150,
--     toDecimal512('1e-154', 18) as scientific_1e_154,
--     toDecimal512('1e-154', 18) as scientific_1e_154_underflow;

-- 3.4 科学计数法精度测试
-- SELECT
    -- toDecimal512('1.234567890123456789e18', 18) as scientific_with_precision,
    -- toDecimal512('1.234567890123456789e10', 18) as scientific_with_precision_10,
    -- toDecimal512('1.234567890123456789e5', 18) as scientific_with_precision_5,
    -- toDecimal512('1.234567890123456789e0', 18) as scientific_with_precision_0;

-- ==============================================
-- 4. 无效字符串隐式转换测试
-- ==============================================

SELECT '=== 无效字符串隐式转换测试 ===' as test_section;

-- 4.1 无效字符串显式转换（应该失败）
-- SELECT
--     toDecimal512('invalid_string', 18) as invalid_string_conversion,
--     toDecimal512('1000000.000000000000000000abc', 18) as mixed_string_conversion,
--     toDecimal512('1000000..000000000000000000', 18) as double_decimal_conversion,
--     toDecimal512('1000000.000000000000000000!@#', 18) as special_char_conversion;
-- 报错，符合预期

-- 4.2 空字符串和空白字符串转换
-- SELECT
--     toDecimal512('', 18) as empty_string_conversion,
--     toDecimal512('   ', 18) as whitespace_string_conversion,
--     toDecimal512('\t', 18) as tab_string_conversion,
--     toDecimal512('\n', 18) as newline_string_conversion;
-- 报错，符合预期

-- 4.3 格式错误字符串转换
SELECT
    toDecimal512('1000000.000000000000000000.', 18) as trailing_decimal_conversion,
    toDecimal512('.1000000.000000000000000000', 18) as leading_decimal_conversion,
    toDecimal512('1000000.000000000000000000.123', 18) as multiple_decimal_conversion;
    -- toDecimal512('1000000.000000000000000000 123', 18) as space_in_number_conversion;

SELECT
    toTypeName(toDecimal512('1000000.000000000000000000.', 18)) AS trailing_decimal_conversion,
    toTypeName(toDecimal512('.1000000.000000000000000000', 18)) AS leading_decimal_conversion,
    toTypeName(toDecimal512('1000000.000000000000000000.123', 18)) AS multiple_decimal_conversion;
    -- toTypeName(toDecimal512('1000000.000000000000000000 123', 18)) AS space_in_number_conversion;

-- 4.4 安全转换函数测试
SELECT
    toDecimal512OrZero('invalid_string', 18) as safe_invalid_string_to_zero,
    toDecimal512OrZero('', 18) as safe_empty_string_to_zero,
    toDecimal512OrZero('   ', 18) as safe_whitespace_string_to_zero,
    toDecimal512OrNull('invalid_string', 18) as safe_invalid_string_to_null,
    toDecimal512OrNull('', 18) as safe_empty_string_to_null,
    toDecimal512OrNull('   ', 18) as safe_whitespace_string_to_null,
    toDecimal512OrDefault('invalid_string', 18, toDecimal512('0.000000000000000000', 18)) as safe_invalid_string_to_default,
    toDecimal512OrDefault('', 18, toDecimal512('0.000000000000000000', 18)) as safe_empty_string_to_default,
    toDecimal512OrDefault('   ', 18, toDecimal512('0.000000000000000000', 18)) as safe_whitespace_string_to_default;

SELECT
    toTypeName(toDecimal512OrZero('invalid_string', 18)) AS safe_invalid_string_to_zero,
    toTypeName(toDecimal512OrZero('', 18)) AS safe_empty_string_to_zero,
    toTypeName(toDecimal512OrZero('   ', 18)) AS safe_whitespace_string_to_zero,
    toTypeName(toDecimal512OrNull('invalid_string', 18)) AS safe_invalid_string_to_null,
    toTypeName(toDecimal512OrNull('', 18)) AS safe_empty_string_to_null,
    toTypeName(toDecimal512OrNull('   ', 18)) AS safe_whitespace_string_to_null,
    toTypeName(toDecimal512OrDefault('invalid_string', 18, toDecimal512('0.000000000000000000', 18))) AS safe_invalid_string_to_default,
    toTypeName(toDecimal512OrDefault('', 18, toDecimal512('0.000000000000000000', 18))) AS safe_empty_string_to_default,
    toTypeName(toDecimal512OrDefault('   ', 18, toDecimal512('0.000000000000000000', 18))) AS safe_whitespace_string_to_default;

-- ==============================================
-- 5. 边界值隐式转换测试
-- ==============================================

SELECT '=== 边界值隐式转换测试 ===' as test_section;

-- 5.1 零值隐式转换测试
SELECT
    toDecimal512(0, 0) as zero_int_to_decimal512,
    toDecimal512(0, 18) as zero_int_to_decimal512_18,
    toDecimal512('0', 18) as zero_string_to_decimal512,
    toDecimal512('0.0', 18) as zero_decimal_string_to_decimal512,
    toDecimal512('0.000000000000000000', 18) as zero_full_precision_string_to_decimal512;

SELECT
    toTypeName(toDecimal512(0, 0)) AS zero_int_to_decimal512,
    toTypeName(toDecimal512(0, 18)) AS zero_int_to_decimal512_18,
    toTypeName(toDecimal512('0', 18)) AS zero_string_to_decimal512,
    toTypeName(toDecimal512('0.0', 18)) AS zero_decimal_string_to_decimal512,
    toTypeName(toDecimal512('0.000000000000000000', 18)) AS zero_full_precision_string_to_decimal512;

-- 5.2 极小值隐式转换测试
SELECT
    toDecimal512(1, 0) as min_positive_int_to_decimal512,
    toDecimal512(-1, 0) as min_negative_int_to_decimal512,
    toDecimal512('0.000000000000000001', 18) as min_positive_decimal_to_decimal512,
    toDecimal512('-0.000000000000000001', 18) as min_negative_decimal_to_decimal512;
    -- toDecimal512('0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001', 154) as min_positive_154_precision_to_decimal512;

SELECT
    toTypeName(toDecimal512(1, 0)) AS min_positive_int_to_decimal512,
    toTypeName(toDecimal512(-1, 0)) AS min_negative_int_to_decimal512,
    toTypeName(toDecimal512('0.000000000000000001', 18)) AS min_positive_decimal_to_decimal512,
    toTypeName(toDecimal512('-0.000000000000000001', 18)) AS min_negative_decimal_to_decimal512;
    -- toTypeName(toDecimal512('0.000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001', 154)) AS min_positive_154_precision_to_decimal512;

-- 5.3 极大值隐式转换测试
SELECT
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) as max_154_precision_to_decimal512,
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18) as max_154_precision_18_scale_to_decimal512;
    -- toDecimal512('0.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 154) as max_154_precision_154_scale_to_decimal512;

SELECT
    toTypeName(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS max_154_precision_to_decimal512,
    toTypeName(toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18)) AS max_154_precision_18_scale_to_decimal512;
    -- toTypeName(toDecimal512('9.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 154)) AS max_154_precision_154_scale_to_decimal512;

-- 5.4 负值隐式转换测试
SELECT
    toDecimal512(-1, 0) as negative_one_int_to_decimal512,
    toDecimal512(-1000000, 0) as negative_million_int_to_decimal512,
    toDecimal512('-1000000.000000000000000000', 18) as negative_million_string_to_decimal512,
    toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) as negative_max_154_precision_to_decimal512;

SELECT
    toTypeName(toDecimal512(-1, 0)) AS negative_one_int_to_decimal512,
    toTypeName(toDecimal512(-1000000, 0)) AS negative_million_int_to_decimal512,
    toTypeName(toDecimal512('-1000000.000000000000000000', 18)) AS negative_million_string_to_decimal512,
    toTypeName(toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)) AS negative_max_154_precision_to_decimal512;

-- ==============================================
-- 6. 函数参数类型推断测试
-- ==============================================

SELECT '=== 函数参数类型推断测试 ===' as test_section;

-- 6.1 数学函数参数类型推断
SELECT
    abs(1000000) as abs_int_inference,
    abs(toDecimal512('1000000.000000000000000000', 18)) as abs_decimal512_inference,
    abs(-1000000) as abs_negative_int_inference,
    abs(toDecimal512('-1000000.000000000000000000', 18)) as abs_negative_decimal512_inference,
    sign(1000000) as sign_int_inference,
    sign(toDecimal512('1000000.000000000000000000', 18)) as sign_decimal512_inference,
    sign(-1000000) as sign_negative_int_inference,
    sign(toDecimal512('-1000000.000000000000000000', 18)) as sign_negative_decimal512_inference;

SELECT
    toTypeName(abs(1000000)) AS abs_int_inference,
    toTypeName(abs(toDecimal512('1000000.000000000000000000', 18))) AS abs_decimal512_inference,
    toTypeName(abs(-1000000)) AS abs_negative_int_inference,
    toTypeName(abs(toDecimal512('-1000000.000000000000000000', 18))) AS abs_negative_decimal512_inference,
    toTypeName(sign(1000000)) AS sign_int_inference,
    toTypeName(sign(toDecimal512('1000000.000000000000000000', 18))) AS sign_decimal512_inference,
    toTypeName(sign(-1000000)) AS sign_negative_int_inference,
    toTypeName(sign(toDecimal512('-1000000.000000000000000000', 18))) AS sign_negative_decimal512_inference;

-- 6.2 聚合函数参数类型推断
SELECT
    arraySum([1000000, 500000, 2000000]) as array_sum_int_inference,
    arraySum([toDecimal512('1000000.000000000000000000', 18),
              toDecimal512('500000.000000000000000000', 18),
              toDecimal512('2000000.000000000000000000', 18)]) as array_sum_decimal512_inference,
    arrayAvg([1000000, 500000, 2000000]) as array_avg_int_inference,
    arrayAvg([toDecimal512('1000000.000000000000000000', 18),
              toDecimal512('500000.000000000000000000', 18),
              toDecimal512('2000000.000000000000000000', 18)]) as array_avg_decimal512_inference;

SELECT
    toTypeName(arraySum([1000000, 500000, 2000000])) AS array_sum_int_inference,
    toTypeName(arraySum([toDecimal512('1000000.000000000000000000', 18),
                         toDecimal512('500000.000000000000000000', 18),
                         toDecimal512('2000000.000000000000000000', 18)])) AS array_sum_decimal512_inference,
    toTypeName(arrayAvg([1000000, 500000, 2000000])) AS array_avg_int_inference,
    toTypeName(arrayAvg([toDecimal512('1000000.000000000000000000', 18),
                         toDecimal512('500000.000000000000000000', 18),
                         toDecimal512('2000000.000000000000000000', 18)])) AS array_avg_decimal512_inference;

-- 6.3 比较函数参数类型推断
SELECT
    (1000000 > 500000) as comparison_int_inference,
    (toDecimal512('1000000.000000000000000000', 18) > toDecimal512('500000.000000000000000000', 18)) as comparison_decimal512_inference,
    (1000000 = 1000000) as equality_int_inference,
    (toDecimal512('1000000.000000000000000000', 18) = toDecimal512('1000000.000000000000000000', 18)) as equality_decimal512_inference;

SELECT
    toTypeName(1000000 > 500000) AS comparison_int_inference,
    toTypeName(toDecimal512('1000000.000000000000000000', 18) > toDecimal512('500000.000000000000000000', 18)) AS comparison_decimal512_inference,
    toTypeName(1000000 = 1000000) AS equality_int_inference,
    toTypeName(toDecimal512('1000000.000000000000000000', 18) = toDecimal512('1000000.000000000000000000', 18)) AS equality_decimal512_inference;

-- ==============================================
-- 7. 数组类型隐式转换测试
-- ==============================================

SELECT '=== 数组类型隐式转换测试 ===' as test_section;

-- 7.1 混合类型数组隐式转换
SELECT
    [1000000, toDecimal512('500000.000000000000000000', 18), 2000000] as mixed_int_decimal512_array,
    [toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)] as mixed_decimal512_int_array,
    [toDecimal512('1000000.000000000000000000', 18), toDecimal512('500000.000000000000000000', 18), toDecimal512('2000000.000000000000000000', 18)] as pure_decimal512_array;

SELECT
    toTypeName([1000000, toDecimal512('500000.000000000000000000', 18), 2000000]) AS mixed_int_decimal512_array,
    toTypeName([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)]) AS mixed_decimal512_int_array,
    toTypeName([toDecimal512('1000000.000000000000000000', 18), toDecimal512('500000.000000000000000000', 18), toDecimal512('2000000.000000000000000000', 18)]) AS pure_decimal512_array;

-- 7.2 数组函数隐式转换测试
SELECT
    arraySum([1000000, toDecimal512('500000.000000000000000000', 18), 2000000]) as array_sum_mixed_types,
    arrayAvg([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)]) as array_avg_mixed_types,
    arrayMax([1000000, toDecimal512('500000.000000000000000000', 18), 2000000]) as array_max_mixed_types,
    arrayMin([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)]) as array_min_mixed_types;

SELECT
    toTypeName(arraySum([1000000, toDecimal512('500000.000000000000000000', 18), 2000000])) AS array_sum_mixed_types,
    toTypeName(arrayAvg([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)])) AS array_avg_mixed_types,
    toTypeName(arrayMax([1000000, toDecimal512('500000.000000000000000000', 18), 2000000])) AS array_max_mixed_types,
    toTypeName(arrayMin([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)])) AS array_min_mixed_types;

-- 7.3 数组排序隐式转换测试
SELECT
    arraySort([1000000, toDecimal512('500000.000000000000000000', 18), 2000000]) as array_sort_mixed_types,
    arrayReverseSort([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)]) as array_reverse_sort_mixed_types;

SELECT
    toTypeName(arraySort([1000000, toDecimal512('500000.000000000000000000', 18), 2000000])) AS array_sort_mixed_types,
    toTypeName(arrayReverseSort([toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)])) AS array_reverse_sort_mixed_types;

-- 7.4 数组过滤隐式转换测试
SELECT
    arrayFilter(x -> x > 1000000, [1000000, toDecimal512('500000.000000000000000000', 18), 2000000]) as array_filter_mixed_types,
    arrayFilter(x -> x > toDecimal512('1000000.000000000000000000', 18), [toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)]) as array_filter_decimal512_condition;

SELECT
    toTypeName(arrayFilter(x -> x > 1000000, [1000000, toDecimal512('500000.000000000000000000', 18), 2000000])) AS array_filter_mixed_types,
    toTypeName(arrayFilter(x -> x > toDecimal512('1000000.000000000000000000', 18), [toDecimal512('1000000.000000000000000000', 18), 500000, toDecimal512('2000000.000000000000000000', 18)])) AS array_filter_decimal512_condition;

-- 测试完成标记
SELECT '=== Decimal512 边界情况和异常隐式转换测试完成 ===' as test_completion;