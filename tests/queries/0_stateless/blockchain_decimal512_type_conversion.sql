-- ClickHouse Decimal512 类型转换测试 - 区块链场景
-- 测试内容：与其他数据类型的转换

SELECT '=== Decimal512 类型转换测试 - 区块链场景 ===' as test_section;

-- ==============================================
-- 1. 与整数类型转换测试
-- ==============================================

SELECT '=== 与整数类型转换测试 ===' as test_section;

-- 1.1 转换为整数类型
SELECT
    toInt64(toDecimal512('9223372036854775807', 0)) as to_int64_max,
    toInt64(toDecimal512('-9223372036854775808', 0)) as to_int64_min,
    toInt128(toDecimal512('170141183460469231731687303715884105727', 0)) as to_int128_max,
    toInt256(toDecimal512('57896044618658097711785492504343953926634992332820282019728792003956564819967', 0)) as to_int256_max;

-- 1.2 从整数类型转换
SELECT
    toDecimal512(9223372036854775807, 0) as from_int64_max,
    toDecimal512(-9223372036854775808, 0) as from_int64_min,
    toDecimal512(170141183460469231731687303715884105727, 0) as from_int128_max,
    toDecimal512(57896044618658097711785492504343953926634992332820282019728792003956564819967, 0) as from_int256_max;

-- 1.3 无符号整数转换
SELECT
    toUInt64(toDecimal512('18446744073709551615', 0)) as to_uint64_max,
    toUInt128(toDecimal512('340282366920938463463374607431768211455', 0)) as to_uint128_max,
    toUInt256(toDecimal512('115792089237316195423570985008687907853269984665640564039457584007913129639935', 0)) as to_uint256_max;

-- ==============================================
-- 2. 与其他Decimal类型转换测试
-- ==============================================

SELECT '=== 与其他Decimal类型转换测试 ===' as test_section;

-- 2.1 转换为其他Decimal类型
SELECT
    toDecimal32(toDecimal512('2147483647', 0), 0) as to_decimal32,
    toDecimal64(toDecimal512('9223372036854775807', 0), 0) as to_decimal64,
    toDecimal128(toDecimal512('170141183460469231731687303715884105727', 0), 0) as to_decimal128,
    toDecimal256(toDecimal512('57896044618658097711785492504343953926634992332820282019728792003956564819967', 0), 0) as to_decimal256;


-- 2.2 从其他Decimal类型转换
SELECT
    toDecimal512(toDecimal32('999999999', 0), 0) as from_decimal32,
    toDecimal512(toDecimal64('999999999999999999', 0), 0) as from_decimal64,
    toDecimal512(toDecimal128('99999999999999999999999999999999999999', 0), 0) as from_decimal128,
    toDecimal512(toDecimal256('9999999999999999999999999999999999999999999999999999999999999999999999999999', 0), 0) as from_decimal256;


-- ==============================================
-- 3. 与浮点数类型转换测试
-- ==============================================

SELECT '=== 与浮点数类型转换测试 ===' as test_section;

-- 3.1 转换为浮点数类型
SELECT
    toFloat32(toDecimal512('123456789.123456789', 9)) as to_float32,
    toFloat64(toDecimal512('123456789012345678.123456789012345678', 18)) as to_float64;

-- 3.2 从浮点数类型转换
SELECT
    toDecimal512(123456789.123456789, 9) as from_float32,
    toDecimal512(123456789012345678.123456789012345678, 18) as from_float64;

-- ==============================================
-- 4. 与字符串类型转换测试
-- ==============================================

SELECT '=== 与字符串类型转换测试 ===' as test_section;

-- 4.1 转换为字符串类型
SELECT
    toString(toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678', 18)) as to_string,
    formatReadableDecimalSize(toDecimal512('1000000000000000000000.000000000000000000', 18)) as readable_size;

-- 4.2 从字符串类型转换
SELECT
    toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678', 18) as from_string,
    toDecimal512('1000000.000000000000000000', 18) as from_string_simple;

-- ==============================================
-- 5. 十六进制和二进制转换测试
-- ==============================================

SELECT '=== 十六进制和二进制转换测试 ===' as test_section;

-- 5.1 十六进制转换
SELECT
    hex(toDecimal512('255', 0)) as hex_255,
    hex(toDecimal512('65535', 0)) as hex_65535,
    hex(toDecimal512('4294967295', 0)) as hex_4294967295;

-- 5.2 二进制转换
SELECT
    bin(toDecimal512('255', 0)) as bin_255,
    bin(toDecimal512('1024', 0)) as bin_1024,
    bin(toDecimal512('65535', 0)) as bin_65535;

-- 5.3 从十六进制转换
SELECT
    toDecimal512(0xFF, 0) as from_hex_FF,
    toDecimal512(0xFFFF, 0) as from_hex_FFFF,
    toDecimal512(0xFFFFFFFF, 0) as from_hex_FFFFFFFF;

-- ==============================================
-- 6. 精度调整测试
-- ==============================================

SELECT '=== 精度调整测试 ===' as test_section;

-- 6.1 增加精度
SELECT
    toDecimal512('1000000.000000000000000000', 18) as original_18_decimals,
    toDecimal512('1000000.000000000000000000000000000000000000', 36) as increased_to_36_decimals;

-- 6.2 减少精度
SELECT
    toDecimal512('1000000.123456789012345678', 17) as original_18_decimals,
    toDecimal512('1000000.123456789', 8) as reduced_to_9_decimals,
    toDecimal512('1000000.123', 2) as reduced_to_3_decimals;

-- ==============================================
-- 7. 安全转换测试
-- ==============================================

SELECT '=== 安全转换测试 ===' as test_section;

-- 7.1 安全转换 (OrZero)
SELECT
    toDecimal512OrZero('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678', 18) as safe_from_string,
    toDecimal512OrZero('invalid_string', 18) as safe_from_invalid_string;

-- 7.2 安全转换 (OrNull)
SELECT
    toDecimal512OrNull('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678', 18) as safe_from_string_null,
    toDecimal512OrNull('invalid_string', 18) as safe_from_invalid_string_null;

-- 7.3 安全转换 (OrDefault)
SELECT
    toDecimal512OrDefault('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678', 18, toDecimal512('0.000000000000000000', 18)) as safe_from_string_default,
    toDecimal512OrDefault('invalid_string', 18, toDecimal512('0.000000000000000000', 18)) as safe_from_invalid_string_default;

-- ==============================================
-- 8. 区块链场景类型转换测试
-- ==============================================

SELECT '=== 区块链场景类型转换测试 ===' as test_section;

-- 8.1 代币余额转换
SELECT
    toDecimal512('1000000.000000000000000000', 18) as token_balance_decimal512,
    toDecimal128('1000000.000000000000000000', 18) as token_balance_decimal128,
    toFloat64('1000000.000000000000000000') as token_balance_float64,
    toString('1000000.000000000000000000') as token_balance_string;

-- 8.2 Gas费用转换
SELECT
    toDecimal512('21000.000000000000', 12) as gas_fee_decimal512,
    toDecimal64('21000.000000000000', 12) as gas_fee_decimal64,
    toInt64('21000') as gas_fee_int64,
    toUInt64('21000') as gas_fee_uint64;

-- 8.3 区块奖励转换
SELECT
    toDecimal512('6.250000', 6) as block_reward_decimal512,
    toDecimal32('6.250000', 6) as block_reward_decimal32,
    toFloat32('6.25') as block_reward_float32,
    toInt32('6') as block_reward_int32;

-- 测试完成标记
SELECT '=== Decimal512 类型转换测试完成 ===' as test_completion;
