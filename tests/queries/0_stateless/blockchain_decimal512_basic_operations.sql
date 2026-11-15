-- ClickHouse Decimal512 基本操作测试 - 区块链场景
-- 测试内容：基本类型创建、插入、查询功能
-- 区块链场景：代币余额、交易金额、Gas费用、区块奖励

SELECT '=== Decimal512 基本操作测试 - 区块链场景 ===' as test_section;

-- ==============================================
-- 1. 代币余额测试 (100位精度，18位小数)
-- ==============================================

-- SELECT '=== 代币余额测试 (100位精度，18位小数) ===' as test_section;

-- 1.1 基础代币余额创建
DROP TABLE IF EXISTS test_decimal512_table;
CREATE TABLE IF NOT EXISTS test_decimal512_table (
    id UInt32,
    token_balance Decimal512(18),
    transaction_amount Decimal512(8),
    gas_fee Decimal512(12),
    block_reward Decimal512(6)
)ENGINE=Memory;
-- 平常值的插入
INSERT INTO test_decimal512_table VALUES(
    1,
    1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890.123456789012345678,
    12345678901234567890123456789012345678901234567890123456789012345678901234567890.12345678,
    12345678901234567890123456789012345678901234567890.123456789012,
    123456789012345678901234567890123456789012345678901234567890.123456
);
-- 最大值的插入
INSERT INTO test_decimal512_table VALUES(
    2,
    9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999,
    99999999999999999999999999999999999999999999999999999999999999999999999999999999.99999999,
    99999999999999999999999999999999999999999999999999.999999999999,
    999999999999999999999999999999999999999999999999999999999999.999999
);
-- 最小值的插入
INSERT INTO test_decimal512_table VALUES(
    3,
    -9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999,
    -99999999999999999999999999999999999999999999999999999999999999999999999999999999.99999999,
    -99999999999999999999999999999999999999999999999999.999999999999,
    -999999999999999999999999999999999999999999999999999999999999.999999
);
-- 零值的插入
INSERT INTO test_decimal512_table VALUES(
    4, 0, 0, 0, 0
);
-- 四种情况情况的查询
SELECT token_balance, transaction_amount, gas_fee FROM test_decimal512_table WHERE id IN (1, 2, 3, 4) ORDER BY id;
-- 十六进制表示
SELECT '===十六进制===';
SELECT hex(token_balance), hex(transaction_amount), hex(gas_fee) FROM test_decimal512_table ORDER BY id;
INSERT INTO test_decimal512_table VALUES(
    6,
    0xffff,
    0xffff,
    0xffff,
    0xffff
);
SELECT * FROM test_decimal512_table WHERE id = 6;
SELECT '===二进制===';
WITH
    hex(token_balance) AS ht,
    hex(transaction_amount) AS ha,
    hex(gas_fee) AS hg,
    unhex(ht) AS bt,
    unhex(ha) AS ba,
    unhex(hg) AS bg
SELECT
    arrayStringConcat(arrayMap(i -> lpad(bin(reinterpretAsUInt8(substr(bt, i, 1))), 8, '0'), range(1, length(bt) + 1))),
    arrayStringConcat(arrayMap(i -> lpad(bin(reinterpretAsUInt8(substr(ba, i, 1))), 8, '0'), range(1, length(ba) + 1))),
    arrayStringConcat(arrayMap(i -> lpad(bin(reinterpretAsUInt8(substr(bg, i, 1))), 8, '0'), range(1, length(bg) + 1)))
FROM test_decimal512_table
ORDER BY id;
INSERT INTO test_decimal512_table VALUES(
    7,
    bin(255),
    bin(255),
    bin(255),
    bin(255)
);
SELECT * FROM test_decimal512_table WHERE id = 7;
-- 将二进制数据11111111识别为了十进制数据11111111
-- 测试反序列化
-- SELECT token_balance FROM test_decimal512_table FORMAT Native;


SELECT '===溢出插入===';
INSERT INTO test_decimal512_table VALUES(
    5,
    9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.9999999999999999999,
    99999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999,
    99999999999999999999999999999999999999999999999999.9999999999999,
    999999999999999999999999999999999999999999999999999999999999.999999
);
SELECT id, token_balance, transaction_amount, gas_fee FROM test_decimal512_table WHERE id = 5;
-- 对于溢出的精度自动丢弃
-- SELECT
--     toDecimal512('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890.123456789012345678', 18) as token_balance_1,
--     toDecimal512('9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18) as token_balance_max,
--     toDecimal512('0.000000000000000001', 18) as token_balance_min,
--     toDecimal512('0', 18) as token_balance_zero;

-- 1.2 常见代币余额场景
-- SELECT
--     toDecimal512('1000000.000000000000000000', 18) as usdt_balance,  -- 100万USDT
--     toDecimal512('21000000.000000000000000000', 18) as btc_supply,   -- 2100万BTC
--     toDecimal512('1000000000000000000.000000000000000000', 18) as eth_wei,  -- 1 ETH in wei
--     toDecimal512('500000000000000000000.000000000000000000', 18) as large_balance;  -- 500 ETH

-- ==============================================
-- 2. 交易金额测试 (80位精度，8位小数)
-- ==============================================

SELECT '=== 交易金额测试 (80位精度，8位小数) ===' as test_section;

-- 2.1 基础交易金额创建
-- SELECT
--     toDecimal512('12345678901234567890123456789012345678901234567890123456789012345678901234567890.12345678', 8) as transaction_amount_1,
--     toDecimal512('99999999999999999999999999999999999999999999999999999999999999999999999999999999.99999999', 8) as transaction_amount_max,
--     toDecimal512('0.00000001', 8) as transaction_amount_min,
--     toDecimal512('0', 8) as transaction_amount_zero;

-- 2.2 常见交易金额场景
-- SELECT
--     toDecimal512('1000000.00000000', 8) as large_transfer,     -- 100万代币转账
--     toDecimal512('0.00000001', 8) as dust_amount,             -- 粉尘金额
--     toDecimal512('50000.12345678', 8) as medium_transfer,     -- 5万代币转账
--     toDecimal512('99999999999999999999999999999999999999999999999999999999999999999999999999999999.99999999', 8) as max_transfer;

-- ==============================================
-- 3. Gas费用测试 (50位精度，12位小数)
-- ==============================================

SELECT '=== Gas费用测试 (50位精度，12位小数) ===' as test_section;

-- 3.1 基础Gas费用创建
-- SELECT
--     toDecimal512('12345678901234567890123456789012345678901234567890.123456789012', 12) as gas_fee_1,
--     toDecimal512('99999999999999999999999999999999999999999999999999.999999999999', 12) as gas_fee_max,
--     toDecimal512('0.000000000001', 12) as gas_fee_min,
--     toDecimal512('0', 12) as gas_fee_zero;

-- 3.2 常见Gas费用场景
-- SELECT
--     toDecimal512('21000.000000000000', 12) as simple_transfer_gas,    -- 简单转账Gas
--     toDecimal512('100000.000000000000', 12) as contract_call_gas,     -- 合约调用Gas
--     toDecimal512('500000.000000000000', 12) as complex_contract_gas,  -- 复杂合约Gas
--     toDecimal512('0.000000000001', 12) as minimal_gas;               -- 最小Gas费用

-- ==============================================
-- 4. 区块奖励测试 (60位精度，6位小数)
-- ==============================================

SELECT '=== 区块奖励测试 (60位精度，6位小数) ===' as test_section;

-- 4.1 基础区块奖励创建
-- SELECT
--     toDecimal512('123456789012345678901234567890123456789012345678901234567890.123456', 6) as block_reward_1,
--     toDecimal512('999999999999999999999999999999999999999999999999999999999999.999999', 6) as block_reward_max,
--     toDecimal512('0.000001', 6) as block_reward_min,
--     toDecimal512('0', 6) as block_reward_zero;

-- 4.2 常见区块奖励场景
-- SELECT
--     toDecimal512('6.250000', 6) as btc_block_reward,          -- BTC区块奖励
--     toDecimal512('2.000000', 6) as eth_block_reward,          -- ETH区块奖励
--     toDecimal512('0.000001', 6) as minimal_reward,            -- 最小奖励
--     toDecimal512('1000000.000000', 6) as large_reward;        -- 大额奖励

-- ==============================================
-- 5. 边界值测试
-- ==============================================

SELECT '=== 边界值测试 ===' as test_section;

-- 5.1 最大值测试
-- SELECT
--     toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) as max_154_digits,
--     toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18) as max_with_18_decimals,
--     toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999', 12) as max_with_12_decimals;

-- 5.2 最小值测试
-- SELECT
--     toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) as min_154_digits,
--     toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18) as min_with_18_decimals,
--     toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999', 12) as min_with_12_decimals;

-- 5.3 零值和单位值测试
-- SELECT
--     toDecimal512('0', 0) as zero_integer,
--     toDecimal512('0.0', 1) as zero_decimal,
--     toDecimal512('1', 0) as one_integer,
--     toDecimal512('1.0', 1) as one_decimal,
--     toDecimal512('-1', 0) as negative_one_integer,
--     toDecimal512('-1.0', 1) as negative_one_decimal;

-- ==============================================
-- 6. 十六进制和二进制表示测试
-- ==============================================

SELECT '=== 十六进制和二进制表示测试 ===' as test_section;

-- 6.1 十六进制转换测试
-- SELECT
--     toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 0) as decimal_value,
--     hex(toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 0)) as hex_representation;

-- 6.2 二进制转换测试
-- SELECT
--     toDecimal512('255', 0) as decimal_255,
--     bin(toDecimal512('255', 0)) as binary_255,
--     toDecimal512('1024', 0) as decimal_1024,
--     bin(toDecimal512('1024', 0)) as binary_1024;

-- ==============================================
-- 7. 反序列化测试
-- ==============================================

SELECT '=== 反序列化测试 ===' as test_section;

-- 7.1 从字符串反序列化
-- SELECT
--     toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678', 18) as from_string_1,
--     toDecimal512('-987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321.987654321098765432', 18) as from_string_2;

-- SELECT fromString('123.456', 'Decimal512(3)');
-- SELECT toTypeName(fromString('123.456', 'Decimal512(3)'));

-- SELECT JSONExtractDecimal('"transaction_amount":12345678901234567890123456789012345678901234567890123456789012345678901234567890.12345678901234567890', 'transaction_amount');

-- 7.2 从数值反序列化
-- SELECT
--     toDecimal512(123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789, 0) as from_number_1,
--     toDecimal512(-987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321098765432109876543210987654321, 0) as from_number_2;

-- ==============================================
-- 8. 区块链场景综合测试
-- ==============================================

SELECT '=== 区块链场景综合测试 ===' as test_section;

-- 8.1 模拟交易数据
-- SELECT
--     toDecimal512('1000000.000000000000000000', 18) as token_balance,
--     toDecimal512('50000.12345678', 8) as transaction_amount,
--     toDecimal512('21000.000000000000', 12) as gas_fee,
--     toDecimal512('6.250000', 6) as block_reward;

-- 8.2 模拟DeFi场景
-- SELECT
--     toDecimal512('1000000000000000000000.000000000000000000', 18) as liquidity_pool,  -- 1000 ETH
--     toDecimal512('500000000000000000000.000000000000000000', 18) as user_deposit,     -- 500 ETH
--     toDecimal512('0.003000000000000000', 18) as trading_fee,                         -- 0.3% 手续费
--     toDecimal512('1000000.000000000000', 12) as gas_cost;                           -- Gas成本

-- ==============================================
-- 9. 异常情况测试
-- ==============================================

SELECT '=== 异常情况测试 ===' as test_section;

-- 9.1 越界值插入测试
INSERT INTO test_decimal512_table VALUES(
    8,
    toDecimal512('99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.9999999999999999999', 18),
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999', 8),
    toDecimal512('999999999999999999999999999999999999999999999999999.9999999999999', 12),
    toDecimal512('999999999999999999999999999999999999999999999999999999999999.999999', 6)
);
SELECT * FROM test_decimal512_table WHERE id = 8;
-- 9.2 无效格式插入测试
INSERT INTO test_decimal512_table VALUES(
    9,
    '123',
    '234',
    '345',
    '456'
);
-- Cannot parse string '\n' as Decimal(154, 8)
-- Cannot parse string '*' as Decimal(154, 12)
-- Cannot parse string '12@3' as Decimal(154, 6)
SELECT * FROM test_decimal512_table WHERE id = 9;
-- 9.3 特殊值插入测试
-- INSERT INTO test_decimal512_table VALUES(
--     10,
--     '',
--     ' ',
--     '  ',
--     '     '
-- );
-- Attempt to read after eof: while executing
-- 'FUNCTION if(isNull(-dummy-0) : 3, defaultValueOfTypeName('Decimal(154, 18)') :: 2, _CAST(-dummy-0, 'Decimal(154, 18)') :: 4) -> if(isNull(-dummy-0), defaultValueOfTypeName('Decimal(154, 18)'), _CAST(-dummy-0, 'Decimal(154, 18)')... Decimal(154, 18) : 1': While executing ValuesBlockInputFormat: data for INSERT was parsed from query. (ATTEMPT_TO_READ_AFTER_EOF) (version 25.7.6.1)(query:
-- 9.3 特殊值插入测试
-- SELECT * FROM test_decimal512_table WHERE id = 10;
-- 9.4 批量插入异常测试
-- INSERT INTO test_decimal512_table VALUES(
--     11,
--     'invalid',
--     'invalid',
--     'invalid',
--     'invalid'
-- );
-- Cannot parse string 'invalid' as Decimal(154, 18)
-- SELECT * FROM test_decimal512_table WHERE id = 11;
-- 9.5 数据完整性验证

SELECT *
FROM system.parts
WHERE table = 'test_decimal512_table';

SELECT toString(cityHash64(*))
FROM test_decimal512_table
ORDER BY id;


-- ==============================================
-- 10. 数组插入测试
-- ==============================================

DROP TABLE IF EXISTS array_insertion_test;
CREATE TABLE IF NOT EXISTS array_insertion_test(
    id UInt32,
    decimal_arr Array(Decimal512(3))
)ENGINE=Memory;

-- 10.1 插入数组
INSERT INTO array_insertion_test VALUES
(1, [toDecimal512(123.123, 3), toDecimal512(345.345, 3), toDecimal512(678.678, 3)]),
(2, [toDecimal512(23.123, 3), toDecimal512(78.567, 3), toDecimal512(888.888, 3)]);

SELECT id, decimal_arr, decimal_arr[1] FROM array_insertion_test WHERE id IN (1, 2);

-- 10.2 边界值插入
INSERT INTO array_insertion_test VALUES
(3, [toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999', 3), toDecimal512(0, 0), toDecimal512(0, 0)]);

SELECT * FROM array_insertion_test WHERE id = 3;

-- 10.3 越界值插入
-- INSERT INTO array_insertion_test VALUES
-- (4, [toDecimal512('9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999', 3), toDecimal512(0, 0), toDecimal512(0, 0)]);
-- INSERT INTO array_insertion_test VALUES
-- (4, ['*', NULL]);

-- 超大数导致 toDecimal512 转成 Decimal(154,3) 时溢出 → 返回 NULL → 非 Nullable 列不能插 NULL。

-- SELECT * FROM array_insertion_test WHERE id = 4;
-- Cannot convert NULL value to non-Nullable type


-- ==============================================
-- 11. 索引创建测试
-- ==============================================

-- 添加跳过索引
DROP TABLE IF EXISTS index_test;
CREATE TABLE IF NOT EXISTS index_test(
    id UInt32,
    transaction_amount Decimal512(8),
    INDEX transaction_amount_index transaction_amount TYPE minmax GRANULARITY 2
)ENGINE=MergeTree
ORDER BY id;

INSERT INTO index_test VALUES
(1, toDecimal512('999999.99999999', 8)),
(2, toDecimal512('100000.00000001', 8)),
(3, toDecimal512('123456.12345678', 8)),
(4, toDecimal512('987654.98765432', 8)),
(5, toDecimal512('333333.33333333', 8)),
(6, toDecimal512('666666.66666666', 8)),
(7, toDecimal512('456789.45678901', 8)),
(8, toDecimal512('765432.12345678', 8)),
(9, toDecimal512('543210.34567891', 8)),
(10, toDecimal512('888888.88888888', 8));

-- 11.1 使用跳过索引查询
SELECT * FROM index_test WHERE transaction_amount BETWEEN toDecimal512('99999', 0) AND toDecimal512('500000', 0);


DROP TABLE IF EXISTS test_decimal512_table;
-- 测试完成标记
SELECT '=== Decimal512 基本操作测试完成 ===' as test_completion;