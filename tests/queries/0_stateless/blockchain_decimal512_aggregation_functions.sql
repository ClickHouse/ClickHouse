-- ClickHouse Decimal512 聚合函数测试 - 区块链场景
-- 测试内容：聚合操作、统计函数
-- 区块链场景：模拟真实的区块链交易数据

SELECT '=== Decimal512 聚合函数测试 - 区块链场景 ===' as test_section;

DROP TABLE IF EXISTS test_decimal512_table;
CREATE TABLE IF NOT EXISTS test_decimal512_table (
    id UInt32,
    token_balance Decimal512(18),
    transaction_amount Decimal512(8),
    gas_fee Decimal512(12)
)ENGINE=Memory;

INSERT INTO test_decimal512_table VALUES
(1, toDecimal512(1000, 0), toDecimal512(500, 0), toDecimal512(5, 0)),
(2, toDecimal512(2000, 0), toDecimal512(500, 0), toDecimal512(5, 0)),
(3, toDecimal512(1000, 0), toDecimal512(5000, 0), toDecimal512(50, 0)),
(4, toDecimal512(10000, 0), toDecimal512(1000, 0), toDecimal512(10, 0)),
(5, toDecimal512(1000, 0), toDecimal512(50, 0), toDecimal512(5, 0)),
(6, toDecimal512(5000, 0), toDecimal512(200, 0), toDecimal512(2, 0)),
(7, toDecimal512(5000, 0), toDecimal512(5000, 0), toDecimal512(60, 0)),
(8, toDecimal512(30000, 0), toDecimal512(2000, 0), toDecimal512(20, 0)),
(9, toDecimal512(100, 0), toDecimal512(200, 0), toDecimal512(2, 0)),
(10, toDecimal512(1000, 0), toDecimal512(10, 0), toDecimal512(1, 0));


-- ==============================================
-- 1. 基本聚合函数测试
-- ==============================================

SELECT '=== 基本聚合函数测试 ===' as test_section;

-- 1.1 计数聚合 (count)
SELECT
    count(token_balance)
FROM test_decimal512_table;

-- 1.2 求和聚合 (sum)
SELECT
    sum(token_balance)
FROM test_decimal512_table;

-- 1.3 平均值聚合 (avg)
SELECT
    avg(token_balance)
FROM test_decimal512_table;

-- 1.4 最小值聚合 (min)
SELECT
    min(token_balance)
FROM test_decimal512_table;

-- 1.5 最大值聚合 (max)
SELECT
    max(token_balance)
FROM test_decimal512_table;

-- 测试溢出
INSERT INTO test_decimal512_table VALUES
(11, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(12, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(13, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(14, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(15, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(16, toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(17, toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(18, toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(19, toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(20, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999.999999999999999999', 18), 0, 0);
INSERT INTO test_decimal512_table VALUES
(21, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(22, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(23, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(24, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(25, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(26, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(27, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(28, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(29, 0, 0, 0);
INSERT INTO test_decimal512_table VALUES
(30, 0, 0, 0);

-- 1.1 计数聚合溢出 (count)
SELECT
    count(token_balance)
FROM test_decimal512_table;

-- 1.2 求和聚合溢出 (sum)
SELECT
    sum(token_balance)
FROM test_decimal512_table;

-- 1.3 平均值聚合溢出 (avg)
SELECT
    avg(token_balance)
FROM test_decimal512_table;

-- 1.4 最小值聚合溢出 (min)
SELECT
    min(token_balance)
FROM test_decimal512_table;

-- 1.5 最大值聚合溢出 (max)
SELECT
    max(token_balance)
FROM test_decimal512_table;

DROP TABLE IF EXISTS test_decimal512_table;
-- ==============================================
-- 2. 分组聚合测试
-- ==============================================
DROP TABLE IF EXISTS test_group_aggregation;
CREATE TABLE IF NOT EXISTS test_group_aggregation(
    block_number String,
    wallet_address String,
    transaction_amount Decimal512(8),
    token_balance Decimal512(18),
    transaction_time Datetime
)ENGINE=Memory;

INSERT INTO test_group_aggregation VALUES
('block1000', '0x1234', toDecimal512(1234.12345678, 8), toDecimal512(2222.3333333333333333333, 18),
'2025-09-22 11:00:00'),
('block1000', '0x1235', toDecimal512(234.23456789, 8), toDecimal512(555.3333333444444444444, 18),
'2025-09-22 11:30:00'),
('block1001', '0x2345', toDecimal512(1234.12345678, 8), toDecimal512(4567.123456789012345678, 18),
'2025-09-22 11:30:00'),
('block1002', '0x1234', toDecimal512(333.33333333, 8), toDecimal512(9876.987654321098765432, 18),
'2025-09-22 12:00:00'),
('block1002', '0x2346', toDecimal512(2345.34567890, 8), toDecimal512(9999.999999999999999999, 18),
'2025-09-22 12:00:00'),
('block1003', '0x1235', toDecimal512(9876.98765432, 8), toDecimal512(1234.123456789012345678, 18),
'2025-09-22 13:00:00'),
('block1003', '0x2346', toDecimal512(1234.98765432, 8), toDecimal512(3456.123456789012345678, 18),
'2025-09-22 13:30:00'),
('block1003', '0x1234', toDecimal512(9999.99999999, 8), toDecimal512(9999.9999999999999999999, 18),
'2025-09-22 13:30:00'),
('block1004', '0x2345', toDecimal512(2345.34567890, 8), toDecimal512(4444.3333333333333333333, 18),
'2025-09-22 14:00:00'),
('block1005', '0x3456', toDecimal512(4567.45678901, 8), toDecimal512(6666.9999999999333333333, 18),
'2025-09-22 14:00:00');

SELECT '=== 分组聚合测试 ===' as test_section;

-- 2.1 按区块号分组聚合
SELECT
    block_number AS block_number,
    sum(transaction_amount) AS total_amount,
    avg(transaction_amount) AS avg_amount
FROM test_group_aggregation
GROUP BY block_number
ORDER BY multiIf(
    block_number = 'block1005', 1,
    block_number = 'block1002', 2,
    block_number = 'block1003', 3,
    block_number = 'block1004', 4,
    block_number = 'block1000', 5,
    block_number = 'block1001', 6,
    7
);

-- 2.2 按地址分组聚合
SELECT
    wallet_address AS wallet_address,
    sum(transaction_amount) AS total_amount,
    avg(transaction_amount) AS avg_amount
FROM test_group_aggregation
GROUP BY wallet_address
ORDER BY multiIf(
    wallet_address = '0x1234', 1,
    wallet_address = '0x3456', 2,
    wallet_address = '0x1235', 3,
    wallet_address = '0x2346', 4,
    wallet_address = '0x2345', 5,
    6
);

-- ==============================================
-- 3. 条件聚合测试
-- ==============================================

SELECT '=== 条件聚合测试 ===' as test_section;

-- 3.1 条件计数 (countIf)
SELECT
    countIf(block_number == 'block_1000')
FROM test_group_aggregation;

-- 3.2 条件求和 (sumIf)
SELECT
    sumIf(transaction_amount, wallet_address == '0x1234')
FROM test_group_aggregation;

-- 3.3 条件平均值 (avgIf)
SELECT
    avgIf(transaction_amount, wallet_address == '0x2345')
FROM test_group_aggregation;

-- ==============================================
-- 4. 分位数函数测试
-- ==============================================

SELECT '=== 分位数函数测试 ===' as test_section;

-- 4.1 中位数 (quantile) - 使用arraySort和数组索引计算
SELECT
    quantile(0.5)(transaction_amount)
FROM test_group_aggregation;

-- 4.2 四分位数 - 使用arraySort和数组索引计算
SELECT
    quantiles(0.25, 0.5, 0.75)(transaction_amount)
FROM test_group_aggregation;

-- 4.3 百分位数 - 使用arraySort和数组索引计算
SELECT
    quantile(0.99)(transaction_amount)
FROM test_group_aggregation;

-- ==============================================
-- 5. 标准差和方差测试
-- ==============================================

SELECT '=== 标准差和方差测试 ===' as test_section;

-- 5.1 标准差 (stddevPop) - 使用手动计算
SELECT
    stddevPop(transaction_amount)
FROM test_group_aggregation;

-- 5.2 方差 (varPop) - 使用手动计算
SELECT
    varPop(transaction_amount)
FROM test_group_aggregation;

-- ==============================================
-- 6. 协方差和相关性测试
-- ==============================================

SELECT '=== 协方差和相关性测试 ===' as test_section;

-- 6.1 协方差 (covarPop) - 使用手动计算
-- SELECT
--     covarPop(transaction_amount, token_balance)
-- FROM test_group_aggregation;

-- SELECT
--     covarPop(toDecimal256(transaction_amount, 8), toDecimal256(token_balance, 8))
-- FROM test_group_aggregation;

-- SELECT
--     covarPop(toDecimal128(transaction_amount, 8), toDecimal128(token_balance, 8))
-- FROM test_group_aggregation;
-- decimal512, decimal256, decimal128都不支持

-- 6.2 相关性 (corr) - 使用手动计算
-- SELECT
--     corr(transaction_amount, token_balance)
-- FROM test_group_aggregation;

-- SELECT
--     corr(toDecimal256(transaction_amount, 8), toDecimal256(token_balance, 8))
-- FROM test_group_aggregation;

-- SELECT
--     corr(toDecimal128(transaction_amount, 8), toDecimal128(token_balance, 8))
-- FROM test_group_aggregation;
-- decimal512, decimal256, decimal128都不支持

-- ==============================================
-- 7. 时间窗口聚合测试
-- ==============================================

SELECT '=== 时间窗口聚合测试 ===' as test_section;

-- 7.1 滑动窗口求和
SELECT
    toDateTime(window_start),
    sum(transaction_amount) AS total_amount
FROM
(
    SELECT
        arrayJoin(
            range(
                toUnixTimestamp(transaction_time) - 3600,
                toUnixTimestamp(transaction_time),
                600
            )
        ) AS window_start,
        transaction_amount
    FROM test_group_aggregation
)
GROUP BY window_start
ORDER BY window_start;
SELECT '---------------------------------------';
-- 7.2 滑动窗口平均值
SELECT
    toDateTime(window_start),
    avg(transaction_amount) AS total_amount
FROM
(
    SELECT
        arrayJoin(
            range(
                toUnixTimestamp(transaction_time) - 3600,
                toUnixTimestamp(transaction_time),
                600
            )
        ) AS window_start,
        transaction_amount
    FROM test_group_aggregation
)
GROUP BY window_start
ORDER BY window_start;


-- ==============================================
-- 8. 区块链场景综合聚合测试
-- ==============================================

SELECT '=== 区块链场景综合聚合测试 ===' as test_section;

-- 8.1 模拟交易数据聚合
SELECT
    toDateTime(window_start),
    sum(transaction_amount),
    avg(transaction_amount),
    max(transaction_amount),
    min(transaction_amount)
FROM (
    SELECT
        arrayJoin(
            range(
                toUnixTimestamp(transaction_time) - 3600,
                toUnixTimestamp(transaction_time),
                3600
            )
        ) AS window_start,
        transaction_amount
    FROM test_group_aggregation
)
GROUP BY window_start
ORDER BY window_start;

-- 测试完成标记
SELECT '=== Decimal512 聚合函数测试完成 ===' as test_completion;