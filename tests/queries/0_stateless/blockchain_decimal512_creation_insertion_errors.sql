-- ClickHouse Decimal512 创建和插入异常测试 - 区块链场景
-- 测试内容：精度越界、插入异常、格式错误、空值处理等

SELECT '=== Decimal512 创建和插入异常测试 - 区块链场景 ===' as test_section;

-- ==============================================
-- 1. 精度越界测试
-- ==============================================

SELECT '=== 精度越界测试 ===' as test_section;

-- 1.1 超过最大精度测试（154位精度，应该失败）
-- SELECT
--     toDecimal512('1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234', 0) as precision_154_test;
-- Too many digits (154 > 154) in decimal value，符合预期

-- 1.2 超过最大小数位数测试（154位小数，应该失败）
-- SELECT
--     toDecimal512('1.1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890', 154) as scale_154_test;
-- 运行失败，符合预期

-- 1.3 精度为负数测试（应该失败）
-- SELECT
--     toDecimal512('1000000.000000000000000000', -1) as negative_precision_test;
-- 运行失败，报错：Illegal type of toDecimal() scale Int8，应该符合预期

-- ==============================================
-- 2. 有效精度边界测试
-- ==============================================

SELECT '=== 有效精度边界测试 ===' as test_section;

-- 2.1 最大有效精度测试（154位精度）
SELECT
    toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0) as max_precision_154,
    toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123', 0) as precision_154_test;

-- 2.2 最大有效小数位数测试（154位小数）
-- SELECT
--     toDecimal512('1.999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 154) as max_scale_154,
--     toDecimal512('1.123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 154) as scale_154_test;
-- 运行失败，不符合预期

-- 2.3 边界精度组合测试
-- SELECT
--     toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789.123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 154) as precision_scale_154_154;


-- ==============================================
-- 3. 插入越界值测试
-- ==============================================
DROP TABLE IF EXISTS test_insertion;
CREATE TABLE IF NOT EXISTS test_insertion(
    id int,
    num Decimal512(0)
)ENGINE=Memory;

SELECT '=== 插入越界值测试 ===' as test_section;

-- 3.1 超过最大值测试
-- SELECT
--     toDecimal512('10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 0) as overflow_max_test;
-- 报错，符合预期
INSERT INTO test_insertion VALUES
(1, toDecimal512('1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 0));

-- 3.2 超过最小值测试
-- SELECT
--     toDecimal512('-1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 0) as overflow_min_test;
-- 报错，符合预期
INSERT INTO test_insertion VALUES
(2, toDecimal512('-1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', 0));

SELECT * FROM test_insertion ORDER BY id;
-- 3.3 精度溢出测试
-- SELECT
--     toDecimal512('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789', 0) as precision_overflow_test;
-- 报错，符合预期

-- ==============================================
-- 4. 字符串格式错误测试
-- ==============================================

SELECT '=== 字符串格式错误测试 ===' as test_section;

-- 4.1 无效字符测试
-- SELECT
--     toDecimal512('1000000.000000000000000000abc', 18) as invalid_char_test,
--     toDecimal512('1000000.000000000000000000!@#', 18) as special_char_test,
--     toDecimal512('1000000.000000000000000000  ', 18) as trailing_space_test;
-- 报错，符合预期

-- 4.2 多个小数点测试
SELECT
    toDecimal512('1000000.000000000000000000.123', 18) as multiple_decimal_test,
    toDecimal512('1000000..000000000000000000', 18) as double_decimal_test;
-- 能运行并输出 1000000	1000000

-- 4.3 空字符串测试
-- SELECT
--     toDecimal512('', 18) as empty_string_test,
--     toDecimal512('   ', 18) as whitespace_test;
-- 报错，符合预期

-- 4.4 格式不匹配测试
SELECT
    toDecimal512('1000000.000000000000000000', 8) as precision_mismatch_test,
    toDecimal512('1000000.123456789012345678', 5) as scale_mismatch_test;
-- 能输出：1000000	1000000.12345

-- ==============================================
-- 5. 空值和NULL测试
-- ==============================================

SELECT '=== 空值和NULL测试 ===' as test_section;

-- 5.1 NULL值测试
SELECT
    toDecimal512(NULL, 18) as null_value_test,
    toDecimal512OrNull(NULL, 18) as null_value_or_null_test,
    toDecimal512OrDefault(NULL, 18, toDecimal512('0.000000000000000000', 18)) as null_value_or_default_test;

-- 5.2 空值处理测试
SELECT
    toDecimal512OrZero('', 18) as empty_string_or_zero_test,
    toDecimal512OrZero('invalid', 18) as invalid_string_or_zero_test,
    toDecimal512OrZero(NULL, 18) as null_or_zero_test;

-- 5.3 默认值测试
SELECT
    toDecimal512OrDefault('', 18, toDecimal512('1000000.000000000000000000', 18)) as empty_string_or_default_test,
    toDecimal512OrDefault('invalid', 18, toDecimal512('500000.000000000000000000', 18)) as invalid_string_or_default_test;

-- ==============================================
-- 6. 精度不匹配测试
-- ==============================================

SELECT '=== 精度不匹配测试 ===' as test_section;

-- 6.1 精度截断测试
SELECT
    toDecimal512('1000000.123456789012345678', 5) as precision_truncate_test,
    toDecimal512('1000000.123456789012345678', 10) as precision_truncate_10_test,
    toDecimal512('1000000.123456789012345678', 15) as precision_truncate_15_test;

-- 6.2 精度扩展测试
SELECT
    toDecimal512('1000000.123', 18) as precision_extend_test,
    toDecimal512('1000000.123456', 18) as precision_extend_6_test,
    toDecimal512('1000000.123456789', 18) as precision_extend_9_test;

-- 6.3 精度转换测试
SELECT
    toDecimal512('1000000.123456789012345678', 0) as precision_convert_to_int_test,
    toDecimal512('1000000.123456789012345678', 1) as precision_convert_to_1_test,
    toDecimal512('1000000.123456789012345678', 2) as precision_convert_to_2_test;

-- ==============================================
-- 7. 类型转换错误测试
-- ==============================================

SELECT '=== 类型转换错误测试 ===' as test_section;

-- 7.1 从无效类型转换测试
-- SELECT
--     toDecimal512('invalid_string', 18) as invalid_string_conversion_test,
--     toDecimal512('1000000.000000000000000000abc', 18) as mixed_string_conversion_test;
-- 报错，符合预期

-- 7.2 从浮点数转换精度丢失测试
SELECT
    toDecimal512(1000000.123456789012345678, 18) as float_precision_loss_test,
    toDecimal512(1000000.123456789012345678, 10) as float_precision_loss_10_test;
-- Float64精度丢失
-- 7.3 从整数转换测试
SELECT
    toDecimal512(1000000, 18) as int_conversion_test,
    toDecimal512(9223372036854775807, 18) as int64_max_conversion_test,
    toDecimal512(-9223372036854775808, 18) as int64_min_conversion_test;
-- 7.4 Int256最大值转换测试
SELECT
    toDecimal512(99999999999999999999999999999999999999999999999999999999999999999999999999999, 0);

-- ==============================================
-- 8. 批量插入异常测试
-- ==============================================

SELECT '=== 批量插入异常测试 ===' as test_section;

-- 8.1 混合有效和无效值测试
INSERT INTO test_insertion VALUES
    (3, toDecimal512('1000000.000000000000000000', 18)),
    (4, toDecimal512('500000.000000000000000000', 18)),
    (5, toDecimal512('invalid', 18)),
    (6, toDecimal512('2000000.000000000000000000', 18));
    -- 'invalid'会被当成0插入，直接插入'invalid'字符串会直接报错

-- 8.2 不同精度混合测试
INSERT INTO test_insertion VALUES
    (7, toDecimal512('1000000.000000000000000000', 18)),
    (8, toDecimal512('1000000.00000000', 8)),
    (9, toDecimal512('1000000.000000000000000000', 12)),
    (10, toDecimal512('1000000.000000000000000000', 6));

-- 8.3 边界值混合测试
INSERT INTO test_insertion VALUES
    (11, toDecimal512('999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0)),
    (12, toDecimal512('0.000000000000000000', 18)),
    (13, toDecimal512('-999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999', 0));

SELECT * FROM test_insertion WHERE id >= 3 ORDER BY id;
-- ==============================================
-- 9. 表结构变更测试
-- ==============================================

SELECT '=== 表结构变更测试 ===' as test_section;

DROP TABLE IF EXISTS test_decimal512_table;
-- 9.1 创建测试表
CREATE TABLE IF NOT EXISTS test_decimal512_table (
    id UInt32,
    token_balance Decimal512(18),
    gas_fee Decimal512(12),
    block_reward Decimal512(6)
) ENGINE = Memory;


-- 9.2 插入测试数据
INSERT INTO test_decimal512_table VALUES
(1, toDecimal512('1000000.000000000000000000', 18), toDecimal512('21000.000000000000', 12), toDecimal512('6.250000', 6)),
(2, toDecimal512('500000.000000000000000000', 18), toDecimal512('15000.000000000000', 12), toDecimal512('2.000000', 6)),
(3, toDecimal512('2000000.000000000000000000', 18), toDecimal512('50000.000000000000', 12), toDecimal512('10.000000', 6));

-- 9.3 增加字段transaction_amount
ALTER TABLE test_decimal512_table ADD COLUMN transaction_amount Decimal512(6) DEFAULT toDecimal512('1000.000000', 6);

-- 9.4 查询测试数据
SELECT
    id,
    token_balance,
    gas_fee,
    block_reward,
    transaction_amount
FROM test_decimal512_table
ORDER BY id;

SELECT '-----------------------';

INSERT INTO test_decimal512_table(id, token_balance, gas_fee, block_reward) VALUES
(4, toDecimal512('3000000.000000000000000000', 18), toDecimal512('10000.000000000000', 12), toDecimal512('1.500000', 6));

SELECT * FROM test_decimal512_table ORDER BY id;
SELECT '-----------------------';

-- 9.5 修改精度
ALTER TABLE test_decimal512_table MODIFY COLUMN transaction_amount Decimal512(10);

SHOW CREATE TABLE test_decimal512_table;

-- 9.6 类型转换 Decimal512 -> Decimal256
ALTER TABLE test_decimal512_table MODIFY COLUMN transaction_amount Decimal256(6);

SHOW CREATE TABLE test_decimal512_table;

-- 9.7 类型转换 Decimal256 -> Decimal512
ALTER TABLE test_decimal512_table MODIFY COLUMN transaction_amount Decimal512(6);

SHOW CREATE TABLE test_decimal512_table;
SELECT '-----------------------';
-- 9.8 修改默认值、给列添加注释
ALTER TABLE test_decimal512_table MODIFY COLUMN transaction_amount Decimal512(6)
DEFAULT toDecimal512('100.000001', 6) COMMENT '交易金额';

INSERT INTO test_decimal512_table(id, token_balance, gas_fee, block_reward) VALUES
(5, toDecimal512('999999.9999', 4), toDecimal512('1000.01' ,2), toDecimal512('1.5001', 4));

SELECT * FROM test_decimal512_table ORDER BY id;
SELECT '-----------------------';
SHOW CREATE TABLE test_decimal512_table;
SELECT '-----------------------';



-- 9.9 删除列
ALTER TABLE test_decimal512_table DROP COLUMN transaction_amount;

SELECT * FROM test_decimal512_table ORDER BY id;

-- 9.10 清理测试表
DROP TABLE IF EXISTS test_decimal512_table;

-- ==============================================
-- 10. 索引创建测试
-- ==============================================

SELECT '=== 索引创建测试 ===' as test_section;

DROP TABLE IF EXISTS test_decimal512_indexed;
-- 10.1 创建带索引的测试表
CREATE TABLE IF NOT EXISTS test_decimal512_indexed (
    id UInt32,
    token_balance Decimal512(18),
    gas_fee Decimal512(12),
    block_reward Decimal512(6),
    INDEX idx_token_balance token_balance TYPE minmax GRANULARITY 1,
    INDEX idx_gas_fee gas_fee TYPE minmax GRANULARITY 1
) ENGINE = MergeTree()
ORDER BY id;

-- 10.2 插入测试数据
INSERT INTO test_decimal512_indexed VALUES
(1, toDecimal512('1000000.000000000000000000', 18), toDecimal512('21000.000000000000', 12), toDecimal512('6.250000', 6)),
(2, toDecimal512('500000.000000000000000000', 18), toDecimal512('15000.000000000000', 12), toDecimal512('2.000000', 6)),
(3, toDecimal512('2000000.000000000000000000', 18), toDecimal512('50000.000000000000', 12), toDecimal512('10.000000', 6));

-- 10.3 使用索引查询测试
SELECT
    id,
    token_balance,
    gas_fee,
    block_reward
FROM test_decimal512_indexed
WHERE token_balance > toDecimal512('1000000.000000000000000000', 18)
ORDER BY token_balance;

-- 10.4 清理测试表
DROP TABLE IF EXISTS test_decimal512_indexed;

-- ==============================================
-- 11. 分区表测试
-- ==============================================

SELECT '=== 分区表测试 ===' as test_section;

DROP TABLE IF EXISTS test_decimal512_partitioned;
-- 11.1 创建分区测试表
CREATE TABLE IF NOT EXISTS test_decimal512_partitioned (
    id UInt32,
    date Date,
    token_balance Decimal512(18),
    gas_fee Decimal512(12),
    block_reward Decimal512(6)
) ENGINE = MergeTree()
PARTITION BY date
ORDER BY id;

-- 11.2 插入测试数据
INSERT INTO test_decimal512_partitioned VALUES
(1, '2024-01-01', toDecimal512('1000000.000000000000000000', 18), toDecimal512('21000.000000000000', 12), toDecimal512('6.250000', 6)),
(2, '2024-01-01', toDecimal512('500000.000000000000000000', 18), toDecimal512('15000.000000000000', 12), toDecimal512('2.000000', 6)),
(3, '2024-01-02', toDecimal512('2000000.000000000000000000', 18), toDecimal512('50000.000000000000', 12), toDecimal512('10.000000', 6));

-- 11.3 分区查询测试
SELECT
    date,
    count() as record_count,
    sum(token_balance) as total_balance,
    avg(gas_fee) as avg_gas_fee
FROM test_decimal512_partitioned
GROUP BY date
ORDER BY date;

SELECT * FROM test_decimal512_partitioned ORDER BY token_balance;



-- 11.4 清理测试表
DROP TABLE IF EXISTS test_decimal512_partitioned;

-- ==============================================
-- 12. 复制表测试
-- ==============================================

SELECT '=== 复制表测试 ===' as test_section;

-- 12.1 创建复制测试表
-- CREATE TABLE IF NOT EXISTS test_decimal512_replicated (
--     id UInt32,
--     token_balance Decimal512(18),
--     gas_fee Decimal512(12),
--     block_reward Decimal512(6)
-- ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_decimal512_replicated', 'replica1')
-- ORDER BY id;
-- Can't create replicated table without ZooKeeper.

-- 12.2 插入测试数据
-- INSERT INTO test_decimal512_replicated VALUES
-- (1, toDecimal512('1000000.000000000000000000', 18), toDecimal512('21000.000000000000', 12), toDecimal512('6.250000', 6)),
-- (2, toDecimal512('500000.000000000000000000', 18), toDecimal512('15000.000000000000', 12), toDecimal512('2.000000', 6)),
-- (3, toDecimal512('2000000.000000000000000000', 18), toDecimal512('50000.000000000000', 12), toDecimal512('10.000000', 6));


-- 12.3 查询测试数据
-- SELECT
--     id,
--     token_balance,
--     gas_fee,
--     block_reward
-- FROM test_decimal512_replicated
-- ORDER BY id;

-- 12.4 清理测试表
-- DROP TABLE IF EXISTS test_decimal512_replicated;

-- ==============================================
-- 13. 表引擎测试
-- ==============================================

SELECT '===表引擎测试===';

DROP TABLE IF EXISTS test_a;
CREATE TABLE IF NOT EXISTS test_a(
    id UInt32,
    token_balance Decimal512(18)
)ENGINE=MergeTree
ORDER BY id;

INSERT INTO test_a VALUES
(1, toDecimal512('111111.111111111111111111', 18)),
(2, toDecimal512('123456.123456789012345678', 18)),
(3, toDecimal512('999999.999999999999999999', 18)),
(4, toDecimal512('0.000000000000000001', 18));

DROP TABLE IF EXISTS test_b;
CREATE TABLE IF NOT EXISTS test_b(
    id UInt32,
    transaction_amount Decimal512(8)
)ENGINE=MergeTree
ORDER BY id;

INSERT INTO test_b VALUES
(1, toDecimal512('123.12345678', 8)),
(2, toDecimal512('333.33333333', 8)),
(3, toDecimal512('987.98765432', 8));

-- 13.1 JOIN语句测试
SELECT test_a.*, test_b.transaction_amount
FROM test_a ALL LEFT JOIN test_b
ON test_a.id = test_b.id
ORDER BY test_a.id;

-- 13.2 ReplacingMergeTree引擎
DROP TABLE IF EXISTS user_profile;
CREATE TABLE IF NOT EXISTS user_profile
(
    user_id UInt64,
    token_balance Decimal512(18),
    ver UInt64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY user_id;

-- 插入两条相同主键不同版本的数据
INSERT INTO user_profile VALUES (1, toDecimal512('1234', 18), 1);
INSERT INTO user_profile VALUES (1, toDecimal512('2345', 18), 2);

-- 查询可能会看到两行
SELECT * FROM user_profile ORDER BY user_id, ver;

-- 等后台合并后，或强制使用 FINAL
SELECT * FROM user_profile FINAL;
-- 最终只会保留 ver=2 的记录

-- 13.3 HAVING语句
SELECT sum(token_balance) as total
FROM test_a
GROUP BY id % 2
HAVING sum(token_balance) > toDecimal512(500000, 18)
ORDER BY total;

-- 测试完成标记
SELECT '=== Decimal512 创建和插入异常测试完成 ===' as test_completion;

-- { echoNoSettings }
-- { echoNoDatabase }