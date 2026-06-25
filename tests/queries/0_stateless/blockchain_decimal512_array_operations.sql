-- ClickHouse Decimal512 数组操作测试 - 区块链场景
-- 测试内容：数组中的Decimal512操作

SELECT '=== Decimal512 数组操作测试 - 区块链场景 ===' as test_section;

-- ==============================================
-- 1. 数组基本操作测试
-- ==============================================

SELECT '=== 数组基本操作测试 ===' as test_section;

-- 1.1 数组长度测试
SELECT
    length([toDecimal512('1000000.000000000000000000', 18),
            toDecimal512('500000.000000000000000000', 18),
            toDecimal512('2000000.000000000000000000', 18),
            toDecimal512('750000.000000000000000000', 18)]) as array_length;

-- 1.2 数组元素访问测试
SELECT
    [toDecimal512('1000000.000000000000000000', 18),
     toDecimal512('500000.000000000000000000', 18),
     toDecimal512('2000000.000000000000000000', 18),
     toDecimal512('750000.000000000000000000', 18)][1] as first_element,

    [toDecimal512('1000000.000000000000000000', 18),
     toDecimal512('500000.000000000000000000', 18),
     toDecimal512('2000000.000000000000000000', 18),
     toDecimal512('750000.000000000000000000', 18)][-1] as last_element;

-- ==============================================
-- 2. 数组累积操作测试
-- ==============================================

SELECT '=== 数组累积操作测试 ===' as test_section;

-- 2.1 累积和 (cumsum)
-- SELECT
--     arrayCumSum([toDecimal512('1000000.000000000000000000', 18),
--                  toDecimal512('500000.000000000000000000', 18),
--                  toDecimal512('2000000.000000000000000000', 18),
--                  toDecimal512('750000.000000000000000000', 18)]) as cumsum_transactions;

SELECT
    arrayCumSum([toDecimal256('1000000.000000000000000000', 18),
                 toDecimal256('500000.000000000000000000', 18),
                 toDecimal256('2000000.000000000000000000', 18),
                 toDecimal256('750000.000000000000000000', 18)]) as cumsum_transactions;

SELECT
    arrayCumSum([toDecimal128('1000000', 0),
                 toDecimal128('500000', 0),
                 toDecimal128('2000000', 0),
                 toDecimal128('750000', 0)]) as cumsum_transactions;

SELECT
    arrayCumSum([toDecimal64('1000000', 0),
                 toDecimal64('500000', 0),
                 toDecimal64('2000000', 0),
                 toDecimal64('750000', 0)]) as cumsum_transactions;

-- 2.2 非负累积和 (cumsumNonNegative)
-- SELECT
--     arrayCumSumNonNegative([toDecimal512('1000000.000000000000000000', 18),
--                             toDecimal512('-500000.000000000000000000', 18),
--                             toDecimal512('2000000.000000000000000000', 18),
--                             toDecimal512('-750000.000000000000000000', 18)]) as cumsum_non_negative;

SELECT
    arrayCumSumNonNegative([toDecimal256('1000000.000000000000000000', 18),
                 toDecimal256('-500000.000000000000000000', 18),
                 toDecimal256('2000000.000000000000000000', 18),
                 toDecimal256('-750000.000000000000000000', 18)]) as cumsum_transactions;

SELECT
    arrayCumSumNonNegative([toDecimal128('1000000', 0),
                 toDecimal128('-500000', 0),
                 toDecimal128('2000000', 0),
                 toDecimal128('-750000', 0)]) as cumsum_transactions;

SELECT
    arrayCumSumNonNegative([toDecimal64('1000000', 0),
                 toDecimal64('-500000', 0),
                 toDecimal64('2000000', 0),
                 toDecimal64('-750000', 0)]) as cumsum_transactions;

-- 2.3 差分 (difference)
-- SELECT
--     arrayDifference([toDecimal512('1000000.000000000000000000', 18),
--                      toDecimal512('1500000.000000000000000000', 18),
--                      toDecimal512('2000000.000000000000000000', 18),
--                      toDecimal512('1750000.000000000000000000', 18)]) as difference_transactions;

SELECT
    arrayDifference([toDecimal256('1000000.000000000000000000', 18),
                     toDecimal256('1500000.000000000000000000', 18),
                     toDecimal256('2000000.000000000000000000', 18),
                     toDecimal256('1750000.000000000000000000', 18)]) as difference_transactions;

SELECT
    arrayDifference([toDecimal128('1000000', 0),
                     toDecimal128('1500000', 0),
                     toDecimal128('2000000', 0),
                     toDecimal128('1750000', 0)]) as difference_transactions;

SELECT
    arrayDifference([toDecimal64('1000000', 0),
                     toDecimal64('1500000', 0),
                     toDecimal64('2000000', 0),
                     toDecimal64('1750000', 0)]) as difference_transactions;

-- ==============================================
-- 3. 数组处理操作测试
-- ==============================================

SELECT '=== 数组处理操作测试 ===' as test_section;

-- 3.1 排序 (sort)
SELECT
    arraySort([toDecimal512('1000000.000000000000000000', 18),
               toDecimal512('500000.000000000000000000', 18),
               toDecimal512('2000000.000000000000000000', 18),
               toDecimal512('750000.000000000000000000', 18)]) as sorted_transactions;

-- 3.2 去重 (distinct)
SELECT
    arrayDistinct([toDecimal512('1000000.000000000000000000', 18),
                   toDecimal512('500000.000000000000000000', 18),
                   toDecimal512('1000000.000000000000000000', 18),
                   toDecimal512('750000.000000000000000000', 18)]) as distinct_transactions;

-- 3.3 紧凑化 (compact)
SELECT
    arrayCompact([toDecimal512('1000000.000000000000000000', 18),
                  toDecimal512('1000000.000000000000000000', 18),
                  toDecimal512('500000.000000000000000000', 18),
                  toDecimal512('500000.000000000000000000', 18),
                  toDecimal512('750000.000000000000000000', 18)]) as compact_transactions;

-- ==============================================
-- 4. 数组集合操作测试
-- ==============================================

SELECT '=== 数组集合操作测试 ===' as test_section;

-- 4.1 交集 (intersect)
SELECT
    arrayIntersect([toDecimal512('1000000.000000000000000000', 18),
                    toDecimal512('500000.000000000000000000', 18),
                    toDecimal512('2000000.000000000000000000', 18)],
                   [toDecimal512('500000.000000000000000000', 18),
                    toDecimal512('2000000.000000000000000000', 18),
                    toDecimal512('750000.000000000000000000', 18)]) as intersect_transactions;

-- 4.2 连接 (concat)
SELECT
    arrayConcat([toDecimal512('1000000.000000000000000000', 18),
                 toDecimal512('500000.000000000000000000', 18)],
                [toDecimal512('2000000.000000000000000000', 18),
                 toDecimal512('750000.000000000000000000', 18)]) as concat_transactions;

-- ==============================================
-- 5. 数组函数式操作测试
-- ==============================================

SELECT '=== 数组函数式操作测试 ===' as test_section;

-- 5.1 过滤 (filter)
SELECT
    arrayFilter(x -> x > toDecimal512('1000000.000000000000000000', 18),
                [toDecimal512('1000000.000000000000000000', 18),
                 toDecimal512('500000.000000000000000000', 18),
                 toDecimal512('2000000.000000000000000000', 18),
                 toDecimal512('750000.000000000000000000', 18)]) as filtered_large_transactions;

-- 5.2 映射 (map)
SELECT
    arrayMap(x -> x * toDecimal512('1.1', 1),
             [toDecimal512('1000000.000000000000000000', 18),
              toDecimal512('500000.000000000000000000', 18),
              toDecimal512('2000000.000000000000000000', 18),
              toDecimal512('750000.000000000000000000', 18)]) as mapped_transactions_with_fee;

-- 5.3 归约 (reduce)
SELECT
    arrayReduce('sum', [toDecimal512('1000000.000000000000000000', 18),
                        toDecimal512('500000.000000000000000000', 18),
                        toDecimal512('2000000.000000000000000000', 18),
                        toDecimal512('750000.000000000000000000', 18)]) as total_transactions,

    arrayReduce('max', [toDecimal512('1000000.000000000000000000', 18),
                        toDecimal512('500000.000000000000000000', 18),
                        toDecimal512('2000000.000000000000000000', 18),
                        toDecimal512('750000.000000000000000000', 18)]) as max_transaction,

    arrayReduce('min', [toDecimal512('1000000.000000000000000000', 18),
                        toDecimal512('500000.000000000000000000', 18),
                        toDecimal512('2000000.000000000000000000', 18),
                        toDecimal512('750000.000000000000000000', 18)]) as min_transaction;

-- ==============================================
-- 6. 数组存在性检查测试
-- ==============================================

SELECT '=== 数组存在性检查测试 ===' as test_section;

-- 6.1 存在性检查
SELECT
    has([toDecimal512('1000000.000000000000000000', 18),
         toDecimal512('500000.000000000000000000', 18),
         toDecimal512('2000000.000000000000000000', 18),
         toDecimal512('750000.000000000000000000', 18)],
        toDecimal512('1000000.000000000000000000', 18)) as has_large_transaction,

    has([toDecimal512('1000000.000000000000000000', 18),
         toDecimal512('500000.000000000000000000', 18),
         toDecimal512('2000000.000000000000000000', 18),
         toDecimal512('750000.000000000000000000', 18)],
        toDecimal512('300000.000000000000000000', 18)) as has_small_transaction;

-- 6.2 索引查找
SELECT
    indexOf([toDecimal512('1000000.000000000000000000', 18),
             toDecimal512('500000.000000000000000000', 18),
             toDecimal512('2000000.000000000000000000', 18),
             toDecimal512('750000.000000000000000000', 18)],
            toDecimal512('2000000.000000000000000000', 18)) as index_of_large_transaction;

-- ==============================================
-- 7. 区块链场景综合数组操作测试
-- ==============================================

SELECT '=== 区块链场景综合数组操作测试 ===' as test_section;

-- 7.1 模拟交易历史分析
SELECT
    -- arrayCumSum([toDecimal512('1000000.000000000000000000', 18),
    --              toDecimal512('500000.000000000000000000', 18),
    --              toDecimal512('2000000.000000000000000000', 18),
    --              toDecimal512('750000.000000000000000000', 18)]) as cumulative_balance,

    arrayFilter(x -> x > toDecimal512('1000000.000000000000000000', 18),
                [toDecimal512('1000000.000000000000000000', 18),
                 toDecimal512('500000.000000000000000000', 18),
                 toDecimal512('2000000.000000000000000000', 18),
                 toDecimal512('750000.000000000000000000', 18)]) as large_transactions,

    arrayReduce('sum', [toDecimal512('1000000.000000000000000000', 18),
                        toDecimal512('500000.000000000000000000', 18),
                        toDecimal512('2000000.000000000000000000', 18),
                        toDecimal512('750000.000000000000000000', 18)]) as total_volume;

-- 7.2 模拟Gas费用分析
SELECT
    -- arrayCumSum([toDecimal512('21000.000000000000', 12),
    --              toDecimal512('15000.000000000000', 12),
    --              toDecimal512('50000.000000000000', 12),
    --              toDecimal512('25000.000000000000', 12)]) as cumulative_gas_fees,

    arrayAvg([toDecimal512('21000.000000000000', 12),
              toDecimal512('15000.000000000000', 12),
              toDecimal512('50000.000000000000', 12),
              toDecimal512('25000.000000000000', 12)]) as avg_gas_fee,

    arrayMax([toDecimal512('21000.000000000000', 12),
              toDecimal512('15000.000000000000', 12),
              toDecimal512('50000.000000000000', 12),
              toDecimal512('25000.000000000000', 12)]) as max_gas_fee;

-- 7.3 模拟DeFi协议流动性分析
SELECT
    -- arrayCumSumNonNegative([toDecimal512('1000000000000000000000.000000000000000000', 18),
    --                         toDecimal512('-500000000000000000000.000000000000000000', 18),
    --                         toDecimal512('2000000000000000000000.000000000000000000', 18),
    --                         toDecimal512('-750000000000000000000.000000000000000000', 18)]) as liquidity_changes,

    arraySort([toDecimal512('1000000000000000000000.000000000000000000', 18),
               toDecimal512('500000000000000000000.000000000000000000', 18),
               toDecimal512('2000000000000000000000.000000000000000000', 18),
               toDecimal512('750000000000000000000.000000000000000000', 18)]) as sorted_liquidity,

    arrayReduce('sum', [toDecimal512('1000000000000000000000.000000000000000000', 18),
                        toDecimal512('500000000000000000000.000000000000000000', 18),
                        toDecimal512('2000000000000000000000.000000000000000000', 18),
                        toDecimal512('750000000000000000000.000000000000000000', 18)]) as total_liquidity;

-- 测试完成标记
SELECT '=== Decimal512 数组操作测试完成 ===' as test_completion;
