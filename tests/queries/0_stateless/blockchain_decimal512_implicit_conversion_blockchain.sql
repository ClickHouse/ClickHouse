-- ClickHouse Decimal512 区块链场景隐式转换测试
-- 测试内容：代币余额、Gas费用、交易金额、DeFi协议、治理代币等区块链场景的隐式转换

SELECT '=== Decimal512 区块链场景隐式转换测试 ===' as test_section;

-- ==============================================
-- 1. 代币余额隐式转换测试
-- ==============================================

SELECT '=== 代币余额隐式转换测试 ===' as test_section;

-- 1.1 ETH（Wei单位）隐式转换测试
SELECT
    toDecimal512(1000000000000000000000, 0) as eth_1000_wei_to_decimal512,
    toDecimal512(500000000000000000000, 0) as eth_500_wei_to_decimal512,
    toDecimal512(1000000000000000000, 0) as eth_1_wei_to_decimal512,
    toDecimal512(1000000000000000000000, 18) as eth_1000_wei_to_decimal512_18_precision,
    toDecimal512(500000000000000000000, 18) as eth_500_wei_to_decimal512_18_precision,
    toDecimal512(1000000000000000000, 18) as eth_1_wei_to_decimal512_18_precision;

-- 1.2 USDC（6位小数）隐式转换测试
SELECT
    toDecimal512(1000000000, 0) as usdc_1000_units_to_decimal512,
    toDecimal512(500000000, 0) as usdc_500_units_to_decimal512,
    toDecimal512(1000000, 0) as usdc_1_units_to_decimal512,
    toDecimal512(1000000000, 6) as usdc_1000_units_to_decimal512_6_precision,
    toDecimal512(500000000, 6) as usdc_500_units_to_decimal512_6_precision,
    toDecimal512(1000000, 6) as usdc_1_units_to_decimal512_6_precision;

-- 1.3 WBTC（Satoshi单位）隐式转换测试
SELECT
    toDecimal512(100000000, 0) as wbtc_1_satoshi_to_decimal512,
    toDecimal512(5000000000, 0) as wbtc_50_satoshi_to_decimal512,
    toDecimal512(10000000000, 0) as wbtc_100_satoshi_to_decimal512,
    toDecimal512(100000000, 8) as wbtc_1_satoshi_to_decimal512_8_precision,
    toDecimal512(5000000000, 8) as wbtc_50_satoshi_to_decimal512_8_precision,
    toDecimal512(10000000000, 8) as wbtc_100_satoshi_to_decimal512_8_precision;

-- 1.4 代币余额混合运算隐式转换测试
SELECT
    toDecimal512(1000000000000000000000, 18) + toDecimal512(500000000000000000000, 18) as eth_balance_addition,
    toDecimal512(1000000000000000000000, 18) - toDecimal512(500000000000000000000, 18) as eth_balance_subtraction,
    toDecimal512(1000000000000000000000, 18) * toDecimal512('2.000000000000000000', 18) as eth_balance_multiplication,
    toDecimal512(1000000000000000000000, 18) / toDecimal512('2.000000000000000000', 18) as eth_balance_division;

-- ==============================================
-- 2. Gas费用隐式转换测试
-- ==============================================

SELECT '=== Gas费用隐式转换测试 ===' as test_section;

-- 2.1 Gas使用量隐式转换测试
SELECT
    toDecimal512(21000, 0) as gas_usage_21000_to_decimal512,
    toDecimal512(100000, 0) as gas_usage_100000_to_decimal512,
    toDecimal512(500000, 0) as gas_usage_500000_to_decimal512,
    toDecimal512(21000, 12) as gas_usage_21000_to_decimal512_12_precision,
    toDecimal512(100000, 12) as gas_usage_100000_to_decimal512_12_precision,
    toDecimal512(500000, 12) as gas_usage_500000_to_decimal512_12_precision;

-- 2.2 Gas价格（Gwei/Wei单位）隐式转换测试
SELECT
    toDecimal512(20, 0) as gas_price_20_gwei_to_decimal512,
    toDecimal512(50, 0) as gas_price_50_gwei_to_decimal512,
    toDecimal512(100, 0) as gas_price_100_gwei_to_decimal512,
    toDecimal512(20, 12) as gas_price_20_gwei_to_decimal512_12_precision,
    toDecimal512(50, 12) as gas_price_50_gwei_to_decimal512_12_precision,
    toDecimal512(100, 12) as gas_price_100_gwei_to_decimal512_12_precision;

-- 2.3 Gas费用计算隐式转换测试
SELECT
    toDecimal512(21000, 12) * toDecimal512(20, 12) as gas_cost_21k_20gwei,
    toDecimal512(100000, 12) * toDecimal512(50, 12) as gas_cost_100k_50gwei,
    toDecimal512(500000, 12) * toDecimal512(100, 12) as gas_cost_500k_100gwei,
    toDecimal512(21000, 12) * toDecimal512(20, 12) / toDecimal512('1000000000000000000.000000000000000000', 18) as gas_cost_in_eth;

-- 2.4 Gas费用聚合隐式转换测试
SELECT
    arraySum([toDecimal512(21000, 12), toDecimal512(100000, 12), toDecimal512(500000, 12)]) as total_gas_usage,
    arrayAvg([toDecimal512(20, 12), toDecimal512(50, 12), toDecimal512(100, 12)]) as avg_gas_price,
    arrayMax([toDecimal512(21000, 12), toDecimal512(100000, 12), toDecimal512(500000, 12)]) as max_gas_usage,
    arrayMin([toDecimal512(20, 12), toDecimal512(50, 12), toDecimal512(100, 12)]) as min_gas_price;

-- ==============================================
-- 3. 交易金额隐式转换测试
-- ==============================================

SELECT '=== 交易金额隐式转换测试 ===' as test_section;

-- 3.1 交易金额隐式转换测试
SELECT
    toDecimal512(1000000000000000000000, 0) as transaction_amount_1k_eth_wei,
    toDecimal512(500000000000000000000, 0) as transaction_amount_500_eth_wei,
    toDecimal512(1000000000000000000, 0) as transaction_amount_1_eth_wei,
    toDecimal512(1000000000000000000000, 18) as transaction_amount_1k_eth_decimal,
    toDecimal512(500000000000000000000, 18) as transaction_amount_500_eth_decimal,
    toDecimal512(1000000000000000000, 18) as transaction_amount_1_eth_decimal;

-- 3.2 区块号隐式转换测试
SELECT
    toDecimal512(1000000, 0) as block_number_1m_to_decimal512,
    toDecimal512(5000000, 0) as block_number_5m_to_decimal512,
    toDecimal512(10000000, 0) as block_number_10m_to_decimal512,
    toDecimal512(1000000, 6) as block_number_1m_to_decimal512_6_precision,
    toDecimal512(5000000, 6) as block_number_5m_to_decimal512_6_precision,
    toDecimal512(10000000, 6) as block_number_10m_to_decimal512_6_precision;

-- 3.3 交易索引隐式转换测试
SELECT
    toDecimal512(0, 0) as transaction_index_0_to_decimal512,
    toDecimal512(100, 0) as transaction_index_100_to_decimal512,
    toDecimal512(1000, 0) as transaction_index_1000_to_decimal512,
    toDecimal512(0, 6) as transaction_index_0_to_decimal512_6_precision,
    toDecimal512(100, 6) as transaction_index_100_to_decimal512_6_precision,
    toDecimal512(1000, 6) as transaction_index_1000_to_decimal512_6_precision;

-- 3.4 交易金额聚合隐式转换测试
SELECT
    arraySum([toDecimal512(1000000000000000000000, 18),
              toDecimal512(500000000000000000000, 18),
              toDecimal512(2000000000000000000000, 18)]) as total_transaction_amount,
    arrayAvg([toDecimal512(1000000000000000000000, 18),
              toDecimal512(500000000000000000000, 18),
              toDecimal512(2000000000000000000000, 18)]) as avg_transaction_amount,
    arrayMax([toDecimal512(1000000000000000000000, 18),
              toDecimal512(500000000000000000000, 18),
              toDecimal512(2000000000000000000000, 18)]) as max_transaction_amount,
    arrayMin([toDecimal512(1000000000000000000000, 18),
              toDecimal512(500000000000000000000, 18),
              toDecimal512(2000000000000000000000, 18)]) as min_transaction_amount;

-- ==============================================
-- 4. DeFi协议隐式转换测试
-- ==============================================

SELECT '=== DeFi协议隐式转换测试 ===' as test_section;

-- 4.1 流动性代币隐式转换测试
SELECT
    toDecimal512(1000000000000000000000, 0) as liquidity_token_1k_eth_wei,
    toDecimal512(500000000000000000000, 0) as liquidity_token_500_eth_wei,
    toDecimal512(2000000000000000000000, 0) as liquidity_token_2k_eth_wei,
    toDecimal512(1000000000000000000000, 18) as liquidity_token_1k_eth_decimal,
    toDecimal512(500000000000000000000, 18) as liquidity_token_500_eth_decimal,
    toDecimal512(2000000000000000000000, 18) as liquidity_token_2k_eth_decimal;

-- 4.2 价格比率隐式转换测试
SELECT
    toDecimal512(2000, 0) as price_ratio_2000_to_decimal512,
    toDecimal512(50000, 0) as price_ratio_50000_to_decimal512,
    toDecimal512(1, 0) as price_ratio_1_to_decimal512,
    toDecimal512(2000, 18) as price_ratio_2000_to_decimal512_18_precision,
    toDecimal512(50000, 18) as price_ratio_50000_to_decimal512_18_precision,
    toDecimal512(1, 18) as price_ratio_1_to_decimal512_18_precision;

-- 4.3 总锁定价值隐式转换测试
SELECT
    toDecimal512(10000000000000000000000, 0) as tvl_10k_eth_wei,
    toDecimal512(5000000000000000000000, 0) as tvl_5k_eth_wei,
    toDecimal512(20000000000000000000000, 0) as tvl_20k_eth_wei,
    toDecimal512(10000000000000000000000, 18) as tvl_10k_eth_decimal,
    toDecimal512(5000000000000000000000, 18) as tvl_5k_eth_decimal,
    toDecimal512(20000000000000000000000, 18) as tvl_20k_eth_decimal;

-- 4.4 DeFi协议聚合隐式转换测试
SELECT
    arraySum([toDecimal512(1000000000000000000000, 18),
              toDecimal512(500000000000000000000, 18),
              toDecimal512(2000000000000000000000, 18)]) as total_liquidity,
    arrayAvg([toDecimal512(2000, 18),
              toDecimal512(50000, 18),
              toDecimal512(1, 18)]) as avg_price_ratio,
    arrayMax([toDecimal512(10000000000000000000000, 18),
              toDecimal512(5000000000000000000000, 18),
              toDecimal512(20000000000000000000000, 18)]) as max_tvl,
    arrayMin([toDecimal512(10000000000000000000000, 18),
              toDecimal512(5000000000000000000000, 18),
              toDecimal512(20000000000000000000000, 18)]) as min_tvl;

-- ==============================================
-- 5. 治理代币隐式转换测试
-- ==============================================

SELECT '=== 治理代币隐式转换测试 ===' as test_section;

-- 5.1 投票权重隐式转换测试
SELECT
    toDecimal512(100000000000000000000000, 0) as voting_weight_100k_tokens_wei,
    toDecimal512(50000000000000000000000, 0) as voting_weight_50k_tokens_wei,
    toDecimal512(200000000000000000000000, 0) as voting_weight_200k_tokens_wei,
    toDecimal512(100000000000000000000000, 18) as voting_weight_100k_tokens_decimal,
    toDecimal512(50000000000000000000000, 18) as voting_weight_50k_tokens_decimal,
    toDecimal512(200000000000000000000000, 18) as voting_weight_200k_tokens_decimal;

-- 5.2 投票数隐式转换测试
SELECT
    toDecimal512(1000000, 0) as vote_count_1m_to_decimal512,
    toDecimal512(5000000, 0) as vote_count_5m_to_decimal512,
    toDecimal512(10000000, 0) as vote_count_10m_to_decimal512,
    toDecimal512(1000000, 6) as vote_count_1m_to_decimal512_6_precision,
    toDecimal512(5000000, 6) as vote_count_5m_to_decimal512_6_precision,
    toDecimal512(10000000, 6) as vote_count_10m_to_decimal512_6_precision;

-- 5.3 总供应量隐式转换测试
SELECT
    toDecimal512(1000000000000000000000000, 0) as total_supply_1m_tokens_wei,
    toDecimal512(500000000000000000000000, 0) as total_supply_500k_tokens_wei,
    toDecimal512(2000000000000000000000000, 0) as total_supply_2m_tokens_wei,
    toDecimal512(1000000000000000000000000, 18) as total_supply_1m_tokens_decimal,
    toDecimal512(500000000000000000000000, 18) as total_supply_500k_tokens_decimal,
    toDecimal512(2000000000000000000000000, 18) as total_supply_2m_tokens_decimal;

-- 5.4 治理代币聚合隐式转换测试
SELECT
    arraySum([toDecimal512(100000000000000000000000, 18),
              toDecimal512(50000000000000000000000, 18),
              toDecimal512(200000000000000000000000, 18)]) as total_voting_weight,
    arrayAvg([toDecimal512(1000000, 6),
              toDecimal512(5000000, 6),
              toDecimal512(10000000, 6)]) as avg_vote_count,
    arrayMax([toDecimal512(1000000000000000000000000, 18),
              toDecimal512(500000000000000000000000, 18),
              toDecimal512(2000000000000000000000000, 18)]) as max_total_supply,
    arrayMin([toDecimal512(1000000000000000000000000, 18),
              toDecimal512(500000000000000000000000, 18),
              toDecimal512(2000000000000000000000000, 18)]) as min_total_supply;

-- ==============================================
-- 6. 综合区块链场景隐式转换测试
-- ==============================================

SELECT '=== 综合区块链场景隐式转换测试 ===' as test_section;

-- 6.1 跨协议代币转换测试
SELECT
    toDecimal512(1000000000000000000000, 18) as eth_1k_to_decimal512,
    toDecimal512(1000000000, 6) as usdc_1k_to_decimal512,
    toDecimal512(100000000, 8) as wbtc_1_to_decimal512,
    toDecimal512(1000000000000000000000, 18) + toDecimal512(1000000000, 6) as eth_plus_usdc_conversion,
    toDecimal512(1000000000000000000000, 18) + toDecimal512(100000000, 8) as eth_plus_wbtc_conversion,
    toDecimal512(1000000000, 6) + toDecimal512(100000000, 8) as usdc_plus_wbtc_conversion;

-- 6.2 跨协议Gas费用转换测试
SELECT
    toDecimal512(21000, 12) as gas_usage_21k_to_decimal512,
    toDecimal512(20, 12) as gas_price_20_gwei_to_decimal512,
    toDecimal512(21000, 12) * toDecimal512(20, 12) as gas_cost_calculation,
    toDecimal512(21000, 12) * toDecimal512(20, 12) / toDecimal512('1000000000000000000.000000000000000000', 18) as gas_cost_in_eth;

-- 6.3 跨协议交易金额转换测试
SELECT
    toDecimal512(1000000000000000000000, 18) as transaction_1k_eth,
    toDecimal512(1000000, 0) as block_number_1m,
    toDecimal512(100, 0) as transaction_index_100,
    toDecimal512(1000000000000000000000, 18) + toDecimal512(1000000, 0) as transaction_plus_block_number,
    toDecimal512(1000000000000000000000, 18) + toDecimal512(100, 0) as transaction_plus_index;

-- 6.4 跨协议DeFi协议转换测试
SELECT
    toDecimal512(1000000000000000000000, 18) as liquidity_1k_eth,
    toDecimal512(2000, 18) as price_ratio_2000,
    toDecimal512(10000000000000000000000, 18) as tvl_10k_eth,
    toDecimal512(1000000000000000000000, 18) * toDecimal512(2000, 18) as liquidity_times_price,
    toDecimal512(1000000000000000000000, 18) + toDecimal512(10000000000000000000000, 18) as liquidity_plus_tvl;

-- 6.5 跨协议治理代币转换测试
-- SELECT
--     toDecimal512(100000000000000000000000, 18) as voting_weight_100k,
--     toDecimal512(1000000, 6) as vote_count_1m,
--     toDecimal512(1000000000000000000000000, 18) as total_supply_1m,
--     toDecimal512(100000000000000000000000, 18) / toDecimal512(1000000000000000000000000, 18) as voting_weight_ratio,
--     toDecimal512(1000000, 6) / toDecimal512(1000000000000000000000000, 18) as vote_count_ratio;
-- Decimal result's scale is less than argument's one

-- 测试完成标记
SELECT '=== Decimal512 区块链场景隐式转换测试完成 ===' as test_completion;
