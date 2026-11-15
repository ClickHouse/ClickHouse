-- ClickHouse Decimal512 区块链特殊场景测试
-- 测试内容：以太坊Wei、比特币Satoshi、DeFi流动性池、Gas费用计算等

SELECT '=== Decimal512 区块链特殊场景测试 ===' as test_section;

-- ==============================================
-- 1. 以太坊Wei单位测试
-- ==============================================

SELECT '=== 以太坊Wei单位测试 ===' as test_section;

-- 1.1 Wei单位转换测试
SELECT
    toDecimal512('1000000000000000000', 0) as wei_1_eth,
    toDecimal512('1000000000000000000.000000000000000001', 18) as wei_1_eth_decimal,
    toDecimal512('1000000000000000000000', 0) as wei_1000_eth,
    toDecimal512('1000000000000000000000.000000000000000001', 18) as wei_1000_eth_decimal;

-- 1.2 Gwei单位测试
SELECT
    toDecimal512('1000000000', 0) as gwei_1_eth,
    toDecimal512('1000000000.000000000000000001', 18) as gwei_1_eth_decimal,
    toDecimal512('21000000000', 0) as gwei_21_eth,
    toDecimal512('21000000000.000000000000000001', 18) as gwei_21_eth_decimal;

-- 1.3 以太坊余额测试
SELECT
    toDecimal512('1000000000000000000000', 0) as eth_1000_wei,
    toDecimal512('1000000000000000000000.000000000000000001', 18) as eth_1000_decimal,
    toDecimal512('500000000000000000000', 0) as eth_500_wei,
    toDecimal512('500000000000000000000.000000000000000001', 18) as eth_500_decimal;

-- ==============================================
-- 2. 比特币Satoshi单位测试
-- ==============================================

SELECT '=== 比特币Satoshi单位测试 ===' as test_section;

-- 2.1 Satoshi单位转换测试
SELECT
    toDecimal512('100000000', 0) as satoshi_1_btc,
    toDecimal512('100000000.00000001', 8) as satoshi_1_btc_decimal,
    toDecimal512('2100000000000000', 0) as satoshi_21m_btc,
    toDecimal512('2100000000000000.00000001', 8) as satoshi_21m_btc_decimal;

-- 2.2 比特币余额测试
SELECT
    toDecimal512('100000000000000', 0) as btc_1m_satoshi,
    toDecimal512('100000000000000.00000001', 8) as btc_1m_decimal,
    toDecimal512('50000000000000', 0) as btc_500k_satoshi,
    toDecimal512('50000000000000.00000001', 8) as btc_500k_decimal;

-- 2.3 比特币交易费用测试
SELECT
    toDecimal512('1000', 0) as btc_fee_1000_satoshi,
    toDecimal512('1000.00000001', 8) as btc_fee_1000_decimal,
    toDecimal512('5000', 0) as btc_fee_5000_satoshi,
    toDecimal512('5000.00000001', 8) as btc_fee_5000_decimal;

-- ==============================================
-- 3. DeFi流动性池测试
-- ==============================================

SELECT '=== DeFi流动性池测试 ===' as test_section;

-- 3.1 Uniswap V3流动性池测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) as uniswap_eth_liquidity,
    toDecimal512('2000000000000000000000.000000000000000001', 18) as uniswap_eth_liquidity_2x,
    toDecimal512('500000000000000000000.000000000000000001', 18) as uniswap_eth_liquidity_half;

-- 3.2 Curve流动性池测试
SELECT
    toDecimal512('10000000000000000000000.000000000000000001', 18) as curve_stablecoin_liquidity,
    toDecimal512('5000000000000000000000.000000000000000001', 18) as curve_stablecoin_liquidity_half,
    toDecimal512('20000000000000000000000.000000000000000001', 18) as curve_stablecoin_liquidity_2x;

-- 3.3 流动性挖矿奖励测试
SELECT
    toDecimal512('100000000000000000000.000000000000000001', 18) as liquidity_mining_reward,
    toDecimal512('50000000000000000000.000000000000000001', 18) as liquidity_mining_reward_half,
    toDecimal512('200000000000000000000.000000000000000001', 18) as liquidity_mining_reward_2x;

-- ==============================================
-- 4. Gas费用计算测试
-- ==============================================

SELECT '=== Gas费用计算测试 ===' as test_section;

-- 4.1 基础Gas费用测试
SELECT
    toDecimal512('21000.000000000001', 12) as basic_transfer_gas,
    toDecimal512('100000.000000000001', 12) as contract_call_gas,
    toDecimal512('500000.000000000001', 12) as complex_contract_gas;

-- 4.2 Gas价格测试
SELECT
    toDecimal512('20.000000000001', 12) as gas_price_20_gwei,
    toDecimal512('50.000000000001', 12) as gas_price_50_gwei,
    toDecimal512('100.000000000001', 12) as gas_price_100_gwei;

-- 4.3 Gas费用计算测试
SELECT
    toDecimal512('21000.000000000001', 12) * toDecimal512('20.000000000001', 12) as gas_cost_21k_20gwei,
    toDecimal512('100000.000000000001', 12) * toDecimal512('50.000000000001', 12) as gas_cost_100k_50gwei,
    toDecimal512('500000.000000000001', 12) * toDecimal512('100.000000000001', 12) as gas_cost_500k_100gwei;

-- ==============================================
-- 5. 代币精度差异测试
-- ==============================================

SELECT '=== 代币精度差异测试 ===' as test_section;

-- 5.1 USDT精度测试（6位小数）
SELECT
    toDecimal512('1000000.000001', 6) as usdt_1m,
    toDecimal512('500000.000001', 6) as usdt_500k,
    toDecimal512('10000000.000001', 6) as usdt_10m;

-- 5.2 USDC精度测试（6位小数）
SELECT
    toDecimal512('1000000.000001', 6) as usdc_1m,
    toDecimal512('500000.000001', 6) as usdc_500k,
    toDecimal512('10000000.000001', 6) as usdc_10m;

-- 5.3 WBTC精度测试（8位小数）
SELECT
    toDecimal512('1.00000001', 8) as wbtc_1,
    toDecimal512('0.50000001', 8) as wbtc_half,
    toDecimal512('10.00000001', 8) as wbtc_10;

-- 5.4 代币精度转换测试
SELECT
    toDecimal512('1000000.000001', 6) as usdt_1m_6_decimal,
    toDecimal512('1000000.000000000000000001', 18) as usdt_1m_18_decimal,
    toDecimal512('1.00000001', 8) as wbtc_1_8_decimal,
    toDecimal512('1.000000000000000001', 18) as wbtc_1_18_decimal;

-- ==============================================
-- 6. 价格预言机测试
-- ==============================================

SELECT '=== 价格预言机测试 ===' as test_section;

-- 6.1 Chainlink价格预言机测试
SELECT
    toDecimal512('2000.000000000000000001', 18) as eth_usd_price,
    toDecimal512('50000.000000000000000001', 18) as btc_usd_price,
    toDecimal512('1.000000000000000001', 18) as usd_usd_price;

-- 6.2 价格聚合测试
SELECT
    toDecimal512('2000.000000000000000001', 18) as eth_price_chainlink,
    toDecimal512('2001.000000000000000001', 18) as eth_price_uniswap,
    toDecimal512('1999.000000000000000001', 18) as eth_price_curve;

-- 6.3 价格计算测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) * toDecimal512('2000.000000000000000001', 18) as eth_1000_usd_value,
    toDecimal512('100000000.00000001', 8) * toDecimal512('50000.000000000000000001', 18) as btc_1_usd_value;

-- ==============================================
-- 7. 闪电贷测试
-- ==============================================

SELECT '=== 闪电贷测试 ===' as test_section;

-- 7.1 Aave闪电贷测试
SELECT
    toDecimal512('10000000000000000000000.000000000000000001', 18) as aave_flash_loan_10k_eth,
    toDecimal512('5000000000000000000000.000000000000000001', 18) as aave_flash_loan_5k_eth,
    toDecimal512('20000000000000000000000.000000000000000001', 18) as aave_flash_loan_20k_eth;

-- 7.2 dYdX闪电贷测试
SELECT
    toDecimal512('10000000000000000000000.000000000000000001', 18) as dydx_flash_loan_10k_eth,
    toDecimal512('5000000000000000000000.000000000000000001', 18) as dydx_flash_loan_5k_eth,
    toDecimal512('20000000000000000000000.000000000000000001', 18) as dydx_flash_loan_20k_eth;

-- 7.3 闪电贷费用测试
SELECT
    toDecimal512('10000000000000000000000.000000000000000001', 18) * toDecimal512('0.000900000000000001', 18) as flash_loan_fee_0_09_percent,
    toDecimal512('10000000000000000000000.000000000000000001', 18) * toDecimal512('0.001000000000000001', 18) as flash_loan_fee_0_1_percent;

-- ==============================================
-- 8. 治理代币投票测试
-- ==============================================

SELECT '=== 治理代币投票测试 ===' as test_section;

-- 8.1 UNI治理代币测试
SELECT
    toDecimal512('1000000000000000000000000.000000000000000001', 18) as uni_total_supply,
    toDecimal512('100000000000000000000000.000000000000000001', 18) as uni_10_percent,
    toDecimal512('50000000000000000000000.000000000000000001', 18) as uni_5_percent;

-- 8.2 COMP治理代币测试
SELECT
    toDecimal512('10000000000000000000000000.000000000000000001', 18) as comp_total_supply,
    toDecimal512('1000000000000000000000000.000000000000000001', 18) as comp_10_percent,
    toDecimal512('500000000000000000000000.000000000000000001', 18) as comp_5_percent;

-- 8.3 投票权重计算测试
SELECT
    toDecimal256('100000000000000000000000.123456789012345678', 18) / toDecimal256('1000000000000000000000000.9999999999999999999', 18) as uni_voting_weight_10_percent,
    toDecimal512('50000000000000000000000.000000000000000001', 18) / toDecimal512('1000000000000000000000000.000000000000000001', 18) as uni_voting_weight_5_percent;

-- ==============================================
-- 9. 跨链桥测试
-- ==============================================

SELECT '=== 跨链桥测试 ===' as test_section;

-- 9.1 Polygon跨链桥测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) as polygon_bridge_1k_eth,
    toDecimal512('500000000000000000000.000000000000000001', 18) as polygon_bridge_500_eth,
    toDecimal512('2000000000000000000000.000000000000000001', 18) as polygon_bridge_2k_eth;

-- 9.2 Arbitrum跨链桥测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) as arbitrum_bridge_1k_eth,
    toDecimal512('500000000000000000000.000000000000000001', 18) as arbitrum_bridge_500_eth,
    toDecimal512('2000000000000000000000.000000000000000001', 18) as arbitrum_bridge_2k_eth;

-- 9.3 跨链桥费用测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) * toDecimal512('0.001000000000000001', 18) as bridge_fee_0_1_percent,
    toDecimal512('1000000000000000000000.000000000000000001', 18) * toDecimal512('0.002000000000000001', 18) as bridge_fee_0_2_percent;

-- ==============================================
-- 10. 综合区块链场景测试
-- ==============================================

SELECT '=== 综合区块链场景测试 ===' as test_section;

-- 10.1 DeFi协议综合测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) as defi_protocol_tvl,
    toDecimal512('100000000000000000000.000000000000000001', 18) as defi_protocol_revenue,
    toDecimal512('10000000000000000000.000000000000000001', 18) as defi_protocol_fees;

-- 10.2 跨链DeFi测试
SELECT
    toDecimal512('1000000000000000000000.000000000000000001', 18) as cross_chain_eth_locked,
    toDecimal512('500000000000000000000.000000000000000001', 18) as cross_chain_eth_borrowed,
    toDecimal512('1500000000000000000000.000000000000000001', 18) as cross_chain_eth_total;

-- 10.3 治理和投票综合测试
SELECT
    toDecimal512('1000000000000000000000000.000000000000000001', 18) as governance_total_tokens,
    toDecimal512('100000000000000000000000.000000000000000001', 18) as governance_voted_tokens,
    toDecimal512('900000000000000000000000.000000000000000001', 18) as governance_remaining_tokens;

-- 测试完成标记
SELECT '=== Decimal512 区块链特殊场景测试完成 ===' as test_completion;