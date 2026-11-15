-- ==============================================
-- 1. 函数覆盖率测试
-- ==============================================

SELECT '===分组聚合测试===';


-- 1.1 isNaN函数

-- SELECT isNaN(toDecimal512('1.1', 1));

-- Argument for function isNaN must be a number: In scope SELECT isNaN(toDecimal512('1.1', 1)). (ILLEGAL_TYPE_OF_ARGUMENT)

-- 1.2 isFinite函数

-- SELECT isFinite(toDecimal512('1.1', 1));

-- Argument for function isFinite must be a number

-- 使用确定性值以保证输出稳定，仍覆盖 Decimal512 乘法路径
SELECT toDecimal512('9044903845911985044', 0) * toDecimal512('100', 0);

-- ==============================================
-- 2. JOIN / 子查询 / CTE
-- ==============================================

SELECT '===JOIN / 子查询 / CTE';

-- JOIN 作为连接键
DROP TABLE IF EXISTS t1;
CREATE TABLE IF NOT EXISTS t1 (
    x Decimal512(3)
)ENGINE=Memory;
DROP TABLE IF EXISTS t2;
CREATE TABLE IF NOT EXISTS t2 (
    y Decimal512(3)
)ENGINE=Memory;
INSERT INTO t1 VALUES (1.123), (2.456);
INSERT INTO t2 VALUES (2.456), (3.789);

SELECT x, y FROM t1 JOIN t2 ON t1.x = t2.y;

-- 子查询 IN
SELECT x FROM t1 WHERE x IN (SELECT y FROM t2);

-- WITH CTE
WITH toDecimal512('123.456', 3) AS val
SELECT val + 1;


-- ==============================================
-- 3. 窗口函数
-- ==============================================

SELECT '===窗口函数===';

DROP TABLE IF EXISTS t_win;
CREATE TABLE IF NOT EXISTS t_win(x Decimal512(2)) ENGINE=Memory;
INSERT INTO t_win VALUES (1.11), (2.22), (3.33);

SELECT x, sum(x) OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum
FROM t_win;

SELECT x, lag(x, 1) OVER (ORDER BY x) AS prev_val FROM t_win;


-- ==============================================
-- 4. 索引 & 存储格式
-- ==============================================

SELECT '===索引&存储格式===';

DROP TABLE IF EXISTS t_index;

CREATE TABLE IF NOT EXISTS t_index
(
    id UInt32,
    amount Decimal512(5)
)
ENGINE = MergeTree
ORDER BY (amount);

INSERT INTO t_index VALUES (1, 123.45678), (2, 99999.99999);

-- min/max 索引检查
SELECT min(amount), max(amount) FROM t_index;

-- PARTITION BY
CREATE TABLE t_part
(
    d Decimal512(2)
)
ENGINE = MergeTree
PARTITION BY d
ORDER BY d;

INSERT INTO t_part VALUES (1.23), (4.56);


-- ==============================================
-- 5. 分布式/复制集群
-- ==============================================

-- 跨 shard 插入 + 查询
-- CREATE TABLE shard_local (x Decimal512(2)) ENGINE=MergeTree ORDER BY x;
-- CREATE TABLE dist (x Decimal512(2)) ENGINE=Distributed('test_cluster', 'default', 'shard_local', rand());

-- INSERT INTO dist VALUES (1.23), (4.56);
-- SELECT sum(x) FROM dist;


-- ==============================================
-- 6. 导入导出 & 外部格式
-- ==============================================

SELECT '===导入导出===';
-- JSONEachRow 导入导出
INSERT INTO t1 FORMAT JSONEachRow {"x": "123.456"}{"eof":"NULL"};

SELECT * FROM t1 ORDER BY x FORMAT JSONEachRow;

-- CSV
-- INSERT INTO t1 FORMAT CSV '789.012';
-- SELECT * FROM t1 FORMAT CSV;


-- ==============================================
-- 7. 异常与边界补充
-- ==============================================

SELECT '===异常与边界补充===';

-- 精度超限
-- SELECT toDecimal512('123.456', 200) AS too_large_scale; -- 应该报错

-- 负数边界
SELECT toDecimal512('-999999999999999999999999999999999999999999999999999999999999', 0);

-- Null 参与运算
SELECT toDecimal512OrNull(NULL, 3) + toDecimal512('1.23', 2) AS result;


-- ==============================================
-- 8. 并发 & 事务一致性
-- ==============================================

SELECT '===并发&事物一致性===';

-- 物化视图
CREATE TABLE base (x Decimal512(3)) ENGINE=MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv TO base AS
SELECT toDecimal512('1.234', 3) AS x;

INSERT INTO base VALUES (2.345);
SELECT * FROM base;

SELECT '===test complete===';