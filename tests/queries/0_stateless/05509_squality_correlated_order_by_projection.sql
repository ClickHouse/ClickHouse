DROP TABLE IF EXISTS squality_correlated_order_by;

CREATE TABLE squality_correlated_order_by (a Int32, b Int32) ENGINE = Memory;
INSERT INTO squality_correlated_order_by VALUES (1, 2), (2, 1), (3, 3);

SELECT
    (SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS cnt
FROM squality_correlated_order_by AS outer_table
ORDER BY 1;

SELECT
    (SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS cnt
FROM squality_correlated_order_by AS outer_table
ORDER BY cnt;

SELECT
    EXISTS(SELECT 1 FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS present
FROM squality_correlated_order_by AS outer_table
ORDER BY 1;

SELECT cnt
FROM
(
    SELECT
        outer_table.a,
        (SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS cnt
    FROM squality_correlated_order_by AS outer_table
    ORDER BY cnt
)
ORDER BY cnt;

SELECT
    (SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS cnt,
    (SELECT count() FROM squality_correlated_order_by AS inner_table WHERE inner_table.b < outer_table.b) AS cnt
FROM squality_correlated_order_by AS outer_table
ORDER BY cnt;

SELECT number IN (1, 2) AS present
FROM numbers(3)
ORDER BY 1;

DROP TABLE squality_correlated_order_by;
