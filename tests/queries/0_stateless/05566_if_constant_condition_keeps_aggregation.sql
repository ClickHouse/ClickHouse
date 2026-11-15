-- A constant-condition if must not drop the query's aggregate functions or arrayJoin with the branch not taken
DROP TABLE IF EXISTS t_if_const_agg;
DROP TABLE IF EXISTS t_if_const_agg_empty;
CREATE TABLE t_if_const_agg (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_if_const_agg SELECT number, number FROM numbers(10);
CREATE TABLE t_if_const_agg_empty (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple();

SELECT count() FROM (SELECT if(1, toUInt32(5), max(a)) FROM t_if_const_agg);
SELECT count() FROM (SELECT if(0, max(a), toUInt32(5)) FROM t_if_const_agg);
SELECT count() FROM (SELECT multiIf(1, toUInt32(5), max(a)) FROM t_if_const_agg);
SELECT count() FROM (SELECT multiIf(1, toUInt32(5), max(a)) FROM t_if_const_agg SETTINGS optimize_multiif_to_if = 0);
SELECT count() FROM (SELECT CASE WHEN 1 THEN toUInt32(5) ELSE max(a) END FROM t_if_const_agg);
SELECT count() FROM (SELECT if(1, toUInt64(5), count()) FROM t_if_const_agg);
SELECT count() FROM (SELECT if(1, toUInt32(5), if(0, max(a), max(b))) FROM t_if_const_agg);
SELECT count() FROM (SELECT if(1, toUInt32(5), max(max(a)) OVER ()) FROM t_if_const_agg);
SELECT count() FROM (SELECT 1 FROM t_if_const_agg HAVING if(1, 1, max(a) > 0));
SELECT count() FROM (SELECT 1 FROM t_if_const_agg ORDER BY if(1, toUInt32(1), max(a)));
SELECT count() FROM (SELECT if(1, toUInt32(5), max(a)) FROM t_if_const_agg_empty);
SELECT (SELECT if(1, toUInt32(5), max(a)) FROM t_if_const_agg);
SELECT count() FROM (SELECT if(1, toUInt32(5), max(a)) FROM remote('127.0.0.{1,2}', currentDatabase(), t_if_const_agg));
SELECT if(1, toUInt32(5), max(a)), if(0, max(a), toUInt32(7)) FROM t_if_const_agg;
SELECT count() FROM (SELECT if(1, toUInt8(5), arrayJoin([1, 2, 3])) FROM numbers(2));
SELECT sum(c) FROM (SELECT if(1, b, arrayJoin([b, b])) AS k, count() AS c FROM t_if_const_agg GROUP BY k);
SELECT count(), sum(c) FROM (SELECT if(1, b, arrayJoin([b, b])) AS k, count() AS c FROM t_if_const_agg GROUP BY ROLLUP(k) SETTINGS group_by_use_nulls = 1);

-- Controls: an aggregate of a subquery is not this query's, and an aggregate in the branch taken is still folded to.
SELECT count() FROM (SELECT if(1, toUInt32(5), (SELECT max(a) FROM t_if_const_agg)) FROM t_if_const_agg);
SELECT count() FROM (SELECT 1 FROM t_if_const_agg AS t ORDER BY if(1, toNullable(t.a), (SELECT max(n.a) FROM t_if_const_agg AS n WHERE n.a = t.a))) SETTINGS allow_correlated_subqueries = 1;
SELECT sum(if(0, (SELECT max(n.a) FROM t_if_const_agg AS n WHERE n.a = t.a), toNullable(t.b))) FROM t_if_const_agg AS t SETTINGS allow_correlated_subqueries = 1;
SELECT count() FROM (SELECT 1 FROM t_if_const_agg AS t ORDER BY if(1, toNullable(t.a), (SELECT arrayJoin([n.a]) FROM t_if_const_agg AS n WHERE n.a = t.a))) SETTINGS allow_correlated_subqueries = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT if(1, max(a), toUInt32(5)) FROM t_if_const_agg) WHERE explain ILIKE '%function_name: if,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT if(1, a, b) FROM t_if_const_agg) WHERE explain ILIKE '%function_name: if,%';

DROP TABLE t_if_const_agg;
DROP TABLE t_if_const_agg_empty;
