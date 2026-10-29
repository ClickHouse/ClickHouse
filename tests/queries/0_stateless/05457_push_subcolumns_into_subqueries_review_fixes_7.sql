-- A tuple element whose flattened name is a nested path of another element (`a.b` next to `a`) is not
-- pushed through any number of subquery layers: `getSubcolumn(c, 'a.b')` would read `a`.`b` instead.
-- A filter attached during planning that reads the whole column through an `ALIAS` column keeps it read.
-- A `JSON` column whose old parts keep the type from before a lazy type hint change is read whole,
-- because a typed subcolumn read directly from such a part gives defaults.

SET enable_analyzer = 1;
SET optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE IF EXISTS t_amb;
DROP TABLE IF EXISTS t_alias_filter;
DROP TABLE IF EXISTS t_lazy_hint;

CREATE TABLE t_amb (c Tuple(a Tuple(b UInt32), `a.b` UInt32)) ENGINE = Memory;
INSERT INTO t_amb SELECT CAST(tuple(tuple(3), 99), 'Tuple(a Tuple(b UInt32), `a.b` UInt32)');

SELECT 'ambiguous tuple element';
SELECT tupleElement(c, 'a.b') FROM (SELECT c FROM t_amb);
SELECT tupleElement(c, 'a.b') FROM (SELECT c FROM (SELECT c FROM t_amb));
SELECT tupleElement(c, 2) FROM (SELECT c FROM (SELECT c FROM t_amb));
SELECT tupleElement(c, 'a.b') FROM (SELECT c FROM (SELECT c FROM (SELECT c FROM t_amb)));
SELECT x FROM (SELECT tupleElement(c, 'a.b') AS x FROM (SELECT c FROM (SELECT c FROM t_amb)));
SELECT tupleElement(c, 'a') FROM (SELECT c FROM (SELECT c FROM t_amb));

CREATE TABLE t_alias_filter (tup Tuple(a String, b UInt8), a1 ALIAS tup, a2 ALIAS a1, b1 ALIAS tup.b)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_filter SELECT (repeat(hex(cityHash64(number)), 12), number % 255) FROM numbers(10000);

SELECT 'additional filter reads the whole column through an ALIAS column';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_alias_filter)
    SETTINGS additional_table_filters = {'t_alias_filter': 'cityHash64(a1) > 0'}) WHERE explain LIKE '%tup.a%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_alias_filter)
    SETTINGS additional_table_filters = {'t_alias_filter': 'cityHash64(a2) > 0'}) WHERE explain LIKE '%tup.a%';
SELECT sum(cityHash64(tup.a)) = (SELECT sum(cityHash64(tup.a)) FROM t_alias_filter WHERE cityHash64(tup) > 0)
FROM (SELECT tup FROM t_alias_filter) SETTINGS additional_table_filters = {'t_alias_filter': 'cityHash64(a1) > 0'};

SELECT 'additional filter reads another subcolumn through an ALIAS column';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT sum(cityHash64(tup.a)) FROM (SELECT tup FROM t_alias_filter)
    SETTINGS additional_table_filters = {'t_alias_filter': 'b1 > 0'}) WHERE explain LIKE '%tup.a%';

CREATE TABLE t_lazy_hint (x UInt64, json JSON(SHARED a UInt64))
ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
ALTER TABLE t_lazy_hint MODIFY COLUMN json JSON(SHARED a UInt64, SHARED b UInt64) SETTINGS enable_json_lazy_type_hints = 1;
INSERT INTO t_lazy_hint SELECT number, concat('{"a":', toString(number), ',"b":', toString(number * 2), '}') FROM numbers(40);
ALTER TABLE t_lazy_hint MODIFY COLUMN json JSON(SHARED a UInt64) SETTINGS enable_json_lazy_type_hints = 1;

SELECT 'typed subcolumn over a part with a lazily removed type hint';
SELECT sum(json.b.:Int64) FROM (SELECT json FROM t_lazy_hint) SETTINGS optimize_push_subcolumns_into_subqueries = 0;
SELECT sum(json.b.:Int64) FROM (SELECT json FROM t_lazy_hint) SETTINGS optimize_push_subcolumns_into_subqueries = 1;
SELECT sum(json.b.:Int64) FROM (SELECT json FROM (SELECT json FROM t_lazy_hint)) SETTINGS optimize_push_subcolumns_into_subqueries = 1;

DROP TABLE t_amb;
DROP TABLE t_alias_filter;
DROP TABLE t_lazy_hint;
