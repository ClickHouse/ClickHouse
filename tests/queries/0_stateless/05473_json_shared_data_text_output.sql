-- Values of JSON paths stored in shared data must be written as text exactly like the same values in dynamic paths.

DROP TABLE IF EXISTS t_json_shared_data_text;
CREATE TABLE t_json_shared_data_text (id UInt64, d JSON, s JSON(max_dynamic_paths = 0)) ENGINE = Memory;

INSERT INTO t_json_shared_data_text SELECT id, j, j FROM values('id UInt64, j String',
    (1, '{"i":-42,"u":18446744073709551615,"f":3.25,"b":true,"str":"q\\"b\\\\s/c\\u0001é","dt":"2024-01-02","dt64":"2024-06-15 12:34:56.789"}'),
    (2, '{"an":[1,null,3],"as":["x","y"],"ae":[],"mixed":[1,"a",2.5],"aj":[{"k":1},{"k":"v"}],"a":{"b":{"c":"nested"}}}'));

SELECT id, JSONDynamicPaths(s), arraySort(JSONSharedDataPaths(s)) = arraySort(JSONDynamicPaths(d)), JSONSharedDataPaths(d) FROM t_json_shared_data_text ORDER BY id;

SELECT id, toString(s) FROM t_json_shared_data_text ORDER BY id;
SELECT count() FROM t_json_shared_data_text WHERE toString(s) != toString(d);

SELECT s FROM t_json_shared_data_text ORDER BY id FORMAT JSONEachRow;
SELECT d FROM t_json_shared_data_text ORDER BY id FORMAT JSONEachRow;
SELECT s FROM t_json_shared_data_text ORDER BY id FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT d FROM t_json_shared_data_text ORDER BY id FORMAT JSONEachRow SETTINGS output_format_json_quote_64bit_integers = 1;
SELECT s FROM t_json_shared_data_text ORDER BY id FORMAT Vertical;
SELECT d FROM t_json_shared_data_text ORDER BY id FORMAT Vertical;

DROP TABLE t_json_shared_data_text;
