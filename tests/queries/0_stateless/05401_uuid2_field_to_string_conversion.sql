-- Converting a `UUID2` constant to a `String` field keeps the identifier.
-- See https://github.com/ClickHouse/ClickHouse/pull/110084

SELECT s FROM values('s String', toUUID2('00000000-0000-0000-0000-000000000001'));
SELECT s FROM values('s String', toUUID('00000000-0000-0000-0000-000000000001'));
SELECT s FROM values('s String', [toUUID2('00000000-0000-0000-0000-000000000001')]);
SELECT s FROM values('s String', tuple(toUUID2('00000000-0000-0000-0000-000000000001'), 1));
SELECT s FROM values('s String', map(toUUID2('00000000-0000-0000-0000-000000000001'), toUUID2('00000000-0000-0000-0000-000000000002')));

CREATE TABLE t_05401 (s String) ENGINE = Memory;
SET input_format_values_deduce_templates_of_expressions = 0;
INSERT INTO t_05401 VALUES (toUUID2('00000000-0000-0000-0000-000000000001'));
SET input_format_values_deduce_templates_of_expressions = 1;
INSERT INTO t_05401 VALUES (toUUID2('00000000-0000-0000-0000-000000000003'));
SELECT s FROM t_05401 ORDER BY s;
DROP TABLE t_05401;
