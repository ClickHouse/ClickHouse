SET param_p = 'a';
WITH '{"a":[{"b":1},{"b":2}]}'::JSON AS j SELECT j.a[].b, j.{p:Identifier}[].b;
WITH '{"a":[[{"b":1}],[{"b":2},{"b":3}]]}'::JSON AS j SELECT j.a[][].b, j.{p:Identifier}[][].b;
WITH '{"a":[{"b":1},{"b":2}]}'::JSON AS j SELECT j.{unset_p:Identifier}[].b; -- { serverError UNKNOWN_QUERY_PARAMETER }
SELECT formatQuery('SELECT j.{p:Identifier}[].b');

DROP TABLE IF EXISTS t_json_path_param;
CREATE TABLE t_json_path_param (j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_path_param VALUES ('{"a":[{"b":1},{"b":2}]}');
SELECT j.a[].b, j.{p:Identifier}[].b FROM t_json_path_param;
SELECT formatQuerySingleLine('ALTER TABLE t_json_path_param DROP COLUMN IF EXISTS j.{p:Identifier}[]');
ALTER TABLE t_json_path_param DROP COLUMN IF EXISTS j.{unset_p:Identifier}[]; -- { serverError UNKNOWN_QUERY_PARAMETER }
DROP TABLE t_json_path_param;

SET param_p = 'o';
WITH '{"o":{"x":1}}'::JSON AS j SELECT j.^o, j.o.x.:Int64, j.@o;
WITH '{"o":{"x":1}}'::JSON AS j SELECT j.^{p:Identifier}; -- { clientError SYNTAX_ERROR }
WITH '{"o":{"x":1}}'::JSON AS j SELECT j.o.x.:{p:Identifier}; -- { clientError SYNTAX_ERROR }
WITH '{"o":{"x":1}}'::JSON AS j SELECT j.@{p:Identifier}; -- { clientError SYNTAX_ERROR }
