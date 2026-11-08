-- A projection text index assigns document ids to the rows of the parent part,
-- so its expression must not expand rows with `arrayJoin`, also through a SQL UDF.

SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_proj_text_array_join;
DROP TABLE IF EXISTS t_proj_text_array_join_2;
DROP FUNCTION IF EXISTS udf_proj_text_array_join_05570;

CREATE FUNCTION udf_proj_text_array_join_05570 AS (x) -> arrayJoin(x);

CREATE TABLE t_proj_text_array_join (id UInt64, a Array(String)) ENGINE = MergeTree ORDER BY id;

ALTER TABLE t_proj_text_array_join ADD PROJECTION p INDEX arrayJoin(a) TYPE text(tokenizer = splitByNonAlpha); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_proj_text_array_join ADD PROJECTION p INDEX udf_proj_text_array_join_05570(a) TYPE text(tokenizer = splitByNonAlpha); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_proj_text_array_join_2 (id UInt64, a Array(String), PROJECTION p INDEX arrayJoin(a) TYPE text(tokenizer = splitByNonAlpha)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- An index on the array itself is fine.
ALTER TABLE t_proj_text_array_join ADD PROJECTION p INDEX a TYPE text(tokenizer = splitByNonAlpha);
INSERT INTO t_proj_text_array_join VALUES (2, ['x y', 'z']), (1, ['q', 'w']);
SELECT id FROM t_proj_text_array_join WHERE has(a, 'z');

DROP TABLE t_proj_text_array_join;
DROP FUNCTION udf_proj_text_array_join_05570;
