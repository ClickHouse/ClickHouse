-- A row policy or `additional_table_filters` of a table read by a materialized view that names the view's source table
-- reads the inserted block, like the rest of the view's query.

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src VALUES (1);
CREATE TABLE src_null (x UInt64) ENGINE = Null;

CREATE TABLE child_policy (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE child_join (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE child_atf (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE child_null (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE child_view (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE other (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO child_policy VALUES (1), (2), (3);
INSERT INTO child_join VALUES (1), (2), (3);
INSERT INTO child_atf VALUES (1), (2), (3);
INSERT INTO child_null VALUES (1), (2), (3);
INSERT INTO child_view VALUES (1);
INSERT INTO other VALUES (1), (2), (3), (4);

CREATE ROW POLICY OR REPLACE p_in_table ON child_policy USING x IN src TO ALL;
CREATE ROW POLICY OR REPLACE p_in_join ON child_join USING x IN (SELECT o.x FROM other AS o INNER JOIN src AS s ON o.x = s.x) TO ALL;
CREATE ROW POLICY OR REPLACE p_null ON child_null USING x IN src_null TO ALL;
CREATE ROW POLICY OR REPLACE p_view ON child_view USING x IN src TO ALL;
CREATE VIEW v AS SELECT x FROM child_view;

CREATE TABLE dst (name String, x UInt64) ENGINE = MergeTree ORDER BY (name, x);
CREATE MATERIALIZED VIEW mv_in_table TO dst AS SELECT 'policy_in_table' AS name, c.x AS x FROM src AS s CROSS JOIN child_policy AS c;
CREATE MATERIALIZED VIEW mv_in_join TO dst AS SELECT 'policy_in_join' AS name, c.x AS x FROM src AS s CROSS JOIN child_join AS c;
CREATE MATERIALIZED VIEW mv_atf TO dst AS SELECT 'additional_table_filters' AS name, c.x AS x FROM src AS s CROSS JOIN child_atf AS c;
CREATE MATERIALIZED VIEW mv_view TO dst AS SELECT 'view_inner' AS name, c.x AS x FROM src AS s CROSS JOIN v AS c;
CREATE MATERIALIZED VIEW mv_null TO dst AS SELECT 'null_source' AS name, c.x AS x FROM src_null AS s CROSS JOIN child_null AS c;

-- Not an asynchronous insert: its flush context has no current database.
INSERT INTO src SETTINGS async_insert = 0, additional_table_filters = {'child_atf': 'x IN src'} VALUES (2), (3), (4);
INSERT INTO src_null SETTINGS async_insert = 0 VALUES (2), (3), (4);

SELECT name, groupArray(x) FROM (SELECT DISTINCT name, x FROM dst ORDER BY name, x) GROUP BY name ORDER BY name;

DROP ROW POLICY p_in_table ON child_policy;
DROP ROW POLICY p_in_join ON child_join;
DROP ROW POLICY p_null ON child_null;
DROP ROW POLICY p_view ON child_view;
