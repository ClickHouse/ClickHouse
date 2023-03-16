-- Tags: no-parallel

DROP DATABASE IF EXISTS 25341_db;
CREATE DATABASE 25341_db;
DROP TABLE IF EXISTS 25341_db.25341_rptable;
DROP TABLE IF EXISTS 25341_db.25341_rptable_another;
CREATE TABLE 25341_db.25341_rptable (x UInt8, y UInt8) ENGINE = MergeTree ORDER BY x;

INSERT INTO 25341_db.25341_rptable VALUES (1, 10), (2, 20), (3, 30), (4, 40);

CREATE TABLE 25341_db.25341_rptable_another ENGINE = MergeTree ORDER BY x AS SELECT * FROM 25341_db.25341_rptable;


DROP ROW POLICY IF EXISTS 25341_filter_1 ON 25341_db.25341_rptable;
DROP ROW POLICY IF EXISTS 25341_filter_2 ON 25341_db.*;
DROP ROW POLICY IF EXISTS 25341_filter_3 ON 25341_db.25341_rptable;
DROP ROW POLICY IF EXISTS 25341_filter_4 ON 25341_db.25341_rptable;
DROP ROW POLICY IF EXISTS 25341_filter_5 ON 25341_db.*;

-- the test assumes users_without_row_policies_can_read_rows is true

SELECT 'None';
SELECT * FROM 25341_db.25341_rptable;

CREATE ROW POLICY 25341_filter_1 ON 25341_db.25341_rptable USING x=1 AS permissive TO ALL;
SELECT 'R1: x == 1';
SELECT * FROM 25341_db.25341_rptable;

CREATE ROW POLICY 25341_filter_2 ON 25341_db.* USING x=2 AS permissive TO ALL;
SELECT 'R1, R2: (x == 1) OR (x == 2)';
SELECT * FROM 25341_db.25341_rptable;

SELECT 'R1, R2: (x == 2) FROM ANOTHER';
SELECT * FROM 25341_db.25341_rptable_another;

CREATE ROW POLICY 25341_filter_3 ON 25341_db.25341_rptable USING x=3 AS permissive TO ALL;
SELECT 'R1, R2, R3: (x == 1) OR (x == 2) OR (x == 3)';
SELECT * FROM 25341_db.25341_rptable;

CREATE ROW POLICY 25341_filter_4 ON 25341_db.25341_rptable USING x<=2 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2)';
SELECT * FROM 25341_db.25341_rptable;

CREATE ROW POLICY 25341_filter_5 ON 25341_db.* USING y>=20 AS restrictive TO ALL;
SELECT 'R1, R2, R3, R4, R5: ((x == 1) OR (x == 2) OR (x == 3)) AND (x <= 2) AND (y >= 20)';
SELECT * FROM 25341_db.25341_rptable;

CREATE TABLE 25341_db.25341_after_rp ENGINE = MergeTree ORDER BY x AS SELECT * FROM 25341_db.25341_rptable;
SELECT * FROM 25341_db.25341_after_rp;

-- does not matter if policies or table are created first
SELECT 'R1, R2, R3, R4, R5: (x == 2) AND (y >= 20) FROM AFTER_RP';
SELECT * FROM 25341_db.25341_after_rp;

SELECT 'R1, R2, R3, R4, R5: (x == 2) AND (y >= 20) FROM ANOTHER';
SELECT * FROM 25341_db.25341_rptable_another;

DROP ROW POLICY 25341_filter_1 ON 25341_db.25341_rptable;
SELECT 'R2, R3, R4, R5: ((x == 2) OR (x == 3)) AND (x <= 2) AND (y >= 20)';
SELECT * FROM 25341_db.25341_rptable;

DROP ROW POLICY 25341_filter_2 ON 25341_db.*;
SELECT 'R3, R4, R5: (x == 3) AND (x <= 2) AND (y >= 20)';
SELECT * FROM 25341_db.25341_rptable;

DROP ROW POLICY 25341_filter_3 ON 25341_db.25341_rptable;
SELECT 'R4, R5: (x <= 2) AND (y >= 20)';
SELECT * FROM 25341_db.25341_rptable;

DROP ROW POLICY 25341_filter_4 ON 25341_db.25341_rptable;
SELECT 'R5: (x >= 2)';
SELECT * FROM 25341_db.25341_rptable;

CREATE TABLE 25341_db.25341_unexpected_columns (xx UInt8, yy UInt8) ENGINE = MergeTree ORDER BY xx;
SELECT 'Policy not applicable';
SELECT * FROM 25341_db.25341_unexpected_columns; -- { serverError 47 } -- Missing columns: 'x' while processing query

DROP ROW POLICY 25341_filter_5 ON 25341_db.*;
SELECT 'None';
SELECT * FROM 25341_db.25341_rptable;

SELECT 'No problematic policy, select works';
SELECT 'Ok' FROM 25341_db.25341_unexpected_columns;

DROP TABLE 25341_db.25341_rptable;
DROP TABLE 25341_db.25341_rptable_another;
DROP TABLE 25341_db.25341_unexpected_columns;
DROP DATABASE 25341_db;
