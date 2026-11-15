-- Random settings limits: query_plan_remove_unused_columns=(1, 1)
-- A count through a view or a subquery over a Merge table must not compute the default of a column that old parts lack.

DROP VIEW IF EXISTS vw;
DROP VIEW IF EXISTS vv;
DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS mm;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS mu;
DROP TABLE IF EXISTS t_old;
DROP TABLE IF EXISTS t_new;
DROP TABLE IF EXISTS u_old;
DROP TABLE IF EXISTS u_new;

CREATE TABLE t_old (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_old VALUES ('n/a');
ALTER TABLE t_old ADD COLUMN n UInt8 DEFAULT toUInt8(s) FIRST;
CREATE TABLE t_new (n UInt8, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_new VALUES (1, '1');
CREATE TABLE m (n UInt8, s String) ENGINE = Merge(currentDatabase(), '^t_(old|new)$');
CREATE TABLE mm (n UInt8, s String) ENGINE = Merge(currentDatabase(), '^m$');
CREATE VIEW v AS SELECT * FROM m;
CREATE VIEW vv AS SELECT * FROM v;
CREATE VIEW vw AS SELECT * FROM m WHERE s != '';

CREATE TABLE u_old (k UInt64, url String) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO u_old SELECT number, concat('https://', hex(sipHash64(number)), '.com/', repeat(hex(sipHash128(number)), 30)) FROM numbers(1000);
ALTER TABLE u_old ADD COLUMN domain String MATERIALIZED domain(url);
CREATE TABLE u_new (k UInt64, url String, domain String MATERIALIZED domain(url)) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO u_new (k, url) VALUES (1000, 'https://a.com/');
CREATE TABLE mu (domain String, url String) ENGINE = Merge(currentDatabase(), '^u_(old|new)$');

SELECT count() FROM v;
SELECT count() FROM (SELECT * FROM m);
SELECT count() FROM vv;
SELECT count() FROM (SELECT * FROM mm);
SELECT count() FROM vw;
SELECT count() FROM m;

DROP ROW POLICY IF EXISTS rp ON t_old;
CREATE ROW POLICY rp ON t_old USING s != '' TO ALL;
SELECT count() FROM v;
DROP ROW POLICY rp ON t_old;

SELECT count() FROM (SELECT * FROM mu) SETTINGS max_bytes_to_read = 100000;

DROP VIEW vw;
DROP VIEW vv;
DROP VIEW v;
DROP TABLE mm;
DROP TABLE m;
DROP TABLE mu;
DROP TABLE t_old;
DROP TABLE t_new;
DROP TABLE u_old;
DROP TABLE u_new;
