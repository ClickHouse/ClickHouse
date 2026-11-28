-- Statements of other tables in a MySQL dump are skipped up to their end, also when a quoted name in them has a space.

SELECT * FROM format(MySQLDump, 'id Int32, b Int32', $$INSERT INTO `t1` (`id`, `b`) VALUES (1,100),(2,200);
INSERT INTO `t2` (`first name`) VALUES ('x');
$$) ORDER BY id;

SELECT * FROM format(MySQLDump, $$INSERT INTO "t2" ("first name") VALUES ('x');
INSERT INTO "t1" ("id", "b") VALUES (1,100),(2,200);
$$) ORDER BY id SETTINGS input_format_mysql_dump_table_name = 't1';

SELECT * FROM format(MySQLDump, $$CREATE TABLE `t2` (`first name` int);
CREATE TABLE `t1` (`id` int, `b` int);
INSERT INTO `t1` VALUES (1,100),(2,200);
$$) ORDER BY id SETTINGS input_format_mysql_dump_table_name = 't1';

SELECT * FROM format(MySQLDump, 'id Int32, b Int32', $$INSERT INTO `t1` VALUES (1,100);
REPLACE INTO `t2` (`a`,`b c`) VALUES (1);
INSERT INTO `t1` VALUES (2,200);
$$) ORDER BY id;

INSERT INTO FUNCTION file(currentDatabase() || '_dump.sql', RawBLOB) SELECT $$INSERT INTO `t1` VALUES (1,100),(2,200);
INSERT INTO `t2` (`first name`) VALUES ('x');
$$ SETTINGS engine_file_truncate_on_insert = 1;
SELECT count() FROM file(currentDatabase() || '_dump.sql', MySQLDump, 'id Int32, b Int32');
