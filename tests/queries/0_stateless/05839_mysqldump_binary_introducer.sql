-- mysqldump writes BLOB, VARBINARY and BINARY values with the _binary charset introducer.

SELECT toTypeName(b), toTypeName(v), toTypeName(f), hex(b), if(v IS NULL, 'NULL', hex(v)), hex(f) FROM format(MySQLDump, $$CREATE TABLE `t` (`b` BLOB NOT NULL, `v` VARBINARY(4) DEFAULT NULL, `f` BINARY(3) NOT NULL);
INSERT INTO `t` VALUES (_binary 'A\0B',_binary '',_binary 'XYZ'),(_binary 'Q',NULL,_binary 'P\0R');
$$) ORDER BY hex(b);

SELECT hex(b) FROM format(MySQLDump, $$CREATE TABLE `t` (`b` BLOB NOT NULL);
INSERT INTO `t` VALUES (_binary '');
$$);

SELECT id, hex(b) FROM format(MySQLDump, 'id Int32, b String', $$INSERT INTO `t` VALUES (1,_binary'A\0B'),(2,_binary  'C');
$$) ORDER BY id;

SELECT id FROM format(MySQLDump, 'id Int32', $$INSERT INTO `t` (`id`, `b`) VALUES (1,_binary 'A\0B'),(2,_binary 'C');
$$) ORDER BY id SETTINGS input_format_skip_unknown_fields = 1;

DESC format(MySQLDump, $$INSERT INTO `t` VALUES (1,_binary 'A\0B');
$$);
SELECT c1, hex(c2) FROM format(MySQLDump, $$INSERT INTO `t` VALUES (1,_binary 'A\0B');
$$);
