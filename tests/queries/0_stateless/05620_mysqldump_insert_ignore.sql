-- Modifiers between `INSERT`/`REPLACE` and `INTO` must be skipped, also in statements for other tables.

SELECT * FROM format(MySQLDump, $$INSERT IGNORE INTO `t1` VALUES (1,'a'),(2,NULL);
INSERT HIGH_PRIORITY INTO `t2` VALUES (9,'z');
INSERT DELAYED INTO `t1` VALUES (3,'b');
insert  low_priority   ignore into `t1` VALUES (4,'c');
REPLACE LOW_PRIORITY INTO `t1` VALUES (5,'d');$$) ORDER BY c1;

SELECT * FROM format(MySQLDump, 'c1 UInt32, c2 String', $$INSERT IGNORE INTO `t1` VALUES (1,'a');
INSERT IGNORE INTO `t1` VALUES (2,'b');$$) ORDER BY c1;

SELECT * FROM format(MySQLDump, 'c1 UInt32', $$INSERT BOGUS INTO `t1` VALUES (1);$$); -- { serverError INCORRECT_DATA }
