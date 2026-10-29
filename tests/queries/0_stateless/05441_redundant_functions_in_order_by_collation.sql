-- Tags: no-fasttest
-- Under COLLATE distinct values of a column can compare equal, so a function of that column
-- after it in ORDER BY still breaks ties and has to stay in the sort key.

SET optimize_redundant_functions_in_order_by = 1;

SELECT s, hex(s) FROM (SELECT arrayJoin(['b', 'a', 'A', 'B']) AS s) ORDER BY s COLLATE 'en-u-ks-level1', hex(s);
SELECT s FROM (SELECT arrayJoin(['b', 'a', 'A', 'B']) AS s) ORDER BY s COLLATE 'en-u-ks-level1', hex(s) LIMIT 1;
