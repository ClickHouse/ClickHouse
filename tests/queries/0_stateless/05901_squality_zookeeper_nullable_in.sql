-- Tags: zookeeper

SET in_null_as_unknown = 1;

SELECT
    (SELECT count() FROM system.zookeeper WHERE path IN ('/', NULL)) =
    (SELECT count() FROM system.zookeeper WHERE path IN ('/'));
