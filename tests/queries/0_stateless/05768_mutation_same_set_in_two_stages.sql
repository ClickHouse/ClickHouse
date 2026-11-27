-- Two commands of one mutation are two stages, and each stage has sets of its own. The same `IN` subquery in
-- both of them gives two sets with the same key in the `PreparedSetsCache` of a plain `MergeTree` mutation.
-- Built at the same time, the second set waited for the first in the same executor: the mutation never finished.

DROP TABLE IF EXISTS t_same_set_two_stages;

CREATE TABLE t_same_set_two_stages (k UInt64, v UInt8, w UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_same_set_two_stages SELECT number, 0, 0 FROM numbers(4);

ALTER TABLE t_same_set_two_stages
    UPDATE v = k IN (SELECT number FROM numbers(2)) WHERE 1,
    UPDATE w = k IN (SELECT number FROM numbers(2)) WHERE 1
SETTINGS mutations_sync = 2;

SELECT k, v, w FROM t_same_set_two_stages ORDER BY k;

-- The same with the sets in the predicates.
ALTER TABLE t_same_set_two_stages
    UPDATE v = 2 WHERE k IN (SELECT number FROM numbers(1, 2)),
    UPDATE w = 3 WHERE k IN (SELECT number FROM numbers(1, 2))
SETTINGS mutations_sync = 2;

SELECT k, v, w FROM t_same_set_two_stages ORDER BY k;

DROP TABLE t_same_set_two_stages;
