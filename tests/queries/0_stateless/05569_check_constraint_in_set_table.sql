-- A `Set` table on the right-hand side of `IN` in a `CHECK` constraint is a ready set:
-- checking it runs no query, so such constraints are accepted, as in released versions.

DROP TABLE IF EXISTS checked_ready_set;
DROP TABLE IF EXISTS allowed_ready_set;
DROP TABLE IF EXISTS not_a_set;

CREATE TABLE allowed_ready_set (id UInt64) ENGINE = Set;
INSERT INTO allowed_ready_set VALUES (1);

CREATE TABLE checked_ready_set (id UInt64, CONSTRAINT c CHECK id IN allowed_ready_set) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO checked_ready_set VALUES (1);
INSERT INTO checked_ready_set VALUES (2); -- { serverError VIOLATED_CONSTRAINT }

ALTER TABLE checked_ready_set ADD CONSTRAINT c2 CHECK id + 1 IN allowed_ready_set; -- the constraint itself is accepted
INSERT INTO checked_ready_set VALUES (1); -- { serverError VIOLATED_CONSTRAINT }
ALTER TABLE checked_ready_set DROP CONSTRAINT c2;

INSERT INTO allowed_ready_set VALUES (2);
INSERT INTO checked_ready_set VALUES (2);
SELECT * FROM checked_ready_set ORDER BY id;

-- A table of any other engine is still rejected.
CREATE TABLE not_a_set (id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE checked_ready_set ADD CONSTRAINT c3 CHECK id IN not_a_set; -- { serverError BAD_ARGUMENTS }

DROP TABLE checked_ready_set;
DROP TABLE allowed_ready_set;
DROP TABLE not_a_set;
