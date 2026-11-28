SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_set_nested_calendar_guard;
CREATE TABLE squality_set_nested_calendar_guard (k DateTime) ENGINE = Set;
INSERT INTO squality_set_nested_calendar_guard VALUES ('2000-01-01 00:00:00');

SELECT toDate('2000-01-01') IN squality_set_nested_calendar_guard; -- { serverError NOT_IMPLEMENTED }

DROP TABLE squality_set_nested_calendar_guard;
