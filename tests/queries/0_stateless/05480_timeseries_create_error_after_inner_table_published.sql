-- Tags: no-parallel, no-fasttest
-- Tag no-parallel: the failpoint is global and would fail the `CREATE` of a `TimeSeries` table in another test.
-- Tag no-fasttest: tests that enable a failpoint run alone, and they are kept out of the fast test.

-- An inner table of a `TimeSeries` table whose `CREATE` fails after the table is published, such as from its `startup`, is dropped
-- with the others when the `CREATE` of the `TimeSeries` table fails.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

SYSTEM ENABLE FAILPOINT time_series_inner_table_fail_after_create;
CREATE TABLE ts ENGINE = TimeSeries; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT time_series_inner_table_fail_after_create;
SELECT count() FROM system.tables WHERE database = currentDatabase();

-- A valid `CREATE` still creates the table with its inner tables.
CREATE TABLE ts ENGINE = TimeSeries;
SELECT count() > 1 FROM system.tables WHERE database = currentDatabase();

DROP TABLE ts;
