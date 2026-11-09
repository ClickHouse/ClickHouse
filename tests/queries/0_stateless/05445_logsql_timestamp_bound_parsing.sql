-- Timestamp bounds of the LogsQL `_time:` filter, as documented in
-- docs/guides/clickhouse/logsql.mdx ("Time filter"): a timestamp prefix denotes
-- the whole period of its precision, and a trailing `+hh:mm`/`-hh:mm` offset may
-- follow any prefix directly, so `2024-01-02:30` is the month `2024-01` read in
-- the zone `-02:30`. Anything else is rejected as a syntax error.
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_05445;
CREATE TABLE logs_05445
(
    `_time` DateTime64(9, 'UTC'),
    `_msg` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05445 VALUES
    ('2024-01-01 00:00:00.000000000', 'a'),
    ('2024-01-01 00:00:00.500000000', 'b'),
    ('2024-01-01 02:30:00.000000000', 'c'),
    ('2024-01-15 12:00:00.000000000', 'd');

SET enable_logsql_dialect = 1;
SET logsql_table = 'logs_05445';
SET dialect = 'logsql';

-- A bare `YYYY-MM` prefix is the whole month in local civil time: all four rows.
_time:2024-01 | stats count() as rows_in_month;

-- A detached negative offset shifts that month: `2024-01` in the zone `-02:30`
-- starts at 02:30 UTC, so the midnight rows drop out and the January 15 row stays.
_time:2024-01-02:30 | fields _msg | sort by (_msg);

-- The same detachment at second precision: 10:00:00 at `-02:00` is 12:00:00 UTC.
_time:2024-01-15T10:00:00-02:00 | fields _msg;

-- A fractional part is padded to nanoseconds and narrows the period to one
-- nanosecond, so `.5` and `.500000000` both select exactly the half-second row.
_time:2024-01-01T00:00:00.5 | fields _msg;
_time:2024-01-01T00:00:00.500000000 | fields _msg;

-- An empty bound has nothing to parse.
_time:"" | stats count(); -- { error SYNTAX_ERROR }

-- A separator that is not followed by a full two-digit component leaves the
-- position untouched, so the leftover text is then read as a timezone offset
-- and fails there instead: short month, and a day cut off by the `Z`.
_time:2024-1 | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-0Z | stats count(); -- { error SYNTAX_ERROR }

-- Out-of-range components are rejected one by one.
_time:2024-13 | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-02T25 | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-02T12:60 | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-02T12:00:61 | stats count(); -- { error SYNTAX_ERROR }

-- A decimal point with no digit after it is not a fractional part.
_time:2024-01-01T00:00:00. | stats count(); -- { error SYNTAX_ERROR }

-- An attached offset must be the full `±hh:mm`: the hour alone, a missing
-- minute, a truncated minute and an hour above 23 are all rejected.
_time:2024+1 | stats count(); -- { error SYNTAX_ERROR }
_time:2024+12 | stats count(); -- { error SYNTAX_ERROR }
_time:2024+12: | stats count(); -- { error SYNTAX_ERROR }
_time:2024+25:00 | stats count(); -- { error SYNTAX_ERROR }

-- Trailing text after a complete bound is never ignored, whether it follows the
-- year, a capped nine-digit fraction, or an otherwise valid offset of either sign.
_time:2024x | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-01T00:00:00.5000000001 | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-02T03:04:05-01:30x | stats count(); -- { error SYNTAX_ERROR }
_time:2024-01-02T03:04:05+01:30x | stats count(); -- { error SYNTAX_ERROR }

SET dialect = 'clickhouse';
DROP TABLE logs_05445;
