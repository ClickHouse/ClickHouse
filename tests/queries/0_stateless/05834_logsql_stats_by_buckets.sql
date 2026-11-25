-- Expected behaviour source: parsePipeStats() in src/Parsers/LogsQL/LogsQLPipes.cpp, which
-- implements the grouping list of the LogsQL `stats` pipe: the `_time` bucket steps and their
-- `offset`, the `field:/<bits>` IPv4 subnet buckets, the numeric `field:<step>` buckets, the
-- `switch(...)` case lists and the per-function `if (...)` filters.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_05834;
CREATE TABLE logs_05834
(
    `_time` DateTime,
    `_msg` String,
    `ip` String,
    `num` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05834 VALUES
    ('2024-01-01 00:40:00', 'alpha', '10.1.2.3', '-7'),
    ('2024-01-01 00:50:00', 'alpha', '10.1.2.200', '3'),
    ('2024-01-01 01:40:00', 'gamma', '10.9.0.1', '0.6');

SET enable_logsql_dialect = 1;
SET logsql_table = 'logs_05834';
SET dialect = 'logsql';

-- The grouping keys are a parenthesized list, even for a single field.
* | stats by _msg count(); -- { error SYNTAX_ERROR }

-- An empty field name means `_msg`, like everywhere else in LogsQL.
* | stats by ("") count() | sort by (_msg);

-- `field:/<bits>` buckets the field as an IPv4 subnet: the values are grouped by their network
-- address. The bits must fit into an address, and a subnet bucket has no offset.
* | stats by (ip:/24) count() | sort by (ip);
* | stats by (ip:/33) count(); -- { error SYNTAX_ERROR }
* | stats by (ip:/24 offset 1) count(); -- { error SYNTAX_ERROR }

-- A `_time` bucket step is a named step or a duration, and so is its offset. The offset shifts
-- the bucket boundaries, so `1h offset 30m` buckets start at half past the hour.
* | stats by (_time:nope) count(); -- { error SYNTAX_ERROR }
* | stats by (_time:1h offset 30m) count() | sort by (_time);
* | stats by (_time:1h offset nope) count(); -- { error SYNTAX_ERROR }

-- A numeric bucket step and offset are numbers. An integral step with an integral offset keeps
-- the keys of integer values exact, a fractional one buckets the Float64 value of the field.
* | stats by (num:nope) count(); -- { error SYNTAX_ERROR }
* | stats by (num:10 offset nope) count(); -- { error SYNTAX_ERROR }
* | stats by (num:10 offset -5) count() | sort by (num);
* | stats by (num:0.5 offset 0.25) count() | sort by (num);

-- The fields of the `by` list are separated by commas.
* | stats by (num nope) count(); -- { error SYNTAX_ERROR }

-- `switch(...)` is a parenthesized, comma-separated list of `case`/`default` entries, with at
-- least one entry and at most one `default`.
* | stats count() switch x; -- { error SYNTAX_ERROR }
* | stats count() switch(); -- { error SYNTAX_ERROR }
* | stats count() switch(nope as a); -- { error SYNTAX_ERROR }
* | stats count() switch(default as a, default as b); -- { error SYNTAX_ERROR }
* | stats count() switch(case (*) as a nope); -- { error SYNTAX_ERROR }

-- A `switch(...)` is followed by a comma and more stats functions, or by the end of the pipe.
* | stats count() switch(case (ip:="10.1.2.3") as one, default as rest), count() as total;
* | stats count() switch(default as rest) nope; -- { error SYNTAX_ERROR }

-- A per-function `if (...)` takes a parenthesized filter; `if (*)` matches every row.
* | stats count() if x; -- { error SYNTAX_ERROR }
* | stats count() if (_msg:alpha | count(); -- { error SYNTAX_ERROR }
* | stats count() if (*) as all_rows;

-- Without an alias the result column is named after the function and the filter text.
* | stats count() if (_msg:alpha) | fields "count(*) if (_msg:alpha)";

-- A stats function is followed by an alias, a comma or the end of the pipe.
* | stats count() as c nope; -- { error SYNTAX_ERROR }

SET dialect = 'clickhouse';
DROP TABLE logs_05834;
