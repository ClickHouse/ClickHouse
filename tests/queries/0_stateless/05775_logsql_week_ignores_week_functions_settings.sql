-- LogsQL weeks start on Monday regardless of `week_functions_starting_day`: the `week_range` filter
-- and the `week` buckets of `stats`.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_week;
CREATE TABLE logs_week
(
    `_time` DateTime,
    `_msg` String
) ENGINE = MergeTree ORDER BY _time;

-- 2024-01-01 is a Monday.
INSERT INTO logs_week VALUES
    ('2024-01-01 06:00:00', 'monday'),
    ('2024-01-06 12:00:00', 'saturday'),
    ('2024-01-07 12:00:00', 'sunday'),
    ('2024-01-08 06:00:00', 'next monday');

SET enable_logsql_dialect = 1;
SET logsql_table = 'logs_week';

SELECT 'week_range, weeks from Tuesday';
SET week_functions_starting_day = 'tuesday';
SET dialect = 'logsql';
_time:week_range[Mon, Mon] | count();
_time:week_range[Sat, Sat] | count();
SET dialect = 'clickhouse';

SELECT 'stats by week, weeks from Sunday';
SET week_functions_starting_day = 'sunday';
SET to_start_of_interval_preserves_argument_type = 1;
SET dialect = 'logsql';
* | stats by (_time:week) count() | sort by (_time);
* | stats by (_time:week offset 1d) count() | sort by (_time);
SET dialect = 'clickhouse';

SELECT 'stats by week, weeks from Sunday, legacy result type';
SET to_start_of_interval_preserves_argument_type = 0;
SET dialect = 'logsql';
* | stats by (_time:week) count() | sort by (_time);
SET dialect = 'clickhouse';

DROP TABLE logs_week;
