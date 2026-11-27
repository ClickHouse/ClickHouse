-- `SETTINGS` after `FORMAT` of the query in `body(...)` of the `url` table function apply to the whole query,
-- also when its last operand is a set operation in parentheses, which cannot take them as its own `SETTINGS` clause.
-- The request is sent to the HTTP interface of the server itself, which executes the query from the URL followed by the body.

SET enable_analyzer = 1;

-- Every operand sees the setting: 0 + 13 + 13 + 13.
SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+0', TSV, 'x UInt64',
    body(SELECT '+ ' || getSetting('max_threads')::String
         UNION ALL (SELECT '+ ' || getSetting('max_threads')::String UNION ALL SELECT '+ ' || getSetting('max_threads')::String)
         FORMAT TSVRaw SETTINGS max_threads = 13));

-- The `SETTINGS` clause of the operand in parentheses is its own.
SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+0', TSV, 'x UInt64',
    body(SELECT '+ ' || getSetting('max_threads')::String
         UNION ALL (SELECT '+ ' || getSetting('max_threads')::String UNION ALL SELECT '+ ' || getSetting('max_threads')::String SETTINGS max_threads = 7)
         FORMAT TSVRaw SETTINGS max_threads = 13));

-- Format settings: the rows are `-1+2`, `-3+4` and `-5+6`.
SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+0', TSV, 'x Int64',
    body(SELECT -1 AS x, 2 AS y UNION ALL (SELECT -3, 4 UNION ALL SELECT -5, 6) FORMAT CSV SETTINGS format_csv_delimiter = '+'));

-- The same for `INTERSECT` and `EXCEPT`.
SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+0', TSV, 'x UInt64',
    body(SELECT '+ ' || getSetting('max_threads')::String INTERSECT (SELECT '+ 13' UNION ALL SELECT '+ 14') FORMAT TSVRaw SETTINGS max_threads = 13));
SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+0', TSV, 'x UInt64',
    body(SELECT '+ ' || getSetting('max_threads')::String EXCEPT (SELECT '+ 12' UNION ALL SELECT '+ 14') FORMAT TSVRaw SETTINGS max_threads = 13));

-- The formatted query has the settings at the same scope, and it is parsed back.
SELECT formatQuerySingleLine('SELECT * FROM url(\'http://127.0.0.1:8123/\', body(SELECT 1 AS x, 2 AS y UNION ALL (SELECT 3, 4 UNION ALL SELECT 5, 6) FORMAT CSV SETTINGS format_csv_delimiter = \';\'))') AS q FORMAT TSVRaw;
SELECT formatQuerySingleLine('SELECT * FROM url(\'http://127.0.0.1:8123/\', body(((SELECT 1 AS x, 2 AS y) UNION ALL (SELECT 3, 4 UNION ALL SELECT 5, 6) SETTINGS format_csv_delimiter = \';\'), \'CSV\'))') AS q FORMAT TSVRaw;
SELECT formatQuerySingleLine('SELECT * FROM url(\'http://127.0.0.1:8123/\', body((SELECT 1 UNION ALL (SELECT 2 UNION ALL SELECT 3) SETTINGS max_threads = 1)))') AS q FORMAT TSVRaw;
SELECT * FROM url('http://127.0.0.1:8123/?query=SELECT+0', TSV, 'x UInt64',
    body(((SELECT '+ ' || getSetting('max_threads')::String)
         UNION ALL (SELECT '+ ' || getSetting('max_threads')::String UNION ALL SELECT '+ ' || getSetting('max_threads')::String)
         SETTINGS max_threads = 13), 'TSVRaw'));
