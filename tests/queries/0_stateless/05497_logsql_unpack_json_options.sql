-- The options of the LogsQL `unpack_json` pipe: an implicit `from`, `preserve_keys`,
-- `result_prefix`, `keep_original_fields`, `skip_empty_results` and an `if` condition.
-- Expected behaviour: docs/guides/clickhouse/logsql.mdx (unsupported syntax is rejected
-- with NOT_IMPLEMENTED) and the `unpack_json` section of 04615_logsql_dialect.sql.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS logs_05497;
CREATE TABLE logs_05497
(
    `_time` DateTime,
    `_msg` String,
    `payload` String,
    `level` String,
    `a` String
) ENGINE = MergeTree ORDER BY _time;

INSERT INTO logs_05497 VALUES
    ('2024-01-01 00:00:00', 'first', '{"a":"x","obj":{"k":1},"z":null}', 'error', 'orig1'),
    ('2024-01-01 00:01:00', 'second', '{"a":"","obj":[1,2],"z":"q"}', 'info', 'orig2');

SET enable_logsql_dialect = 1;
SET logsql_table = 'logs_05497';
SET dialect = 'logsql';

-- The source field may be given without the `from` keyword; strings are decoded.
* | unpack_json payload fields (a) | fields a | sort by (a);

-- `preserve_keys` keeps the raw JSON text of the listed keys, the other keys are decoded.
* | unpack_json from payload fields (a, obj, z) preserve_keys (obj, z) | fields a, obj, z | sort by (_time);

-- `result_prefix` writes into prefixed fields and leaves the original field alone.
* | unpack_json from payload fields (a) result_prefix u_ | fields a, u_a | sort by (_time);

-- `keep_original_fields`: a non-empty existing value wins over the decoded one.
* | unpack_json from payload fields (a) keep_original_fields | fields a | sort by (_time);

-- `skip_empty_results`: an empty decoded value leaves the existing value in place.
* | unpack_json from payload fields (a) skip_empty_results | fields a | sort by (_time);

-- An `if` condition unpacks only the matching rows and keeps the existing value elsewhere.
* | unpack_json if (level:info) from payload fields (a) | fields a | sort by (_time);

-- A `fields` list requires parentheses, allows no name prefixes and no missing separator.
* | unpack_json from payload fields a | count(); -- { error SYNTAX_ERROR }
* | unpack_json from payload fields (a*) | count(); -- { error NOT_IMPLEMENTED }
* | unpack_json from payload fields (a obj) | count(); -- { error SYNTAX_ERROR }

-- `preserve_keys` has the same shape requirements.
* | unpack_json from payload fields (a) preserve_keys obj | count(); -- { error SYNTAX_ERROR }
* | unpack_json from payload fields (a) preserve_keys (obj z) | count(); -- { error SYNTAX_ERROR }

-- The set of columns of a ClickHouse table is fixed, so the `fields` list is mandatory.
* | unpack_json from payload | count(); -- { error NOT_IMPLEMENTED }

SET dialect = 'clickhouse';
DROP TABLE logs_05497;
