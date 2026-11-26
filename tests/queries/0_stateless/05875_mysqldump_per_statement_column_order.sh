#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --multiquery <<'SQL'
SET max_block_size=1, input_format_mysql_dump_table_name='src',
    input_format_mysql_dump_map_column_names=1, input_format_skip_unknown_fields=0,
    input_format_defaults_for_omitted_fields=0, input_format_null_as_default=0,
    input_format_allow_errors_num=0, input_format_allow_errors_ratio=0,
    input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1,
    schema_inference_hints='', input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576,
    optimize_count_from_files=1, use_cache_for_count_from_files=0, engine_file_truncate_on_insert=1;

-- AC-01: the issue's canonical and reordered inputs infer the same named types.
DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (2,2.5);
$$);

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);
$$);

-- AC-02, AC-05: native `INSERT FORMAT` keeps defaults with the current mapping.
CREATE TABLE mysql_dump_per_statement (b Int64,a Int64,c Int64 DEFAULT a+b) ENGINE=Memory;
SET input_format_defaults_for_omitted_fields=1;
INSERT INTO mysql_dump_per_statement FORMAT MySQLDump
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);


SELECT a,b,c FROM mysql_dump_per_statement ORDER BY a;
TRUNCATE TABLE mysql_dump_per_statement;
SET input_format_defaults_for_omitted_fields=0;
INSERT INTO mysql_dump_per_statement FORMAT MySQLDump
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);


SELECT a,b,c FROM mysql_dump_per_statement ORDER BY a;
TRUNCATE TABLE mysql_dump_per_statement;
SET input_format_defaults_for_omitted_fields=1, input_format_null_as_default=1;
INSERT INTO mysql_dump_per_statement FORMAT MySQLDump
INSERT INTO src (a,b) VALUES (NULL,10);
INSERT INTO src (b,a) VALUES (20,2);


SELECT a,b,c FROM mysql_dump_per_statement ORDER BY a;
DROP TABLE mysql_dump_per_statement;
SET input_format_defaults_for_omitted_fields=0, input_format_null_as_default=0;

-- AC-03, AC-06, AC-22: a three-column cycle and restoration expose inverse permutations.
-- Multiple rows cross blocks; string and numeric fields end in spaces, tabs and newlines.
DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b,c) VALUES (1,1.5,'x' ),(2,2.5,'y'
	 );
INSERT INTO src (b,c,a) VALUES (3.5,'z',3	 ),(4.5,'u',4 );
INSERT INTO src (a,b,c) VALUES (5,5.5,'v'),(6,6.5,'w');
$$);

SELECT a,b,c FROM format(MySQLDump, 'a Int64,b Float64,c String', $$
INSERT INTO src (a,b,c) VALUES (1,1.5,'x' ),(2,2.5,'y'
	 );
INSERT INTO src (b,c,a) VALUES (3.5,'z',3	 ),(4.5,'u',4 );
INSERT INTO src (a,b,c) VALUES (5,5.5,'v'),(6,6.5,'w');
$$) ORDER BY a;

-- AC-04: `CREATE` supplies the schema even when later data changes the column set.
DESCRIBE TABLE format(MySQLDump, $$
CREATE TABLE src (b double,a int);
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,c) VALUES (2,2.5);
$$);

SELECT a,b FROM format(MySQLDump, 'a Int64,b Float64', $$
CREATE TABLE src (b double,a int);
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);
$$) ORDER BY a;

-- AC-13: subsequent headers normalize names, but exact target matches take precedence.
SELECT id,b FROM format(MySQLDump, 'id Int64,b Int64', $$
CREATE TABLE src (id int,b int);
INSERT INTO src (id,b) VALUES (1,10);
INSERT INTO src (B,ID) VALUES (20,2);
$$) ORDER BY id;

SELECT ID,B FROM format(MySQLDump, 'ID Int64,B Int64', $$
CREATE TABLE src (id int,b int);
INSERT INTO src (B,ID) VALUES (10,1);
INSERT INTO src (ID,B) VALUES (2,20);
$$) ORDER BY ID;

SELECT ID,B FROM format(MySQLDump, 'ID Int64,B Int64', $$
CREATE TABLE src (id int,b int);
INSERT INTO src (B,ID) VALUES (10,1);
INSERT INTO src (id,b) VALUES (2,20);
$$); -- { serverError INCORRECT_DATA }

SELECT `a"b`,b FROM format(MySQLDump, '`a"b` Int64,b Int64', $$
CREATE TABLE src ("a""b" int,b int);
INSERT INTO src (b,"a""b") VALUES (10,1);
INSERT INTO src ("a""b",b) VALUES (2,20);
$$) ORDER BY 1;

-- AC-14: reordered unknown fields are skipped, including repeated unknown names.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$
INSERT INTO src (a,x,x,b) VALUES (1,90,91,10);
INSERT INTO src (x,b,x,a) VALUES (92,20,93,2);
$$) ORDER BY a SETTINGS input_format_skip_unknown_fields=1;

-- Same width and distinct names, different multiplicities: the source set changed.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$
INSERT INTO src (a,x,x,y,b) VALUES (1,90,91,92,10);
INSERT INTO src (a,x,y,y,b) VALUES (2,93,94,95,20);
$$) SETTINGS input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

-- AC-10, AC-11, AC-12: deletion on read, addition on inference, rename on count.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a) VALUES (2);
$$); -- { serverError INCORRECT_DATA }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b,c) VALUES (2,2.5,9);
$$) SETTINGS input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES (2,20);$$;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64'); -- { serverError INCORRECT_DATA }

-- AC-17: reject both directions between named and positional list modes.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES (2,20);
$$); -- { serverError INCORRECT_DATA }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (2,2.5);
$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Empty and absent lists share positional semantics, including a return to the first form.
SELECT * FROM format(MySQLDump, $$
INSERT INTO src VALUES (1,'x'
	 );
INSERT INTO src () VALUES (2,'y' );
INSERT INTO src VALUES (3,'z');
$$) ORDER BY 1;

-- AC-12: disabled name mapping accepts reorderings and list mode changes on read.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);
INSERT INTO src VALUES (3,30);
INSERT INTO src () VALUES (4,40);
INSERT INTO src (a,b) VALUES (5,50);
$$) ORDER BY a SETTINGS input_format_mysql_dump_map_column_names=0;

-- AC-21, AC-22: positional inference merges heterogeneous types at their input positions.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$)
SETTINGS input_format_mysql_dump_map_column_names=0;

SELECT a,b FROM format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$) ORDER BY a
SETTINGS input_format_mysql_dump_map_column_names=0;

SELECT a,b FROM format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$) ORDER BY a;

-- AC-08, AC-16: short and long rows, in both the first and subsequent statements.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1);$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5,9);$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5);$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2,9);$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10,100);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2,100);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Duplicate names must fail both initial inference and a later target mapping.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,a) VALUES (1,1.5);$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,a) VALUES (2,20);$$); -- { serverError INCORRECT_DATA }

-- A missing first row cannot be hidden by a later valid statement or the sampling limit.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- A header truncated at EOF still undergoes structural checks, including on count.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES$$); -- { serverError INCORRECT_DATA }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES$$;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64'); -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES$$);

-- AC-19: only headers within the row or byte sampling limits are parsed.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=1;

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=2; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);$$)
SETTINGS input_format_max_bytes_to_read_for_schema_inference=1;

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);$$)
SETTINGS input_format_max_bytes_to_read_for_schema_inference=1024; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT a,b FROM format(MySQLDump, 'a Float64,b Float64', $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- AC-06: actual reading continues to remap after inference stops sampling.
SELECT a,b FROM format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);$$) ORDER BY a
SETTINGS input_format_max_rows_to_read_for_schema_inference=1;

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);$$) SETTINGS schema_inference_hints='a Int32';

-- AC-07, AC-18: mixed statements, comma whitespace, trailing whitespace, and block boundaries.
INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES (1,10) , (2,20);
INSERT INTO other (`a b`) VALUES ('x;y');
INSERT INTO src (b,a) VALUES (30,3)
, (40,4);
REPLACE INTO src (a,b) VALUES (5,50);
REPLACE INTO other (`a b`) VALUES ('x;y');
$$ || '\t\n';

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64');
SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a;

-- EOF without a semicolon must count the last row once.
INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES (1,10)$$;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64');

-- AC-20: error allowances do not swallow structure exceptions in ordinary reading.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES (2,20);$$)
SETTINGS input_format_allow_errors_num=10; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES (2,20);$$)
SETTINGS input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

-- AC-09: representative empty dump, declaration-only dump, and absent target table.
SELECT a FROM format(MySQLDump, 'a Int64', $$$$); -- { serverError EMPTY_DATA_PASSED }
DESCRIBE TABLE format(MySQLDump, $$CREATE TABLE src (a int);$$); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT a FROM format(MySQLDump, 'a Int64', $$INSERT INTO other (a) VALUES (1);$$); -- { serverError EMPTY_DATA_PASSED }
SQL

FILE="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sql"
trap 'rm -f -- "$FILE"' EXIT
cat > "$FILE" <<'SQL'
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);
SQL
# Keep the file older than the cache entry so modification-time checks allow reuse.
touch -d '2000-01-01 00:00:00' "$FILE"

${CLICKHOUSE_CLIENT} --multiquery --query "
SET input_format_mysql_dump_table_name='src', input_format_mysql_dump_map_column_names=1,
    input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1,
    schema_inference_hints='', schema_inference_use_cache_for_file=1,
    input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576;
DESCRIBE TABLE file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump);
SET input_format_mysql_dump_map_column_names=0;
DESCRIBE TABLE file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump);
SELECT a,b FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump) ORDER BY a;
SET input_format_mysql_dump_map_column_names=1;
SELECT a,b FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}.sql', MySQLDump) ORDER BY a;
"
