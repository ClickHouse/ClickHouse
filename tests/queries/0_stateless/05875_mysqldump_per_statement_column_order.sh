#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --multiquery <<'SQL'

-- Per-statement mapping through the native client, including omitted defaults.

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

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10),(2,20);
INSERT INTO src (b,a) VALUES (30,3),(40,4);
INSERT INTO src (a,b) VALUES (5,50),(6,60);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

-- `create_order` and `create_without_lists`.

DESCRIBE TABLE format(MySQLDump, $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src  VALUES (10,1);
INSERT INTO src  VALUES (20,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src () VALUES (10,1);
INSERT INTO src  VALUES (20,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src  VALUES (10,1);
INSERT INTO src () VALUES (20,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

-- `unknown_multiset` and `unknown_duplicate_counts`.

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,x,b) VALUES (1,90,10);
INSERT INTO src (b,a,x) VALUES (20,2,91);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_skip_unknown_fields=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,x,x,b) VALUES (1,90,91,10);
INSERT INTO src (x,b,x,a) VALUES (92,20,93,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_skip_unknown_fields=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,x,b) VALUES (1,90,10);
INSERT INTO src (a,y,b) VALUES (2,91,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,x,x,b) VALUES (1,90,91,10);
INSERT INTO src (a,x,b) VALUES (2,91,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,x,b) VALUES (1,90,10);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_skip_unknown_fields=0; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);
INSERT INTO src (a,x,b) VALUES (3,91,30);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_skip_unknown_fields=0; -- { serverError INCORRECT_DATA }

-- `list_mode_transitions` and positional reading.

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src  VALUES (1,10);
INSERT INTO src () VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src () VALUES (1,10);
INSERT INTO src  VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src  VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src  VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src  VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_mysql_dump_map_column_names=0;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src () VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src () VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src () VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_mysql_dump_map_column_names=0;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src  VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src  VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src  VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_mysql_dump_map_column_names=0;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src () VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src () VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src () VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_mysql_dump_map_column_names=0;

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES (20,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_mysql_dump_map_column_names=0;

-- `changed_set` and duplicate target columns.

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a) VALUES (2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a) VALUES (2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,b,c) VALUES (2,20,200);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,b,c) VALUES (2,20,200);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,a) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,a) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,a) VALUES (1,10);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

-- `normalized_names`: reuse `CREATE` spelling, preserving exact header matches.

SELECT id,b FROM format(MySQLDump, 'id Int64,b Int64', $$
CREATE TABLE src (id int,b int);
INSERT INTO src (B,ID) VALUES (10,1);
INSERT INTO src (id,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT ID,B FROM format(MySQLDump, 'ID Int64,B Int64', $$
CREATE TABLE src (id int,b int);
INSERT INTO src (B,ID) VALUES (10,1);
INSERT INTO src (ID,B) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

SELECT ID,B FROM format(MySQLDump, 'ID Int64,B Int64', $$
CREATE TABLE src (id int,b int);
INSERT INTO src (B,ID) VALUES (10,1);
INSERT INTO src (id,b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

-- `inference_order`: types are merged by source name.

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5),(2,2.5);
INSERT INTO src (b,a) VALUES (3.5,3),(4.5,4);
INSERT INTO src (a,b) VALUES (5,5.5),(6,6.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

-- `row_width`: reject both widths before accessing the permutation.

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (1);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5,9);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (1,1.5,9);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- `inference_structure`, independent of the data mapping setting.

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a) VALUES (2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a) VALUES (2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b,c) VALUES (2,2.5,9);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,b,c) VALUES (2,2.5,9);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,c) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,c) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src  VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src  VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src () VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src () VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (A,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (A,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_map_column_names=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src  VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src  VALUES (1,1.5);
INSERT INTO src () VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src () VALUES (1,1.5);
INSERT INTO src (a,b) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src () VALUES (1,1.5);
INSERT INTO src  VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,a) VALUES (1,1.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- `sampling_header` and `sampling_bytes`: do not parse beyond the last sampled row.

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_max_rows_to_read_for_schema_inference=1;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_max_rows_to_read_for_schema_inference=2; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_max_rows_to_read_for_schema_inference=100, input_format_max_bytes_to_read_for_schema_inference=1;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_max_rows_to_read_for_schema_inference=100, input_format_max_bytes_to_read_for_schema_inference=1024; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- `inferred_then_read`: the mapping survives a one-row inference sample.

SELECT a,b FROM format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2);$$) ORDER BY a SETTINGS input_format_max_rows_to_read_for_schema_inference=1, max_block_size=1, input_format_try_infer_integers=1;

-- `create_shortcut`: declaration discovery does not validate later statements.

DESCRIBE TABLE format(MySQLDump, $$
CREATE TABLE src (b double,a int);
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,c) VALUES (2,2.5);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1;

SELECT a,b FROM format(MySQLDump, 'a Int64,b Float64', $$
CREATE TABLE src (b double,a int);
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (a,c) VALUES (2,2.5);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError INCORRECT_DATA }

-- `syntax_and_duplicates` and `empty_data`: preserve existing error codes.

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,b) VALUES (1);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10,100);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,b) VALUES (1,10,100);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Float64,b Float64', $$
INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a VALUES (2.5,2);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$

$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError EMPTY_DATA_PASSED }

DESCRIBE TABLE format(MySQLDump, $$

$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
CREATE TABLE src (a int);
$$) ORDER BY 1,2 SETTINGS max_block_size=1; -- { serverError EMPTY_DATA_PASSED }

DESCRIBE TABLE format(MySQLDump, $$
CREATE TABLE src (a int);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT a,b FROM format(MySQLDump, 'a Int64, b Int64', $$
INSERT INTO other (a,b) VALUES (1,10);
$$) ORDER BY 1,2 SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src'; -- { serverError EMPTY_DATA_PASSED }

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO other (a,b) VALUES (1,10);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1, input_format_mysql_dump_table_name='src'; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

SELECT `a"b`,b FROM format(MySQLDump, '`a"b` Int64,b Int64', $$
CREATE TABLE src ("a""b" int,b int);
INSERT INTO src (b,"a""b") VALUES (10,1);
INSERT INTO src ("a""b",b) VALUES (2,20);
$$) ORDER BY 1,2 SETTINGS max_block_size=1;

-- Unknown fields retain their multiplicity while omitted defaults follow the current mapping.

CREATE TABLE mysql_dump_per_statement (b Int64,a Int64,c Int64 DEFAULT a+b) ENGINE=Memory;

SET input_format_defaults_for_omitted_fields=1, input_format_skip_unknown_fields=1;

INSERT INTO mysql_dump_per_statement FORMAT MySQLDump
INSERT INTO src (a,x,b) VALUES (1,90,10);
INSERT INTO src (b,a,x) VALUES (20,2,91);


SELECT a,b,c FROM mysql_dump_per_statement ORDER BY a;

DROP TABLE mysql_dump_per_statement;

SET input_format_defaults_for_omitted_fields=0;

-- `count_tail`: skip other tables and do not count their values or trailing whitespace.

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10),(2,20);
INSERT INTO other (`a b`) VALUES ('x;y');
INSERT INTO src (b,a) VALUES (30,3),(40,4);
REPLACE INTO src (a,b) VALUES (5,50);
REPLACE INTO other (`a b`) VALUES ('x;y');
$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

-- `count_terminators`.

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10)$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO other (`a b`) VALUES ('x;y');$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

-- `count_row_delimiters`: whitespace before commas, across blocks.

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10) , (2,20);
INSERT INTO src (b,a) VALUES (30,3);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10)
, (2,20);
INSERT INTO src (b,a) VALUES (30,3);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

-- `count_create_without_lists`.

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src  VALUES (10,1);
INSERT INTO src  VALUES (20,2);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src () VALUES (10,1);
INSERT INTO src  VALUES (20,2);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$CREATE TABLE src (b int NOT NULL,a int NOT NULL);
INSERT INTO src  VALUES (10,1);
INSERT INTO src () VALUES (20,2);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

-- `count_structure`: validate source structure even without deserializing values.

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a) VALUES (2);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,b,c) VALUES (2,20,200);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES (2,20);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src  VALUES (2,20);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src () VALUES (2,20);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,x,b) VALUES (1,90,10);
INSERT INTO src (b,a,y) VALUES (20,2,91);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=0, input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

-- `allow_errors`: the format cannot synchronize after a structural exception.

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES (2,20);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=10, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=10, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src  VALUES (2,20);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=10, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=10, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_per_statement.sql', RawBLOB) SELECT $$INSERT INTO src  VALUES (1,10);
INSERT INTO src (a,b) VALUES (2,20);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=10, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=10, input_format_allow_errors_ratio=0; -- { serverError INCORRECT_DATA }

SELECT count() FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM file(currentDatabase() || '_mysqldump_per_statement.sql', MySQLDump, 'a Int64,b Int64') ORDER BY a SETTINGS max_block_size=1, input_format_mysql_dump_table_name='src', optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_allow_errors_num=0, input_format_allow_errors_ratio=1; -- { serverError INCORRECT_DATA }

SET input_format_mysql_dump_map_column_names=1, input_format_mysql_dump_table_name='src',
    input_format_allow_errors_num=0, input_format_allow_errors_ratio=0;

-- A three-column cycle distinguishes a permutation from its inverse.
DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b,c) VALUES (1,1.5,'x');
INSERT INTO src (a,b,c) VALUES (2,2.5,'y');
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1,
    input_format_max_rows_to_read_for_schema_inference=10, input_format_max_bytes_to_read_for_schema_inference=1048576;

DESCRIBE TABLE format(MySQLDump, $$
INSERT INTO src (a,b,c) VALUES (1,1.5,'x');
INSERT INTO src (b,c,a) VALUES (2.5,'y',2);
$$) SETTINGS input_format_try_infer_integers=1, schema_inference_make_columns_nullable=1,
    input_format_max_rows_to_read_for_schema_inference=10, input_format_max_bytes_to_read_for_schema_inference=1048576;

SELECT a,b,c FROM format(MySQLDump, 'a Int64,b Float64,c String', $$
INSERT INTO src (a,b,c) VALUES (1,1.5,'x');
INSERT INTO src (b,c,a) VALUES (2.5,'y',2);
$$) ORDER BY a SETTINGS max_block_size=1;

-- The field count and distinct names stay the same, but the unknown-name multiplicities change.
INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_column_multiplicity.sql', RawBLOB)
SELECT $$INSERT INTO src (a,x,x,y,b) VALUES (1,90,91,92,10);
INSERT INTO src (a,x,y,y,b) VALUES (2,93,94,95,20);$$
SETTINGS engine_file_truncate_on_insert=1;

SELECT a,b FROM file(currentDatabase() || '_mysqldump_column_multiplicity.sql', MySQLDump, 'a Int64,b Int64')
ORDER BY a SETTINGS max_block_size=1, input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

SELECT count() FROM file(currentDatabase() || '_mysqldump_column_multiplicity.sql', MySQLDump, 'a Int64,b Int64')
SETTINGS max_block_size=1, input_format_skip_unknown_fields=1, optimize_count_from_files=1,
    use_cache_for_count_from_files=0; -- { serverError INCORRECT_DATA }

SET input_format_mysql_dump_map_column_names=1, input_format_mysql_dump_table_name='src', max_block_size=1,
    input_format_allow_errors_num=0, input_format_allow_errors_ratio=0, input_format_skip_unknown_fields=0,
    input_format_defaults_for_omitted_fields=0, schema_inference_hints='';

-- Validate a parsed `INSERT` header even when there is no row after `VALUES`.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES$$) SETTINGS input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES$$); -- { serverError INCORRECT_DATA }

-- Without `CREATE TABLE`, schema inference must validate the next header too.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=100; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=100; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Exercise the `file` counting fast path with separate files and no cached counts.
INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_truncated_column_set.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_truncated_column_set.sql', MySQLDump, 'a Int64,b Int64')
SETTINGS optimize_count_from_files=1, use_cache_for_count_from_files=0, input_format_skip_unknown_fields=1; -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_truncated_list_mode.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src VALUES$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_truncated_list_mode.sql', MySQLDump, 'a Int64,b Int64')
SETTINGS optimize_count_from_files=1, use_cache_for_count_from_files=0; -- { serverError INCORRECT_DATA }

-- A valid reordered header at EOF still allows reading the preceding row.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES$$);

-- Positional reading still accepts a different column set at EOF.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES$$) SETTINGS input_format_mysql_dump_map_column_names=0;

-- Schema inference must not inspect headers beyond its row sampling limit.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (a,c) VALUES$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=1, input_format_try_infer_integers=1,
    schema_inference_make_columns_nullable=1;

-- The first `INSERT` must have a value row before its semicolon.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=100; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- A one-row sampling limit must not hide the malformed first statement.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$)
SETTINGS input_format_max_rows_to_read_for_schema_inference=1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- Positional mapping must not bypass the row-start check.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$)
SETTINGS input_format_mysql_dump_map_column_names=0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- The counting fast path must reject the same input without a cached count.
INSERT INTO FUNCTION file(currentDatabase() || '_mysqldump_missing_first_values.sql', RawBLOB)
SELECT $$INSERT INTO src (a,b) VALUES;
INSERT INTO src (b,a) VALUES (20,2);$$ SETTINGS engine_file_truncate_on_insert=1;

SELECT count() FROM file(currentDatabase() || '_mysqldump_missing_first_values.sql', MySQLDump, 'a Int64,b Int64')
SETTINGS optimize_count_from_files=1, use_cache_for_count_from_files=0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- With no following statement, the missing row must still be rejected.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES;$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- A later statement missing its row is already rejected.
SELECT a,b FROM format(MySQLDump, 'a Int64,b Int64', $$INSERT INTO src (a,b) VALUES (1,10);
INSERT INTO src (b,a) VALUES;$$); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Inference uses input positions when column name mapping is disabled.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$)
SETTINGS input_format_mysql_dump_map_column_names=0, input_format_try_infer_integers=1,
    schema_inference_make_columns_nullable=1, schema_inference_hints='',
    input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576;

-- The inferred positional schema must deserialize the same input successfully.
SELECT a,b FROM format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$) ORDER BY a
SETTINGS input_format_mysql_dump_map_column_names=0, input_format_try_infer_integers=1,
    schema_inference_make_columns_nullable=1, schema_inference_hints='',
    input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576;

-- Mapping by name must retain the original column assignments.
SELECT a,b FROM format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$) ORDER BY a
SETTINGS input_format_mysql_dump_map_column_names=1, input_format_try_infer_integers=1,
    schema_inference_make_columns_nullable=1, schema_inference_hints='',
    input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576;

-- A schema hint stays bound to its column across reordered statements.
DESCRIBE TABLE format(MySQLDump, $$INSERT INTO src (a,b) VALUES (1,1.5);
INSERT INTO src (b,a) VALUES (2.5,2 );$$)
SETTINGS input_format_mysql_dump_map_column_names=1, input_format_try_infer_integers=1,
    schema_inference_make_columns_nullable=1, schema_inference_hints='a Int32',
    input_format_max_rows_to_read_for_schema_inference=100,
    input_format_max_bytes_to_read_for_schema_inference=1048576;

-- Inference accepts whitespace before the row terminator, including after strings.
SELECT a,b FROM format(MySQLDump, $$
INSERT INTO src (a,b) VALUES (1,'x'
	 );
INSERT INTO src (b,a) VALUES ('y',2
	 );
$$) ORDER BY a SETTINGS input_format_try_infer_integers=1;

SELECT * FROM format(MySQLDump, $$
INSERT INTO src VALUES (1,'x'
	 ),(2,'y' );
$$) ORDER BY 1 SETTINGS input_format_try_infer_integers=1;
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
