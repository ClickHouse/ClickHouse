-- An empty or missing CSV/TSV field read as the default value must get the first Enum element, not 0.

SELECT 'CSV';
SELECT id, e FROM format(CSV, 'id UInt8, e Enum8(\'x\' = 1, \'y\' = 2), s String', '1,,a\n2,\n3\n4,y,c\n') ORDER BY id SETTINGS input_format_csv_allow_variable_number_of_columns = 1;
SELECT id, e FROM format(CSV, 'id UInt8, e Enum8(\'x\' = 1, \'y\' = 2)', '1,z\n2,y\n') ORDER BY id SETTINGS input_format_csv_use_default_on_bad_values = 1;

SELECT 'TSV';
SELECT id, e FROM format(TSV, 'id UInt8, e Enum8(\'x\' = 1, \'y\' = 2), s String', '1\t\ta\n2\t\n3\n4\ty\tc\n') ORDER BY id SETTINGS input_format_tsv_empty_as_default = 1, input_format_tsv_allow_variable_number_of_columns = 1;

SELECT 'WithNames';
SELECT id, e FROM format(CSVWithNames, 'id UInt8, e Enum8(\'x\' = 1, \'y\' = 2)', 'id\n1\n');
SELECT id, e FROM format(TSVWithNames, 'id UInt8, e Enum8(\'x\' = 1, \'y\' = 2)', 'id\n1\n');

SELECT 'MergeTree';
DROP TABLE IF EXISTS t_enum_empty;
CREATE TABLE t_enum_empty (id UInt8, e Enum16('x' = 300, 'y' = 301), s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_enum_empty FORMAT CSV 1,,a

SELECT * FROM t_enum_empty;
DROP TABLE t_enum_empty;
