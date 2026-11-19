-- `=` and `!=` read a string as an `Enum` value under `input_format_tsv_enum_as_number`, the `IN` set made from them must too.
-- each chain runs with the set rewrite (short threshold) and without it (threshold 100), the results must be equal.

DROP TABLE IF EXISTS t_enum_set_rewrite;
CREATE TABLE t_enum_set_rewrite (e Enum8('z' = 1, 'a' = 2, '1' = 3, '2' = 4), n Nullable(Enum8('z' = 1, 'a' = 2, '1' = 3, '2' = 4))) ENGINE = Memory;
INSERT INTO t_enum_set_rewrite VALUES ('z', 'z'), ('a', 'a'), ('1', '1'), ('2', '2');

SET input_format_tsv_enum_as_number = 1;

SELECT 'not equals with less', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e < '2' AND e != '2' AND e != '3' AND e != '4' SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT 'not equals with less, no rewrite', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e < '2' AND e != '2' AND e != '3' AND e != '4' SETTINGS optimize_min_inequality_conjunction_chain_length = 100;
SELECT 'not equals', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e != '1' AND e != '2' AND e != '3' SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT 'not equals, no rewrite', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e != '1' AND e != '2' AND e != '3' SETTINGS optimize_min_inequality_conjunction_chain_length = 100;
SELECT 'equals', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = '1' OR e = '2' OR e = '3' SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT 'equals, no rewrite', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = '1' OR e = '2' OR e = '3' SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'equals Nullable', arraySort(groupArray(n)) FROM t_enum_set_rewrite WHERE n = '1' OR n = '2' OR n = '3' SETTINGS optimize_min_equality_disjunction_chain_length = 3;
SELECT 'equals Nullable, no rewrite', arraySort(groupArray(n)) FROM t_enum_set_rewrite WHERE n = '1' OR n = '2' OR n = '3' SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'equals LowCardinality', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = toLowCardinality('1') OR e = toLowCardinality('2') SETTINGS optimize_min_equality_disjunction_chain_length = 2;
SELECT 'equals LowCardinality, no rewrite', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = toLowCardinality('1') OR e = toLowCardinality('2') SETTINGS optimize_min_equality_disjunction_chain_length = 100;
SELECT 'equals FixedString', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = toFixedString('1', 1) OR e = toFixedString('2', 1) SETTINGS optimize_min_equality_disjunction_chain_length = 2;
SELECT 'equals FixedString, no rewrite', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = toFixedString('1', 1) OR e = toFixedString('2', 1) SETTINGS optimize_min_equality_disjunction_chain_length = 100;

-- without the setting a string is an `Enum` name for both.
SET input_format_tsv_enum_as_number = 0;

SELECT 'setting off, not equals', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e != '1' AND e != '2' AND e != '3' SETTINGS optimize_min_inequality_conjunction_chain_length = 3;
SELECT 'setting off, equals', arraySort(groupArray(e)) FROM t_enum_set_rewrite WHERE e = '1' OR e = '2' OR e = '3' SETTINGS optimize_min_equality_disjunction_chain_length = 3;

DROP TABLE t_enum_set_rewrite;
