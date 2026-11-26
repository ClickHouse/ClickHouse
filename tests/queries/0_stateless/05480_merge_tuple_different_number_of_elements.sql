-- A Merge table reads a child whose unnamed tuple has fewer or more elements than the Merge column:
-- missing trailing elements get default values and extra ones are dropped (issue #128463).

DROP TABLE IF EXISTS all_query_log;
DROP TABLE IF EXISTS all_query_log_old;
DROP TABLE IF EXISTS all_query_log_rev;
DROP TABLE IF EXISTS query_log;
DROP TABLE IF EXISTS query_log_1;

CREATE TABLE query_log_1 (event_date Date, transaction_id Tuple(UInt64, UInt64, UUID)) ENGINE = MergeTree ORDER BY event_date;
INSERT INTO query_log_1 VALUES ('2026-10-30', (1, 2, '00000000-0000-0000-0000-000000000001'));
CREATE TABLE query_log (event_date Date, transaction_id Tuple(UInt64, UInt64, UUID, Int64)) ENGINE = MergeTree ORDER BY event_date;
INSERT INTO query_log VALUES ('2026-10-31', (3, 4, '00000000-0000-0000-0000-000000000002', 5));
CREATE TABLE all_query_log (event_date Date, transaction_id Tuple(UInt64, UInt64, UUID, Int64)) AS merge(currentDatabase(), '^query_log(_[0-9]+)?$');

SELECT 'issue';
SELECT _table, * FROM all_query_log ORDER BY _table;
SELECT _table, transaction_id FROM all_query_log ORDER BY transaction_id;
SELECT _table, transaction_id.4 FROM all_query_log ORDER BY _table;
SELECT _table, tupleElement(transaction_id, 4) FROM all_query_log ORDER BY _table SETTINGS optimize_functions_to_subcolumns = 0;
SELECT _table, tupleElement(transaction_id, 4) FROM all_query_log ORDER BY _table SETTINGS optimize_functions_to_subcolumns = 1;
SELECT _table, event_date FROM all_query_log WHERE transaction_id.4 = 0 ORDER BY _table;

SELECT 'single child';
CREATE TABLE all_query_log_old (event_date Date, transaction_id Tuple(UInt64, UInt64, UUID, Int64)) ENGINE = Merge(currentDatabase(), '^query_log_1$');
SELECT _table, transaction_id, transaction_id.4 FROM all_query_log_old;

SELECT 'reverse';
CREATE TABLE all_query_log_rev (event_date Date, transaction_id Tuple(UInt64, UInt64, UUID)) ENGINE = Merge(currentDatabase(), '^query_log(_[0-9]+)?$');
SELECT _table, transaction_id, transaction_id.3 FROM all_query_log_rev ORDER BY _table;

DROP TABLE IF EXISTS nested_merge;
DROP TABLE IF EXISTS nested_old;
DROP TABLE IF EXISTS nested_new;

CREATE TABLE nested_old
(
    k UInt8,
    p Tuple(UInt32),
    a Array(Tuple(UInt8)),
    mp Map(String, Tuple(UInt8)),
    nt Tuple(UInt8, Tuple(UInt8)),
    nm Tuple(a UInt8, b Tuple(UInt8)),
    nl Nullable(Tuple(UInt8)),
    ns Tuple(UInt8)
) ENGINE = MergeTree ORDER BY k;
INSERT INTO nested_old VALUES (1, (7), [(1), (2)], {'x': (3)}, (4, (5)), (6, (7)), (8), (9)), (2, (9), [], {}, (0, (0)), (0, (0)), NULL, (0));

CREATE TABLE nested_new
(
    k UInt8,
    p Tuple(UInt64, String),
    a Array(Tuple(UInt8, String)),
    mp Map(String, Tuple(UInt8, UInt8)),
    nt Tuple(UInt8, Tuple(UInt8, UInt8)),
    nm Tuple(a UInt8, b Tuple(UInt8, UInt8)),
    nl Nullable(Tuple(UInt8, UInt8)),
    ns Nullable(Tuple(UInt8, UInt8))
) ENGINE = MergeTree ORDER BY k;
INSERT INTO nested_new VALUES (3, (10, 'z'), [(1, 'a')], {'y': (1, 2)}, (1, (2, 3)), (4, (5, 6)), (7, 8), (9, 10));

CREATE TABLE nested_merge AS nested_new ENGINE = Merge(currentDatabase(), '^nested_(old|new)$');

SELECT 'nested';
SELECT _table, * FROM nested_merge ORDER BY k;
SELECT k, p.2, a.2, mp.values, nt.2.2, nm.b.2, nl.2, nl.null, ns.2 FROM nested_merge ORDER BY k;

SELECT 'not converted';
DROP TABLE IF EXISTS neg_array_merge;
DROP TABLE IF EXISTS neg_named_merge;
DROP TABLE IF EXISTS neg_named_merge_2;
DROP TABLE IF EXISTS neg_array;
DROP TABLE IF EXISTS neg_named;

CREATE TABLE neg_array (k UInt8, t Tuple(Array(UInt8))) ENGINE = MergeTree ORDER BY k;
INSERT INTO neg_array VALUES (1, ([1]));
CREATE TABLE neg_array_merge (k UInt8, t Tuple(UInt8, UInt8)) ENGINE = Merge(currentDatabase(), '^neg_array$');
SELECT * FROM neg_array_merge; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE TABLE neg_named (k UInt8, t Tuple(`1` UInt8, x UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO neg_named VALUES (1, (1, 2));
CREATE TABLE neg_named_merge (k UInt8, t Tuple(UInt8, UInt8, UInt8)) ENGINE = Merge(currentDatabase(), '^neg_named$');
SELECT * FROM neg_named_merge; -- { serverError TYPE_MISMATCH }
CREATE TABLE neg_named_merge_2 (k UInt8, t Tuple(c UInt8, d UInt8, e UInt8)) ENGINE = Merge(currentDatabase(), '^neg_named$');
SELECT * FROM neg_named_merge_2; -- { serverError TYPE_MISMATCH }

DROP TABLE all_query_log;
DROP TABLE all_query_log_old;
DROP TABLE all_query_log_rev;
DROP TABLE query_log;
DROP TABLE query_log_1;
DROP TABLE nested_merge;
DROP TABLE nested_old;
DROP TABLE nested_new;
DROP TABLE neg_array_merge;
DROP TABLE neg_named_merge;
DROP TABLE neg_named_merge_2;
DROP TABLE neg_array;
DROP TABLE neg_named;
