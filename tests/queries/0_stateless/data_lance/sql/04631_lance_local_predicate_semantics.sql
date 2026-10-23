DROP TABLE IF EXISTS lance_local_predicate_semantics;
DROP TABLE IF EXISTS lance_predicate_results;

CREATE TABLE lance_local_predicate_semantics
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SET log_queries = 1;

CREATE TEMPORARY TABLE lance_predicate_results
(
    case_id UInt8,
    ids Array(Int32)
)
ENGINE = Memory;

SET lance_enable_predicate_pushdown = 1;
INSERT INTO lance_predicate_results SELECT 1, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE id >= 2 AND id <= 7;
INSERT INTO lance_predicate_results SELECT 2, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE name = 'quote''d';
INSERT INTO lance_predicate_results SELECT 3, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE score < 4;
INSERT INTO lance_predicate_results SELECT 4, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE event_date >= toDate('2024-01-02');
INSERT INTO lance_predicate_results SELECT 5, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE id IN (1, 3, CAST(NULL, 'Nullable(Int32)'));
INSERT INTO lance_predicate_results SELECT 6, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE id = 2 AND lower(name) = 'b';
INSERT INTO lance_predicate_results SELECT 7, arraySort(groupArray(id)) FROM lance_local_predicate_semantics WHERE id = 1 OR lower(name) = 'x';

SET lance_enable_predicate_pushdown = 0;
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 1) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE id >= 2 AND id <= 7 FORMAT Null SETTINGS log_comment = '04631_lance_predicate_disabled';
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 2) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE name = 'quote''d' FORMAT Null;
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 3) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE score < 4 FORMAT Null;
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 4) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE event_date >= toDate('2024-01-02') FORMAT Null;
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 5) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE id IN (1, 3, CAST(NULL, 'Nullable(Int32)')) FORMAT Null;
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 6) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE id = 2 AND lower(name) = 'b' FORMAT Null;
SELECT throwIf((SELECT ids FROM lance_predicate_results WHERE case_id = 7) != arraySort(groupArray(id))) FROM lance_local_predicate_semantics WHERE id = 1 OR lower(name) = 'x' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT throwIf(ProfileEvents['LancePredicatePushdownDisabled'] = 0)
FROM system.query_log
WHERE type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment = '04631_lance_predicate_disabled'
ORDER BY event_time_microseconds DESC
LIMIT 1
FORMAT Null;

DROP TABLE lance_local_predicate_semantics;
