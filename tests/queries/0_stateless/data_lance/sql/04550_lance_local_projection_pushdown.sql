DROP TABLE IF EXISTS lance_local_projection_pushdown;

SET session_timezone = 'UTC';

CREATE TABLE lance_local_projection_pushdown
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

SELECT id FROM lance_local_projection_pushdown ORDER BY id LIMIT 3;
SELECT name, id FROM lance_local_projection_pushdown WHERE id IN (2, 3) ORDER BY id;
SELECT count(), sum(id), count(score), min(event_date), max(event_time) FROM lance_local_projection_pushdown;
SELECT count(), countIf(_path != '') FROM lance_local_projection_pushdown;
SELECT count(), uniqExact(_data_lake_snapshot_version), min(_data_lake_snapshot_version) = max(_data_lake_snapshot_version) FROM lance_local_projection_pushdown;
SELECT id, _path != '' FROM lance_local_projection_pushdown ORDER BY id LIMIT 2;
SELECT _data_lake_snapshot_version = _data_lake_snapshot_version FROM lance_local_projection_pushdown LIMIT 3;
SELECT id, countIf(_path != '') FROM lance_local_projection_pushdown WHERE id IN (1, 2) GROUP BY id ORDER BY id;
SELECT id FROM lance_local_projection_pushdown WHERE lower(name) = 'x' ORDER BY id;

DROP TABLE lance_local_projection_pushdown;
