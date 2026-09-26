DROP TABLE IF EXISTS lance_local_virtual_only_scan;

CREATE TABLE lance_local_virtual_only_scan
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/versions.lance');

SELECT count()
FROM lance_local_virtual_only_scan
SETTINGS optimize_count_from_files = 0;

SELECT
    count(),
    uniqExact(_data_lake_snapshot_version),
    min(_data_lake_snapshot_version) = max(_data_lake_snapshot_version)
FROM lance_local_virtual_only_scan
SETTINGS optimize_count_from_files = 0;

DROP TABLE lance_local_virtual_only_scan;
