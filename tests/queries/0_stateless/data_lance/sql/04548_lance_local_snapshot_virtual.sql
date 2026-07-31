DROP TABLE IF EXISTS lance_local_snapshot_virtual;

CREATE TABLE lance_local_snapshot_virtual
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/versions.lance');

SELECT
    count(),
    uniqExact(_data_lake_snapshot_version),
    min(_data_lake_snapshot_version) = max(_data_lake_snapshot_version)
FROM lance_local_snapshot_virtual;

DROP TABLE lance_local_snapshot_virtual;
