# Iceberg v3 deletion vector fixtures

Generated with `generate_iceberg_dv_fixture.py` (Spark + Iceberg 1.9.0).

Warehouse: `dv_puffin_warehouse/`

## `default/dv_puffin_source`

Simple table with column `id BIGINT`:

- 200 rows (`id` from 0 to 199)
- Deleted rows via Puffin deletion vector: 2, 5, 7, 100

## `default/dv_puffin_complex`

Table with columns `id BIGINT`, `data STRING`, `label STRING` and multiple snapshots:

- Initial insert `id` 10-99, then deletes `id < 20` and `id >= 90`
- Insert `id` 100-199, delete `id >= 150`
- Schema evolution (`label` column), insert `id` 200-249 with `label = 'new'`
- Delete `id` in (205, 210, 220), update `id = 25` to `label = 'updated'`
- Spark `rewrite_data_files` compaction

Regenerate:

```bash
python3 generate_iceberg_dv_fixture.py
```

Query paths for tests:

```text
data_minio/dv_puffin_warehouse/default/dv_puffin_source
data_minio/dv_puffin_warehouse/default/dv_puffin_complex
```
