# Iceberg v3 deletion vector fixture

Generated with `generate_iceberg_dv_fixture.py` (Spark + Iceberg 1.9.0).

- Warehouse: `dv_puffin_warehouse/`
- Table: `default/dv_puffin_source` with column `id BIGINT`
- 200 rows (`id` from 0 to 199)
- Deleted rows via Puffin deletion vector: 2, 5, 7, 100

Regenerate:

```bash
python3 generate_iceberg_dv_fixture.py
```

Query path for tests:

```text
data_minio/dv_puffin_warehouse/default/dv_puffin_source
```
