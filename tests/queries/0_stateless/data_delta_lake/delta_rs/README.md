# Delta tables from the delta-rs test suite

Copied unchanged from `crates/test/tests/data/` of https://github.com/delta-io/delta-rs
at commit `460c90408c6a85af2237ef8b58aefb62d62d1801` (2026-11-03).

License: Apache License 2.0, the same as ClickHouse (https://github.com/delta-io/delta-rs/blob/main/LICENSE.txt).
Copyright (2020) QP Hou and a number of other contributors.

Used by tests `05550_delta_lake_delta_rs_legacy_writers`, `05551_delta_lake_delta_rs_partitions`,
`05552_delta_lake_delta_rs_log_only_tables` and `05553_delta_lake_delta_rs_table_features`.

The tables `checkpoints`, `checkpoints_vacuumed`, `latest_not_checkpointed` and `table-with-domain-metadata`
contain only `_delta_log`, without data files, as in delta-rs.

Tables that also exist in the delta-kernel-rs test data (`table-with-dv-small`, `with_checkpoint_no_last_checkpoint`,
the `cdf-*` tables, `v2-classic-checkpoint-json`) were not copied.
