Test tables of delta-kernel-rs, copied from the `kernel/tests/data` directory of
https://github.com/delta-io/delta-kernel-rs (the `contrib/delta-kernel-rs` submodule, commit `88ff5a9`),
licensed under the Apache License 2.0. Tables stored there as `.tar.zst` archives are extracted.
Hadoop checksum files (`.*.crc`) are omitted; the Delta version checksum files (`_delta_log/*.crc`) are kept.

The expected results in the tests that read these tables come from the kernel tests that use the same
tables: `kernel/tests/integration/features/cdf.rs`, `kernel/tests/integration/log/v2_checkpoints.rs`,
`kernel/tests/integration/read.rs` and `kernel/src/scan/data_skipping/tests.rs`.
`kernel/tests/README.md` of delta-kernel-rs describes the schema, the protocol and the features of each table.

Some tables (`app-txn-with-last-updated`, `with_checkpoint_no_last_checkpoint`, `partition_cm`,
`crc-full`) are fixtures of the kernel log replay tests and have a transaction log without data files.
The `parsed-stats` table has statistics of a `ts_col` column that is absent in its data files.
