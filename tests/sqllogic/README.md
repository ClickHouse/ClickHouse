# SQLLogic compatibility corpus

The `SQLLogic test` CI job runs ClickHouse against the canonical results of the
SQLite SQLLogicTest corpus used by SQuaLity. The job pins the upstream corpus to
the commit recorded in `exclusions/catalog.json` and compares only ClickHouse
with those canonical expected results.

Every corpus request is executed. Files under `exclusions/` list request IDs
whose current ClickHouse result is incompatible with the canonical result. An
excluded request has the form `test/path.test:position`, where `position` is the
source-line position reported by the SQLLogicTest runner. The CI check fails
when:

- a request outside the exclusion manifests fails;
- an excluded request starts passing and should be removed;
- an exclusion is duplicated, malformed, or not declared in
  `exclusions/catalog.json`;
- the run does not execute exactly 5,939,581 requests from the pinned corpus.

The HTML artifact contains the current ClickHouse pass rate, new failures,
fixed exclusions, failure categories, exclusion-group counts, and the issue and
pull-request inventory from `compatibility_status.json`.

## Run locally

Start a ClickHouse server on the default native port, clone the pinned corpus,
and run:

```bash
git clone https://github.com/gregrahn/sqllogictest ./tmp/sqllogictest
git -C ./tmp/sqllogictest checkout 634e46492bff5c206a6eb9ba934f2cd411c74bf0
python3 tests/sqllogic/runner.py complete-test \
    --input-dir ./tmp/sqllogictest/test \
    --out-dir ./tmp/sqllogic-output
```

The CI entry point also starts the server, runs the runner self-test and
statement-only stage, validates exclusions, and creates the report:

```bash
PYTHONPATH=.:./ci python3 ci/jobs/sqllogic_test.py
```

## Update exclusions

Download the `complete-clickhouse` `report.json` artifact or use a local report,
then regenerate the path-based manifests:

```bash
python3 tests/sqllogic/generate_known_failures.py \
    ./tmp/sqllogic-output/complete-test-stages/complete-clickhouse/report.json \
    --output-dir tests/sqllogic/exclusions
```

Review every newly added request. Move a group into a more specific manifest
and add its tracking issue or pull request to `compatibility_status.json` when
the root cause is known. Update `verified_at` whenever those public states are
reviewed.
