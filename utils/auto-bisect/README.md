# auto-bisect

Automated ClickHouse regression bisection using CI pre-built binaries. Downloads the binary for each merge commit, sets up the test environment, and runs your test to locate the first bad commit.

## Usage

```bash
./bisect.sh --good <start_commit> --bad <end_commit> --path <path_to_clickhouse_repo> [options]
```

## Options

| Option | Description |
|--------|-------------|
| `--good <sha>` | Known good commit hash |
| `--bad <sha>` | Known bad commit hash |
| `--path <path>` | Path to ClickHouse repo (`CH_ROOT` env var used by default) |
| `--test <file>` | Test script to run per commit (default: `tests/test.sh`) |
| `--env <mode>` | Environment: `single` (default), `replicateddb`, `sharedcatalog`, `nothing` |
| `--private` | Use private CI builds (requires credentials; see below) |
| `--build-missing-binary` | Compile if no CI binary available |
| `--build-directory <path>` | Build directory for compilation fallback |
| `--walker` | Test every commit linearly instead of binary bisect |
| `--walker-steps <N>` | With `--walker`: test only N evenly spaced commits |
| `--walker-commits "<shas>"` | With `--walker`: test a specific list of commits |

## Environments

- **`single`** — single ClickHouse server on port 9000
- **`replicateddb`** — two nodes with Keeper, replicateddb database
- **`sharedcatalog`** — two nodes with Keeper, Shared Catalog
- **`nothing`** — no server setup (for client-side bisection)

## Examples

```bash
# Basic bisect on single-node setup
./bisect.sh --good a1b2c3d4 --bad e5f6g7h8 --path ~/work/ClickHouse
# Replicated setup with custom test
./bisect.sh --good a1b2c3d4 --bad e5f6g7h8 --path ~/work/ClickHouse \
  --env replicateddb --test ./tests/my_repro.sh

# Walker mode: test every commit, 20 evenly-spaced steps
./bisect.sh --good a1b2c3d4 --bad e5f6g7h8 --path ~/work/ClickHouse \
  --walker --walker-steps 20

# Private CI builds
./bisect.sh --good a1b2c3d4 --bad e5f6g7h8 --path ~/work/ClickHouse \
  --private```

## Prerequisites

The following directories must exist and be writable by the current user:

```
/etc/clickhouse-server
/etc/clickhouse-client
/var/lib/clickhouse
```

For replicateddb/sharedcatalog environments, also:

```
/etc/clickhouse-server1
/var/lib/clickhouse1
```

## Private CI Credentials

By default, binaries are downloaded from the public S3 bucket. Use `--private` for private CloudFront builds.

Set credentials via `CH_CI_USER` and `CH_CI_PASSWORD` env vars:

```bash
export CH_CI_USER="myuser"
export CH_CI_PASSWORD="mypassword"
```

The script will fail immediately if `--private` is used without these variables set.

## Writing a Test Script

The test script receives the ClickHouse work tree path as `$1` and must return:
- `0` — commit is **good** (bug not present)
- `1` — commit is **bad** (bug present)
- `125` — **skip** this commit (e.g., unrelated build failure)

The downloaded binary is available as `$CH_PATH`. Example:

```bash
#!/bin/bash
set -e
WORK_TREE="$1"

OUTPUT=$($CH_PATH client --query "SELECT ..." 2>&1 || true)

if echo "$OUTPUT" | grep -q "Code: 36"; then
  exit 1  # bad
else
  exit 0  # good
fi
```

## Binary Caching

Compiled binaries are cached in `data/file-cache/` using `helpers/cache.sh`. Downloaded CI binaries are not cached (they are re-downloaded each run). To manage the cache:

```bash
./helpers/cache.sh list
./helpers/cache.sh remove <name|id>
```
