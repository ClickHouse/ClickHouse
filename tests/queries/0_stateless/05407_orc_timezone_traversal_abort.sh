#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A malformed ORC timezone name used to reach an empty TZif file outside TZDIR and abort
# inside libc++. Keep each attempt in a child clickhouse-local process and disable core dumps.
ulimit -c 0

WORK_DIR="${CLICKHOUSE_TMP}/orc_timezone_traversal_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

regression_status=0
python3 - "${WORK_DIR}" <<'PY' || regression_status=$?
import os
import pathlib
import shlex
import shutil
import subprocess
import sys

work_dir = pathlib.Path(sys.argv[1])
tz_dir = work_dir / "zoneinfo" / "a" / "b"
tz_dir.mkdir(parents=True)

# Use real, minimal zone files so the valid setup and the ORC fixture can be loaded while
# TZDIR points at this private directory. The traversal resolves to this test's empty file.
system_zoneinfo = pathlib.Path("/usr/share/zoneinfo")
for zone in ("UTC", "America/Vancouver"):
    source = system_zoneinfo / zone
    if not source.is_file():
        raise SystemExit(f"required time zone file not found: {source}")
    target = tz_dir / zone
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)

(work_dir / "outside").write_bytes(b"")
if (tz_dir / "../../../outside").resolve() != (work_dir / "outside").resolve():
    raise SystemExit("private timezone traversal fixture is misconfigured")
(work_dir / "dev").mkdir()
(work_dir / "dev" / "null").write_bytes(b"")
if (tz_dir / "../../../dev/null").resolve() != (work_dir / "dev" / "null").resolve():
    raise SystemExit("private ORC metadata traversal fixture is misconfigured")

env = os.environ.copy()
env["TZDIR"] = str(tz_dir)
env["TZ"] = "UTC"
local_command = shlex.split(env["CLICKHOUSE_LOCAL"])


def run_local(path, *args, query=None, input_data=None):
    path.mkdir(parents=True, exist_ok=True)
    command = local_command + ["--path", str(path), *args]
    if query is not None:
        command += ["--query", query]
    return subprocess.run(
        command,
        input=input_data,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


safe_query = (
    "SELECT toDateTime(0, 'America/Vancouver') AS t FORMAT ORC "
    "SETTINGS output_format_orc_writer_time_zone_name='America/Vancouver', "
    "output_format_orc_compression_method='none'"
)
fixture = run_local(work_dir / "safe", query=safe_query)
if fixture.returncode != 0 or not fixture.stdout.startswith(b"ORC"):
    diagnostic = (fixture.stderr + fixture.stdout).decode(errors="replace")
    raise SystemExit(f"could not create valid ORC fixture: {diagnostic[:1000]}")

# Preserve the ORC file's byte lengths while replacing its writer timezone metadata
# with the path from the customer reproducer. This exercises a file supplied by a
# reader, without requiring a malicious query setting.
safe_name = b"America/Vancouver"
unsafe_name = b"../../../dev/null"
if len(safe_name) != len(unsafe_name) or fixture.stdout.count(safe_name) != 1:
    raise SystemExit("valid ORC fixture has unexpected timezone metadata")
malicious_file = fixture.stdout.replace(safe_name, unsafe_name, 1)

malicious_writer_query = (
    "SELECT toDateTime(0, 'America/Vancouver') AS t FORMAT ORC "
    "SETTINGS output_format_orc_writer_time_zone_name='../../../outside', "
    "output_format_orc_compression_method='none'"
)
writer = run_local(work_dir / "writer", query=malicious_writer_query)
reader = run_local(
    work_dir / "reader",
    "--input-format=ORC",
    "--input_format_orc_reader_time_zone_name=../../../outside",
    query="SELECT * FROM table",
    input_data=fixture.stdout,
)
reader_file = run_local(
    work_dir / "reader_file",
    "--input-format=ORC",
    query="SELECT * FROM table",
    input_data=malicious_file,
)

failed = False
for label, result in (("writer", writer), ("reader", reader), ("reader file", reader_file)):
    if result.returncode < 0:
        print(f"{label}: terminated by signal {-result.returncode}")
        failed = True
    elif result.returncode == 0:
        print(f"{label}: malformed timezone unexpectedly succeeded")
        failed = True
    elif b"Invalid time zone name" in result.stdout + result.stderr:
        print(f"{label}: Invalid time zone name")
    else:
        diagnostic = (result.stderr + result.stdout).decode(errors="replace")
        first_line = next((line for line in diagnostic.splitlines() if line), "no diagnostic")
        print(f"{label}: unexpected error: {first_line[:300]}")
        failed = True

if failed:
    raise SystemExit(1)
PY

echo '--- stateless server is still alive ---'
${CLICKHOUSE_CLIENT} --query "SELECT 1"
exit "${regression_status}"
