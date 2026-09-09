# Runtime checks for the Windows port that no other job can make.
#
# The cross-build configures with `ENABLE_TESTS=0`, and there is no Windows runner: the only
# place a Windows binary executes in CI is the Wine step of `Build (amd_windows)`, which runs
# `clickhouse.exe`. Behaviour that `clickhouse.exe` does not reach on the way to answering
# `SELECT 1` - the emulation of per-call flags Winsock does not have, the memory limit a job
# object imposes - therefore has nowhere to be checked. This executable is that place; the same
# job runs it right after `clickhouse.exe`. See `ci/jobs/build_clickhouse.py`.
#
# It links `_poco_net` and `common` only, so it builds in seconds and stays available even
# while parts of `src/` do not compile for Windows.

add_executable (clickhouse-windows-selftest windows-selftest.cpp)
target_link_libraries (clickhouse-windows-selftest PRIVATE _poco_net common)
