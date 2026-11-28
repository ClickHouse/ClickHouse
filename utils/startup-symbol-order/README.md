# Startup symbol order

`programs/startup_symbol_order.txt` lists the functions that ClickHouse executes at startup, in the order of
their first execution. The `clickhouse` binary is linked with `--symbol-ordering-file` of this list (see
`ENABLE_STARTUP_SYMBOL_ORDERING` in `programs/CMakeLists.txt`), so that these functions are placed next to each
other at the beginning of the code.

Why: a cold start of the binary (the executable not in the page cache, e.g. after a reboot, on a new machine,
or with a network disk) reads it from disk by page faults. Without the ordering, the functions that run at
startup are spread over the whole 400+ MiB of code, and every one of them faults in a page and its readahead
window. For `clickhouse local --query 'SELECT 1'`, the executed functions take 14 MiB, but the startup read
200 MiB of the executable in about 1450 major page faults. With the ordering, it reads 88 MiB in about 420
faults; the server reads 190 MiB instead of 290 MiB before it accepts connections.

## Regenerating

The nightly `NightlyStartupSymbolOrder` workflow (`ci/jobs/startup_symbol_order_nightly.py`) builds the release
configuration (ARM, ThinLTO) of master, regenerates the list from it, and keeps a pull request from the
`robot/startup-symbol-order` branch open while the functions of the list differ from the committed ones.

The list goes out of date slowly: functions that are renamed or removed are ignored by the linker, and new
functions are placed with the rest of the code. The list has to come from a build with ThinLTO, like the
binaries we ship: ThinLTO renames the local functions that it promotes (the `.llvm.<hash>` suffix), and a list
from a build without it covers only about 70% of the functions that a release binary executes at startup.

To regenerate it by hand, use a build of the current source that has a symbol table (not stripped):

```
python3 utils/startup-symbol-order/generate.py build/programs/clickhouse
```

It traces two runs of the binary and merges them: `clickhouse local --query 'SELECT 1'`, and
`clickhouse server` with the packaged configuration from `programs/server` up to accepting connections, one
query over HTTP, and the shutdown. It takes a few seconds.

`first_execution.py` is the tracer: it writes a breakpoint instruction at the entry of every function of the
symbol table under `ptrace`, and records each function when its breakpoint is hit for the first time. This
gives the exact and complete order, unlike sampling. It supports AArch64 and x86-64 Linux. Functions that the
linker folded into one (`--icf`) share an address; at most four of their names are listed.
