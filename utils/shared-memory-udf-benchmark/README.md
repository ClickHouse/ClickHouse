# Executable UDF transport benchmark: pipes vs shared memory

This benchmark compares the data transports used by executable user-defined functions:

* `bench_pipe_stream`  — plain pipe transport, flush per row (the documented streaming pattern);
* `bench_pipe_chunk`   — pipe transport with `send_chunk_header`, flush once per chunk (fair per-chunk baseline);
* `bench_shm`          — shared-memory transport (`use_shared_memory`);
* `bench_shm_busy` — shared-memory transport with artificial per-request CPU work in the command.

All functions are functionally identical echoes (see [`functions.xml`](functions.xml) and [`user_scripts/`](user_scripts)), so wall-clock differences are attributable to the transport alone.

## Running

```bash
# uses ./../../build/programs/clickhouse by default, or pass --clickhouse / $CLICKHOUSE
./run.sh --rows 1000000 --iters 9
```

Options: `--clickhouse PATH`, `--rows N`, `--row-bytes B`, `--iters K` (median over K, warmup dropped), `--threads T` (`max_threads`, default 1 to isolate a single transport instance).

The runner uses `clickhouse-local` with a generated config that points at `functions.xml` and `user_scripts/`, runs `SELECT sum(length(fn(val))) FROM (… numbers(N))` for each variant, and reports:

* **median query time** (from `--time`);
* **bytes that crossed the kernel via `read()`/`write()` syscalls** (`OSReadChars` / `OSWriteChars` profile events) — a build-independent structural measure of transport cost. For the pipe transports this equals the payload volume; for the shared-memory transports only the tiny control messages are counted.

## Sweeps

`matrix.sh` runs the same comparison across a range of block sizes, thread counts and row sizes, reporting the pipe-vs-shared-memory ratio for each point:

```bash
CLICKHOUSE=../../build/programs/clickhouse ./matrix.sh
```

## Raw IPC micro-benchmark

`ipc_microbench.c` is a standalone C program (no ClickHouse) that measures the throughput of moving a large buffer between a parent and a child process, one chunk at a time, strictly synchronized — the same lock-step pattern the transport uses. It compares `pipe`, `tmpfs`+`mmap`, `memfd_create`+`mmap` and `vmsplice`. Because it is optimized native code, its numbers are independent of the ClickHouse build type and isolate the transport primitive itself.

```bash
cc -O2 -o ipc_microbench ipc_microbench.c
./ipc_microbench            # 8 MiB chunks
./ipc_microbench 65536 20000   # 64 KiB chunks
```

Typical finding: `tmpfs`+`mmap` and `memfd`+`mmap` are essentially identical (the choice between them is about the child contract, not speed), both clearly beat `pipe`, and `vmsplice` is fast but only streams bytes into a pipe rather than exposing addressable shared memory — which is why it does not fit the UDF model.

## What to expect

The shared-memory transport moves the bulk data through an `mmap`ed `tmpfs` file, so essentially no payload crosses the kernel: `OSReadChars`/`OSWriteChars` drop from the full payload volume to a few tens of kilobytes (control messages only), and wall-clock time drops accordingly.

> The `build/` in this repository is a **Debug** build, so absolute times are much slower than a release build; the meaningful figures are the *relative* transport comparison and the (build-independent) syscall-I/O volume.

## Measured result

One release-build run with `./run.sh --rows 1000000 --row-bytes 100 --iters 9 --threads 1` produced:

| transport | median, s | read via syscalls | write via syscalls |
|---|---:|---:|---:|
| `bench_pipe_stream` | 0.76 | 96.4 MiB | 96.3 MiB |
| `bench_pipe_chunk` | 0.26 | 96.4 MiB | 96.3 MiB |
| `bench_shm` | 0.18 | 0.04 MiB | 0.00 MiB |
| `bench_shm_busy` | 0.56 | 0.04 MiB | 0.00 MiB |

The fair pipe baseline is `bench_pipe_chunk`, because it also exchanges data once per block. In this run `bench_shm` is `1.44x` faster (`0.26 / 0.18`) and removes almost all kernel-visible payload I/O (`~96 MiB` down to `~0.04 MiB`).

The same harness was also swept with `matrix.sh` (`bench_pipe_chunk` vs `bench_shm`, larger shared-memory region, pool size `16`). Representative release-build medians:

| sweep | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| `max_block_size = 8192` | 0.342 | 0.263 | 1.30x |
| `max_block_size = 16384` | 0.391 | 0.255 | 1.53x |
| `max_block_size = 32768` | 0.436 | 0.309 | 1.41x |
| `max_block_size = 65536` | 0.498 | 0.330 | 1.51x |
| `max_block_size = 131072` | 0.547 | 0.357 | 1.53x |

Standalone `ipc_microbench.c` results for the underlying IPC primitive showed `tmpfs`+`mmap` and `memfd`+`mmap` with equivalent throughput, and both above `pipe` for the lock-step bulk-transfer pattern used by executable UDFs. This supports choosing a named `tmpfs` file for compatibility with arbitrary UDF scripts rather than for a microbenchmark-only advantage.
