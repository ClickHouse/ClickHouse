# Executable UDF transport benchmark: pipes vs shared memory

This benchmark compares the data transports used by executable user-defined functions:

* `bench_pipe_stream`  — plain pipe transport, flush per row (the documented streaming pattern);
* `bench_pipe_chunk`   — pipe transport with `send_chunk_header`, flush once per chunk (fair per-chunk baseline);
* `bench_shm`          — shared-memory transport (`use_shared_memory`);
* `bench_shm_busy` — shared-memory transport with artificial per-request CPU work in the command.

The pipe and plain shared-memory functions are functionally identical echoes (see [`functions.xml`](functions.xml) and [`user_scripts/`](user_scripts)), and both clients move a chunk with one bulk read and one bulk write, so their differences are attributable to the transport rather than to per-row work in the command. `bench_shm_busy` additionally measures artificial command-side CPU work.

## Running

```bash
# uses ./../../build/programs/clickhouse by default, or pass --clickhouse / $CLICKHOUSE
./run.sh --rows 1000000 --iters 9
```

Options: `--clickhouse PATH`, `--rows N`, `--row-bytes B`, `--iters K` (median over K, warmup dropped), `--threads T` (`max_threads`, default 1 to isolate a single transport instance). The source is `numbers_mt`, because plain `numbers` is a single stream regardless of `max_threads` and would make every `--threads` value measure the same one-call-at-a-time pipeline.

### Shared memory the benchmark needs

Each shared-memory worker reserves its whole region (`shared_memory_size` in [`functions.xml`](functions.xml), `128 MiB`) with `posix_fallocate`, and one worker is borrowed per parallel UDF call. So `--threads T` needs `T x 128 MiB` in `/dev/shm`, and the 16-thread step of `matrix.sh` needs about `2 GiB` there (plus the same amount charged to the server's memory tracker). A container `/dev/shm` is often `64 MiB`, which does not fit even one worker - both scripts check this before running and stop with the required and available sizes. Mount a bigger `/dev/shm` (`docker run --shm-size=4g`), lower `shared_memory_size`, or use fewer threads.

The runner uses `clickhouse-local` with a generated config that points at `functions.xml` and `user_scripts/`, runs `SELECT sum(length(fn(val))) FROM (… numbers_mt(N))` for each variant, and reports:

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

The shared-memory transport moves the bulk data through an `mmap`ed `tmpfs` file, so essentially no payload crosses the kernel: `OSReadChars`/`OSWriteChars` drop from the full payload volume to a few tens of kilobytes (control messages only). How much of that shows up as wall-clock time depends on how large a share of the query the transport is at all - measure it with the runner instead of assuming.

> The `build/` in this repository is a **Debug** build, so absolute times are much slower than a release build; the meaningful figures are the *relative* transport comparison and the (build-independent) syscall-I/O volume.

## Measured result

The payload volume that crosses the kernel is a property of the transport itself, so it holds for any
build and any client. One release-build run with
`./run.sh --rows 1000000 --row-bytes 100 --iters 9 --threads 1` produced:

| transport | read via syscalls | write via syscalls |
|---|---:|---:|
| `bench_pipe_stream` | 96.4 MiB | 96.3 MiB |
| `bench_pipe_chunk` | 96.4 MiB | 96.3 MiB |
| `bench_shm` | 0.04 MiB | 0.00 MiB |
| `bench_shm_busy` | 0.04 MiB | 0.00 MiB |

The shared-memory transport moves `~96 MiB` of payload out of the kernel path, leaving only the
control messages.

The wall-clock medians of that run (`0.76`, `0.26`, `0.18` and `0.56` seconds respectively) are **not
reproduced here on purpose**: they were measured with the earlier version of `echo_pipe_chunked.py`,
which read the chunk one `readline` per row. That costs about `6.6 ms` of Python parsing and
allocation for a 65k-row block — roughly `0.1 s` of the `0.26 s` reported for `bench_pipe_chunk` over
the ~16 blocks of that run — and the shared-memory client never paid it, so a large part of the
measured gap was the client, not the transport. The client now moves each chunk in bulk on both
sides; regenerate the wall-clock figures with `run.sh` and `matrix.sh` on a release build before
quoting any speedup.

Standalone `ipc_microbench.c` results for the underlying IPC primitive showed `tmpfs`+`mmap` and
`memfd`+`mmap` with equivalent throughput, and both above `pipe` for the lock-step bulk-transfer
pattern used by executable UDFs. This supports choosing a named `tmpfs` file for compatibility with
arbitrary UDF scripts rather than for a microbenchmark-only advantage.
