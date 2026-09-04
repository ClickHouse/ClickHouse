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

Environment: AMD Ryzen 9 7940HS (16 threads), 59 GiB RAM, `/dev/shm` 29.8 GiB, Linux 7.0.0-30,
Release build of this branch (the state that includes the region-integrity checks), machine
otherwise idle. Both pipe and shared-memory
commands move a chunk with one bulk read and one bulk write, so what is compared is the transport.

```bash
./run.sh --clickhouse ../../build_release/programs/clickhouse \
         --rows 1000000 --row-bytes 100 --iters 9 --threads 1
CLICKHOUSE=../../build_release/programs/clickhouse ./matrix.sh   # ITERS=7
```

| transport | median, s | read via syscalls | write via syscalls |
|---|---:|---:|---:|
| `bench_pipe_stream` | 0.750 | 96.37 MB | 96.32 MB |
| `bench_pipe_chunk` | 0.166 | 96.37 MB | 96.32 MB |
| `bench_shm` | 0.150 | 0.04 MB | 0.00 MB |
| `bench_shm_busy` | 0.520 | 0.04 MB | 0.00 MB |

The fair baseline is `bench_pipe_chunk`, which also exchanges data once per block: against it the
shared-memory transport is **1.11x** faster here, and it moves `~96 MiB` of payload out of the
kernel path. Against the streaming pattern the documentation shows for pipes (a flush per row) it is
`5.0x`, but that difference is mostly the per-row flushing, not the transport.

Sweeps (`matrix.sh`, medians of 7):

| block size (2M rows, 100 B, 1 thread) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 8192 | 0.363 | 0.235 | 1.54x |
| 16384 | 0.374 | 0.228 | 1.64x |
| 32768 | 0.333 | 0.235 | 1.42x |
| 65536 | 0.284 | 0.243 | 1.17x |
| 131072 | 0.301 | 0.276 | 1.09x |

| threads (4M rows, 100 B, block 65536) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 1 | 0.543 | 0.437 | 1.24x |
| 2 | 0.320 | 0.284 | 1.13x |
| 4 | 0.218 | 0.219 | 1.00x |
| 8 | 0.215 | 0.247 | 0.87x |
| 16 | 0.226 | 0.255 | 0.89x |

| rows x row bytes (~200 MB, 1 thread, block 65536) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 20000000 x 10 | 1.345 | 1.076 | 1.25x |
| 2000000 x 100 | 0.291 | 0.248 | 1.17x |
| 200000 x 1000 | 0.436 | 0.367 | 1.19x |

How to read this: single-threaded, the transport is worth `1.1x` to `1.6x` depending on the block
size, and it always removes the payload from the kernel path. It is **not** a win at high
parallelism: from 8 threads on, `bench_shm` falls behind the pipe baseline (`0.87x`, `0.89x`), where
each parallel call borrows its own worker with its own `128 MiB` region and pays for faulting that
memory in.

Numbers move by a few per cent between runs on the same machine, so re-run both scripts after any
change to the transport rather than quoting these. Earlier revisions of this file quoted `1.44x` and `1.30-1.53x`; those numbers were
measured against a pipe client that parsed the chunk one `readline` per row, which charged the pipe
baseline several milliseconds per block that the shared-memory client never paid.

Standalone `ipc_microbench.c` results for the underlying IPC primitive showed `tmpfs`+`mmap` and
`memfd`+`mmap` with equivalent throughput, and both above `pipe` for the lock-step bulk-transfer
pattern used by executable UDFs. This supports choosing a named `tmpfs` file for compatibility with
arbitrary UDF scripts rather than for a microbenchmark-only advantage.
