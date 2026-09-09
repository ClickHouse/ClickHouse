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
* **bytes that crossed the kernel via `read()`/`write()` syscalls** (`OSReadChars` / `OSWriteChars` profile events) — a build-independent structural measure of transport cost. For the pipe transports this equals the payload volume; for the shared-memory transports it is the payload once per direction plus the tiny control messages, because the server's side of the region goes through `pread`/`pwrite` (see below), which `taskstats` counts like any other read or write.

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

The shared-memory transport moves the bulk data through a `tmpfs` file that the command `mmap`s.
The command's side is therefore copy-free, but the server's is not: it writes the input with
`pwrite` and reads the output with `pread` rather than through a mapping of its own, because the
command holds the same file open for writing and can shorten it at any moment - through a mapping
that is a `SIGBUS` that takes the whole server down, and no check can close the window (see
`SharedMemoryRegion`). So the payload crosses the kernel once per direction instead of the two
crossings a pipe costs, and `OSReadChars`/`OSWriteChars` show roughly the payload volume rather than
zero. What is saved against a pipe is one copy per direction plus the per-pipeful syscalls; how much
of that shows up as wall-clock time depends on how large a share of the query the transport is at
all - measure it with the runner instead of assuming.

> The `build/` in this repository is a **Debug** build, so absolute times are much slower than a release build; the meaningful figures are the *relative* transport comparison and the (build-independent) syscall-I/O volume.

## Measured result

Environment: AMD Ryzen 9 7940HS (16 threads), 59 GiB RAM, `/dev/shm` 29.8 GiB, Linux 7.0.0-31,
Release build of this branch (the state where the server reaches the region through `pread`/`pwrite`
rather than a mapping of its own), machine otherwise idle. Both pipe and shared-memory commands move
a chunk with one bulk read and one bulk write, so what is compared is the transport.

```bash
./run.sh --clickhouse ../../build_release/programs/clickhouse \
         --rows 1000000 --row-bytes 100 --iters 9 --threads 1
CLICKHOUSE=../../build_release/programs/clickhouse ./matrix.sh   # ITERS=7
```

| transport | median, s | read via syscalls | write via syscalls |
|---|---:|---:|---:|
| `bench_pipe_stream` | 0.745 | 96.37 MB | 96.32 MB |
| `bench_pipe_chunk` | 0.167 | 96.37 MB | 96.32 MB |
| `bench_shm` | 0.182 | 96.36 MB | 96.32 MB |
| `bench_shm_busy` | 0.540 | 96.36 MB | 96.32 MB |

The fair baseline is `bench_pipe_chunk`, which also exchanges data once per block. Against it the
shared-memory transport is a **wash at this block size** — `0.92x` here, `0.99x` on a repeat run,
i.e. within the run-to-run spread. Against the streaming pattern the documentation shows for pipes
(a flush per row) it is `4.5x`, but that difference is mostly the per-row flushing, not the
transport.

The syscall columns are now equal for both transports, and that is the implementation, not a
measurement error: `OSReadChars`/`OSWriteChars` are collected from the *server's* threads, and the
server reaches the region with `pread`/`pwrite`. The saving is real but sits on the command's side —
it works in its own mapping and issues no transfer syscalls at all — so the payload crosses the
kernel once in total instead of twice. No server-side counter can see that; `/proc/<pid>/io` of the
child would, and ClickHouse does not collect it.

Sweeps (`matrix.sh`, medians of 7):

| block size (2M rows, 100 B, 1 thread) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 8192 | 0.379 | 0.288 | 1.32x |
| 16384 | 0.411 | 0.300 | 1.37x |
| 32768 | 0.359 | 0.311 | 1.15x |
| 65536 | 0.311 | 0.355 | 0.88x |
| 131072 | 0.329 | 0.372 | 0.88x |

| threads (4M rows, 100 B, block 65536) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 1 | 0.561 | 0.602 | 0.93x |
| 2 | 0.317 | 0.380 | 0.83x |
| 4 | 0.222 | 0.309 | 0.72x |
| 8 | 0.232 | 0.292 | 0.79x |
| 16 | 0.205 | 0.346 | 0.59x |

| rows x row bytes (~200 MB, 1 thread, block 65536) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 20000000 x 10 | 1.349 | 1.156 | 1.17x |
| 2000000 x 100 | 0.289 | 0.316 | 0.91x |
| 200000 x 1000 | 0.481 | 0.468 | 1.03x |

How to read this: the transport wins where the per-block overheads dominate — `1.15x` to `1.37x` at
block sizes of 8192 to 32768 — and loses once the copy the server now pays for (`S + S'` bytes
through `pread`/`pwrite`) starts to dominate. The break-even is around 32-64k rows. It is **not** a
win at parallelism either: every parallel call borrows its own worker with its own `128 MiB` region,
and the loss grows monotonically to `0.59x` at 16 threads.

The block-size sweep is also the clearest measurement of what the `SIGBUS` fix cost. Before the
server gave up its own mapping, the same sweep on the same machine was a win across the whole range
(`1.09x`-`1.64x`); that difference is the price of not letting a command's `ftruncate` take the
server down. Getting it back needs a backing store the command cannot resize — see the design notes
on `SharedMemoryRegion`.

Numbers move by a few per cent between runs on the same machine, so re-run both scripts after any
change to the transport rather than quoting these — the headline row in particular is inside that
spread. Earlier revisions of this file quoted `1.44x` and `1.30-1.53x`; those numbers were measured
against a pipe client that parsed the chunk one `readline` per row, which charged the pipe baseline
several milliseconds per block that the shared-memory client never paid.

Standalone `ipc_microbench.c` results for the underlying IPC primitive showed `tmpfs`+`mmap` and
`memfd`+`mmap` with equivalent throughput, and both above `pipe` for the lock-step bulk-transfer
pattern used by executable UDFs. This supports choosing a named `tmpfs` file for compatibility with
arbitrary UDF scripts rather than for a microbenchmark-only advantage.
