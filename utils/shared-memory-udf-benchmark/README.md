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

Each shared-memory worker reserves its whole region (`shared_memory_size` in [`functions.xml`](functions.xml), `16 MiB`, growable to `128 MiB` for the row-size sweep) up front, in a sealed `memfd`, and one worker is borrowed per parallel UDF call. So `--threads T` needs `T x 16 MiB` of RAM for the regions (charged to the server's memory tracker as well). There is no `/dev/shm` to size: a `memfd` lives in no filesystem.

Size the region for the block, not generously: it is reserved in full - pages committed - when a worker starts, so an oversized region costs memory for every worker and time for every pool warm-up. (An earlier version of these scripts ran every sample in its own `clickhouse-local`, which starts the pool inside the timed query; with `128 MiB` regions and 16 workers that put `2 GiB` of page zeroing into a `0.2 s` query and looked like a transport loss at high parallelism. It was not one, and the runners now measure a warm pool - see below.)

The runner starts a throw-away `clickhouse server` on a free local port with a generated config that points at `functions.xml` and `user_scripts/` (`lib.sh`), runs one warm-up query per function - which starts its pool of workers and, for shared memory, reserves their regions - and then `SELECT sum(length(fn(val))) FROM (… numbers_mt(N))` for the samples. A server rather than `clickhouse-local` because the transport under test is a *pooled* one: `clickhouse-local` lives for one query, so every sample would restart the workers and pay the pool start-up that a real server pays once. The runner reports:

* **median query time** (from `--time`), at a pinned `max_block_size` (`--block-size`, default
  `65536`) — the block-size sweep below moves the ratio from `1.98x` to `1.31x`, so
  leaving it to the server default would make the headline number depend on something the run does
  not state;
* **bytes that crossed the kernel via `read()`/`write()` syscalls** (`OSReadChars` / `OSWriteChars` profile events) — a build-independent structural measure of transport cost. For the pipe transports this equals the payload volume; for the shared-memory transports it is only the tiny control messages, because both sides work through their mappings and no payload byte goes through `read`/`write` at all.

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

Typical finding: `tmpfs`+`mmap` and `memfd`+`mmap` are essentially identical (the transport uses `memfd`, because it can be sealed against shrinking; the speed is the same), both clearly beat `pipe`, and `vmsplice` is fast but only streams bytes into a pipe rather than exposing addressable shared memory — which is why it does not fit the UDF model.

## What to expect

The shared-memory transport moves the bulk data through a sealed `memfd` that both the server and
the command `mmap`. The server serializes the input straight into its mapping and parses the output
where the command left it; the command reads and writes in a mapping of its own. No payload byte is
copied and no payload byte goes through `read`/`write`, so `OSReadChars`/`OSWriteChars` show only
the control messages. Writing through a mapping of a file the command holds open for writing is
safe only because the file is sealed with `F_SEAL_SHRINK` - the command cannot take pages out from
under the server (see `SharedMemoryRegion`). What is saved against a pipe is two copies per
direction plus the per-pipeful syscalls; how much of that shows up as wall-clock time depends on how
large a share of the query the transport is at all - measure it with the runner instead of assuming.

> The `build/` in this repository is a **Debug** build, so absolute times are much slower than a release build; the meaningful figures are the *relative* transport comparison and the (build-independent) syscall-I/O volume.

## Measured result

Environment: AMD Ryzen 9 7940HS (8 cores / 16 threads), 59 GiB RAM, Linux 7.0.0-31, Release build
of this branch (sealed `memfd`, server works through its mapping), warm pools, machine otherwise
idle. Both pipe and shared-memory commands move a chunk with one bulk read and one bulk write, so
what is compared is the transport.

```bash
./run.sh --clickhouse ../../build_release/programs/clickhouse \
         --rows 1000000 --row-bytes 100 --iters 9 --threads 1 --block-size 65536
CLICKHOUSE=../../build_release/programs/clickhouse ITERS=7 ./matrix.sh
```

| transport | median, s | read via syscalls | write via syscalls |
|---|---:|---:|---:|
| `bench_pipe_stream` | 0.791 | 96.37 MB | 96.32 MB |
| `bench_pipe_chunk` | 0.130 | 96.37 MB | 96.32 MB |
| `bench_shm` | 0.096 | 0.04 MB | 0.00 MB |
| `bench_shm_busy` | 0.459 | 0.04 MB | 0.00 MB |

The fair baseline is `bench_pipe_chunk`, which also exchanges data once per block. Against it the
shared-memory transport is `1.35x` at the default block size. Against the streaming pattern the
documentation shows for pipes (a flush per row) it is `8.2x`, but that difference is mostly the
per-row flushing, not the transport.

The syscall columns are the structural result: the pipe moves the whole payload through
`read`/`write` in both directions, the shared-memory transport moves `40 KB` of control messages
(one request and one response per block) and nothing else. The payload is written once, by the
server into its mapping, and read in place by both sides.

Sweeps (`matrix.sh`, medians of 7):

| block size (2M rows, 100 B, 1 thread) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 8192 | 0.395 | 0.200 | 1.98x |
| 16384 | 0.379 | 0.195 | 1.94x |
| 32768 | 0.318 | 0.173 | 1.84x |
| 65536 | 0.254 | 0.181 | 1.40x |
| 131072 | 0.241 | 0.184 | 1.31x |

| threads (4M rows, 100 B, block 65536) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 1 | 0.484 | 0.348 | 1.39x |
| 2 | 0.270 | 0.205 | 1.32x |
| 4 | 0.176 | 0.144 | 1.22x |
| 8 | 0.158 | 0.139 | 1.14x |
| 16 | 0.150 | 0.142 | 1.06x |

| rows x row bytes (~200 MB, 1 thread, block 65536) | `bench_pipe_chunk`, s | `bench_shm`, s | speedup |
|---|---:|---:|---:|
| 20000000 x 10 | 1.081 | 0.961 | 1.12x |
| 2000000 x 100 | 0.245 | 0.187 | 1.31x |
| 200000 x 1000 | 0.296 | 0.211 | 1.40x |

How to read this: the transport wins everywhere, and the win shrinks as the block grows (`1.98x`
at 8192 rows, `1.31x` at 131072) and as parallelism grows (`1.39x` on one thread, `1.06x` on
sixteen), because in both directions the command's own work and the machine's saturation grow
while the saving per block does not. On this 8-core machine 16 threads means 32 processes
competing for 8 cores, and there the transport is no longer where the time goes.

`perf stat` attached to the warm server and its workers for one 16-thread query (4M rows) shows
where the saving sits: context switches `1.0k` for shared memory against `22.8k` for the pipe,
kernel cycles `1.6G` against `4.8G`, cache misses `15M` against `33M`. The pipe's cost is the
kernel copy and the wake-ups around a 64 KiB buffer; the shared-memory transport has neither. User
cycles go the other way (`8.8G` against `6.0G`): the Python echo copies its slice in user space,
where the pipe client had the kernel copy it - that is the benchmark command's own work, not the
transport's.

Numbers move by a few per cent between runs on the same machine, so re-run both scripts after any
change to the transport rather than quoting these — the headline row in particular is inside that
spread. Earlier revisions of this file quoted `1.44x` and `1.30-1.53x`; those numbers were measured
against a pipe client that parsed the chunk one `readline` per row, which charged the pipe baseline
several milliseconds per block that the shared-memory client never paid.

Standalone `ipc_microbench.c` results for the underlying IPC primitive showed `tmpfs`+`mmap` and
`memfd`+`mmap` with equivalent throughput, and both above `pipe` for the lock-step bulk-transfer
pattern used by executable UDFs. The choice of `memfd` over a named `tmpfs` file is therefore not
about speed: it is what can be sealed against shrinking, which is what lets the server work through
its mapping at all, and `/proc/self/fd/N` keeps the "open a path and `mmap` it" contract for
arbitrary UDF scripts.
