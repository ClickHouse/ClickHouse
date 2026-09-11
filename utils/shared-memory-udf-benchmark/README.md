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

* **query time** (from `--time`): the median of `--iters` runs, its interquartile range, and - for
  every transport - the speedup against `bench_pipe_chunk` as a ratio of medians with a 95%
  bootstrap confidence interval (`stats.py`). All at a pinned `max_block_size` (`--block-size`,
  default `65536`) — the block-size sweep below moves the ratio from `2.06x` to `1.19x`, so leaving
  it to the server default would make the headline number depend on something the run does not
  state. Medians rather than means because noise on a shared machine is one-sided (a run can only
  be slowed down), and a bootstrap interval rather than a t-interval for the same reason: nothing
  here is normal. A speedup whose interval contains `1.0` is parity, whatever the point estimate.
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
what is compared is the transport. 15 runs per point; median, interquartile range, speedup as
ratio of medians with a 95% bootstrap confidence interval.

```bash
./run.sh --clickhouse ../../build_release/programs/clickhouse \
         --rows 1000000 --row-bytes 100 --iters 15 --threads 1 --block-size 65536
CLICKHOUSE=../../build_release/programs/clickhouse ITERS=15 ./matrix.sh
```

| transport | median, s | IQR, s | vs its pipe baseline [95% CI] | read via syscalls | write via syscalls |
|---|---:|---:|---:|---:|---:|
| `bench_pipe_chunk` | 0.132 | 0.129–0.134 | 1 (baseline) | 96.37 MB | 96.32 MB |
| `bench_pipe_stream` | 0.786 | 0.783–0.789 | 0.17x [0.16–0.17] | 96.37 MB | 96.32 MB |
| `bench_pipe_chunk_1m` | 0.119 | 0.117–0.121 | 1.11x [1.07–1.14] | 96.37 MB | 96.32 MB |
| `bench_shm` | 0.097 | 0.093–0.104 | 1.36x [1.25–1.43] | 0.04 MB | 0.00 MB |
| `bench_pipe_busy` | 0.508 | 0.506–0.518 | 1 (baseline) | 96.37 MB | 96.32 MB |
| `bench_shm_busy` | 0.465 | 0.463–0.471 | 1.09x [1.08–1.12] | 0.04 MB | 0.00 MB |

The fair baseline is `bench_pipe_chunk`, which also exchanges data once per block. Against it the
shared-memory transport is `1.36x` at the default block size, with the whole interval well above
`1.0`. Against the streaming pattern the documentation shows for pipes (a flush per row) it is
`8.1x`, but that difference is mostly the per-row flushing, not the transport.

The busy pair compares like with like: `bench_shm_busy` is measured against `bench_pipe_busy`,
the chunked pipe echo with the same artificial per-chunk work. The transport still saves about
`40 ms` of the query - the same absolute amount as in the echo pair - but on a command that
spends `0.5 s` computing, that is `1.09x`, not `1.36x`. Which is the whole point of the pair: the
saving is per block and fixed, so the heavier the command, the smaller its share.

`bench_pipe_chunk_1m` answers the obvious question - would a bigger pipe do? It is the same
chunked pipe function with `command_pipe_capacity` raised to `1 MiB` (`F_SETPIPE_SZ`), which
cuts most of the transfer syscalls. Across two series it gained `1.03x [1.00-1.08]` and
`1.11x [1.07-1.14]` - a small part of what shared memory gains on the same data (`1.38x` and
`1.36x` in those series). The number of syscalls is not most of what the pipe pays for on this
workload; the copies and the wake-ups around the buffer are, and a bigger pipe removes neither.

**Between-series spread.** The same point - 65536-row blocks, one thread - appears in several
series (the headline run, the first row of each sweep, repeated runs of the headline), with point
estimates from `1.29x` to `1.42x`. That spread, about `±5%`, is wider than any single series'
bootstrap interval (`±2-4%`), so the intervals describe the noise *within* a series, not the
reproducibility of a number across runs. Read the headline as "about 1.3-1.4x", and do not
interpret differences below `0.1x` between two series as anything.

The syscall columns are the structural result: the pipe moves the whole payload through
`read`/`write` in both directions, the shared-memory transport moves nothing of the payload. The
`40 KB` it shows are not the protocol either - 16 exchanges of a few dozen bytes each are about a
kilobyte; the rest is the server threads' other I/O during the query, the same for both transports. The payload is written once, by the
server into its mapping, and read in place by both sides.

Sweeps (`matrix.sh`, 15 runs per point):

| block size (2M rows, 100 B, 1 thread) | `pipe_chunk`, s [q1–q3] | `shm`, s [q1–q3] | speedup [95% CI] |
|---|---:|---:|---:|
| 8192 | 0.375 [0.364–0.379] | 0.204 [0.202–0.210] | 1.84x [1.75–1.87] |
| 16384 | 0.388 [0.382–0.408] | 0.188 [0.183–0.194] | 2.06x [1.99–2.19] |
| 32768 | 0.336 [0.331–0.339] | 0.189 [0.184–0.192] | 1.78x [1.74–1.82] |
| 65536 | 0.257 [0.255–0.260] | 0.197 [0.194–0.200] | 1.30x [1.29–1.34] |
| 131072 | 0.262 [0.261–0.270] | 0.221 [0.214–0.240] | 1.19x [1.09–1.25] |

| threads (4M rows, 100 B, block 65536) | `pipe_chunk`, s [q1–q3] | `shm`, s [q1–q3] | speedup [95% CI] |
|---|---:|---:|---:|
| 1 | 0.560 [0.550–0.567] | 0.433 [0.410–0.440] | 1.29x [1.26–1.37] |
| 2 | 0.313 [0.294–0.337] | 0.225 [0.216–0.260] | 1.39x [1.19–1.52] |
| 4 | 0.197 [0.195–0.202] | 0.152 [0.148–0.159] | 1.30x [1.23–1.34] |
| 8 | 0.163 [0.161–0.168] | 0.138 [0.137–0.139] | 1.18x [1.17–1.22] |
| 16 | 0.151 [0.149–0.152] | 0.142 [0.141–0.143] | 1.06x [1.05–1.07] |

| rows x row bytes (~200 MB, 1 thread, block 65536) | `pipe_chunk`, s [q1–q3] | `shm`, s [q1–q3] | speedup [95% CI] |
|---|---:|---:|---:|
| 20000000 x 10 | 1.113 [1.099–1.126] | 1.041 [1.032–1.056] | 1.07x [1.05–1.08] |
| 2000000 x 100 | 0.259 [0.254–0.264] | 0.197 [0.195–0.204] | 1.31x [1.26–1.35] |
| 200000 x 1000 | 0.357 [0.339–0.369] | 0.308 [0.287–0.340] | 1.16x [1.04–1.26] |

How to read this: every interval lies above `1.0`, so the transport wins at every point measured,
and the win shrinks as the block grows (`2.06x` at 16384 rows, `1.19x` at 131072) and as
parallelism grows (`1.29x` on one thread, `1.06x` on sixteen), because in both directions the
command's own work and the machine's saturation grow while the saving per block does not. On this
8-core machine 16 threads means 32 processes competing for 8 cores, and there the transport is no
longer where the time goes - the `1.06x` is distinguishable from parity, but only just. The widest
intervals (2 threads, 1000-byte rows) are the noisiest points, not the strongest claims.

**Idle memory** (`idle_memory.sh`, 16 workers per pool, 16 MiB regions): warming the pipe pool
adds nothing to the server's memory tracker (`+46 MB` of tracked memory is the query's own caches,
the workers' memory is theirs); warming the shared-memory pool adds `+240 MB` tracked - the idle
charge, `pool_size x shared_memory_size` - and the same `+237 MB` of system `Shmem`, where the
regions' pages live. Only the pages the server touched show in its RSS (`+119 MB`). That is the
trade: the transport keeps `pool_size x shared_memory_size` (twice that with the pipeline) resident
for as long as the pool lives, and it is charged against `max_server_memory_usage`.

`perf stat` attached to the warm server and its workers for one 16-thread query (4M rows) shows
where the saving sits: context switches `1.0k` for shared memory against `22.8k` for the pipe,
kernel cycles `1.6G` against `4.8G`, cache misses `15M` against `33M`. The pipe's cost is the
kernel copy and the wake-ups around a 64 KiB buffer; the shared-memory transport has neither. User
cycles go the other way (`8.8G` against `6.0G`): the Python echo copies its slice in user space,
where the pipe client had the kernel copy it - that is the benchmark command's own work, not the
transport's.

Numbers move by a few per cent between runs on the same machine, so re-run both scripts after any
change to the transport rather than quoting these, and read the intervals, not just the medians. Earlier revisions of this file quoted `1.44x` and `1.30-1.53x`; those numbers were measured
against a pipe client that parsed the chunk one `readline` per row, which charged the pipe baseline
several milliseconds per block that the shared-memory client never paid.

Standalone `ipc_microbench.c` results for the underlying IPC primitive showed `tmpfs`+`mmap` and
`memfd`+`mmap` with equivalent throughput, and both above `pipe` for the lock-step bulk-transfer
pattern used by executable UDFs. The choice of `memfd` over a named `tmpfs` file is therefore not
about speed: it is what can be sealed against shrinking, which is what lets the server work through
its mapping at all, and `/proc/self/fd/N` keeps the "open a path and `mmap` it" contract for
arbitrary UDF scripts.
