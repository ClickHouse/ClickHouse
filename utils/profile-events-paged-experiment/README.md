# Paged process-counter experiment

**Draft, not mergeable or suitable for production.** This opt-in experiment reduces the backing footprint of `ProfileEvents::Counters` at `VariableContext::Process` and visits only hot cells and published cold pages when producing a snapshot. It is intended for reproducible server comparisons and design review. The signal and allocation-failure limitations below are unresolved correctness blockers.

Ordinary builds leave `ENABLE_PROFILE_EVENTS_PAGED_EXPERIMENT=OFF`. Experiment declarations, includes, dispatch checks, and destruction changes are excluded by preprocessing; the original dense source path and `Counters` layout remain. Enabled builds are restricted to Linux x86-64/AArch64 and require lock-free 64-bit atomics. No census instrumentation is required.

## Build and select a treatment

Configure with `-DENABLE_PROFILE_EVENTS_PAGED_EXPERIMENT=ON`, then build the usual `clickhouse` target. The enabled binary uses the original dense representation by default. Environment variables are read once, before the first process-counter construction publishes its storage:

| Variable | Meaning |
| --- | --- |
| `CH_COUNTER_STORAGE` | `dense` or `paged`; default `dense` |
| `CH_COUNTER_LAYOUT` | Absolute path containing a complete permutation of the 1562 global numeric event IDs; required for `paged` |
| `CH_COUNTER_HOT` | Hot-prefix count; default 128 |
| `CH_COUNTER_PAGE` | Cold-page cells; 8, 16, 32, 64, or 128; default 32 |
| `CH_COUNTER_DIAGNOSTICS` | Optional absolute path for a new, exclusive diagnostics file; leave unset for timing runs |

The event catalogue is deliberately pinned for this draft. An enabled-build assertion checks its length. The mandatory hot set resolves the actual named event objects when configuration initializes; it does not encode numeric IDs or create a dynamically initialized global lookup array. Event reordering therefore cannot silently redirect the signal reservations. Layouts must place `QueryProfilerConcurrencyOverruns`, `QueryProfilerSignalOverruns`, `QueryProfilerErrors`, `QueryProfilerRuns`, `CannotWriteToWriteBufferDiscard`, `MemoryAllocatedWithoutCheck`, `MemoryAllocatedWithoutCheckBytes`, `QueryMemoryLimitExceeded`, `GlobalMemoryLimitExceeded`, and `PageCacheOvercommitResize` in the hot prefix. A production design needs a maintained catalogue contract.

A public-only example layout is included as `layout_calls128.txt` (SHA256 `ce68dd745c71a1f6de8e0aefcdb4865598f93358af3d0dc922b4851a8c3af652`). It was ranked from synthetic functional-test usage, with required events protected; it contains no customer/cloud data and is not claimed to be production-optimal. Its 1562-ID permutation and all ten mandatory hot positions were checked.

After building the enabled binary, start an owned test server with an explicit test configuration:

```sh
CH_COUNTER_LAYOUT="$PWD/utils/profile-events-paged-experiment/layout_calls128.txt" \
CH_COUNTER_STORAGE=paged CH_COUNTER_HOT=128 CH_COUNTER_PAGE=32 \
    build/programs/clickhouse server --config-file=/absolute/owned-test/config.xml
```

All process-level groups use the selected representation, including descendant and background groups. Thread, user, and global representations remain dense; parent propagation and event tracing retain their existing control flow. Zero amounts skip compact backing work only. An enabled dense control still pays experiment dispatch checks; it is not identical to an ordinary build and must be measured separately.

The hot array is allocated at construction. Cold pages are zero-initialized and published by release CAS. Readers acquire the page pointer; individual values use relaxed 64-bit atomic operations and preserve modular overflow. A losing page candidate is released. Reset atomically zeros retained storage without removing published pages. Snapshots still allocate a full dense destination using the original `Snapshot` constructor; this experiment does not compact query-log or protocol snapshot objects.

## Unresolved production blockers

- A first cold increment allocates through tracked aligned global `operator new`. Previously nonallocating increment paths can now throw. Timer destructors, `noexcept` callers, allocation failure, and allocator-hook reentrancy need a complete design; the current guard is not such a design.
- A recursive nonzero cold update writes a fixed diagnostic and exits with status 79. This makes the research restriction visible instead of silently losing updates or attempting a fallback. It is not acceptable production recovery.
- Compact `incrementSignalSafe` supports hot events only. An unexpected nonzero cold signal event writes a fixed diagnostic and exits with status 80. Current known signal IDs are reserved, but the generic API accepts arbitrary events. A production patch must preserve that contract or redesign it explicitly with enforceable caller restrictions.
- Two pointer-tag bits distinguish dense and paged ownership without adding a member. This is an experimental representation, not a settled production abstraction. Moves transfer the existing pointer and holder; destruction requires detached current-thread/parent references and external quiescence. Free paths can themselves emit profile events.
- A reset retains all pages ever touched. Broad-event workloads can lose the memory advantage. Constructor-reserved storage could avoid lazy-allocation failures but would also remove the principal heap-footprint saving; no such fallback is hidden here.

Configuration failures exit with status 78. No signal, profiler, memory-accounting, or parent-propagation feature is silently disabled. Successful smoke runs cannot establish safety for unexercised signal IDs, OOM, or object lifetimes.

## Diagnostics and validation

Diagnostics retain the experiment's version-1 layout of 24 unsigned 64-bit native-endian words (192 bytes). `mode` is 0 for dense or 1 for paged; the reserved `segment` word is zero. The schema is declared in `ProfileEventsPagedExperiment/adapter.h`. Construction/destruction update raw atomics in a shared mapping; increments have no telemetry updates. Live external reads are nontransactional.

The fields report creations, destructions, live/peak object counts, construction backing sums, and quiescent destruction backing sums/maxima. Requested backing bytes, allocator-usable backing bytes, backend-wrapper bytes, and enclosing `Counters` size are separate. Final sums exclude surviving objects, and published-allocation counts exclude losing CAS candidates. They do not measure peak live backing or query-specific memory. Keep telemetry off for throughput comparisons and use separate matched memory runs.

Run the isolated validator without building the server:

```
python3 utils/profile-events-paged-experiment/validate_adapter.py \
    --repo /absolute/checkout --output /absolute/new-build-directory
```

It defaults to ASan/UBSan and stores unique build/per-case logs plus source/binary hashes. The tests cover dense/paged hot128 and all-hot1562, global-ID values, overflow, zero-update nonallocation, retained reset, modeled ownership transfer, concurrent first publication/snapshots, diagnostics, and explicit unsupported-path/configuration exits. The mandatory-event function uses an explicitly synthetic reservation stub in these standalone checks. They model the adapter's caller; they do not test actual `Counters` moves, real signal delivery, server allocator hooks, or the full server. An enabled full-server build and profiler-enabled functional/stress validation remain necessary before drawing integration conclusions.
