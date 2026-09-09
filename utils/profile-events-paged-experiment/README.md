# Paged profile-counter research harness

**Process counters remain dense.** The standalone adapter supports paged storage for microbenchmarks and correctness experiments. Server integration is blocked until publishers that rely on nonallocating updates have an enforceable contract. This revision does not reduce server counter memory.

Ordinary builds leave `ENABLE_PROFILE_EVENTS_PAGED_EXPERIMENT=OFF`. Enabled builds retain dense process storage and can collect construction/destruction diagnostics. Selecting `CH_COUNTER_STORAGE=paged` in an enabled server exits with status 78 before allocating or exposing a process counter, even with an all-hot layout. The error explicitly identifies the pending publisher audit; the request is not silently changed to dense.

## Why server integration is blocked

The initial 40-event catalogue covers several signal, allocator, CPU scheduler, memory-reservation, IO-resource and timer publishers. It is a research subset, not a complete no-allocation contract.

The review found missing events in existing allocation-denial scopes:

- `OvercommitTracker::needToStopQuery` publishes `MemoryOvercommitWaitTimeMicroseconds` while holding `overcommit_m`.
- `ThreadFromThreadPool::worker` publishes global/local lock-wait and job-wait events under its denial scope. Worker self-removal also reaches the global/local shrink events through destruction.
- Signal-pipe writes and jemalloc sample-hook failure paths have additional publishers outside the initial catalogue.

A larger list alone cannot establish the full contract. `AsyncLoader::finish` reaches `LoadJob::finish` before its allocation allowance; the latter clears stored `std::function` targets. Captured objects can have destructors that publish arbitrary events. Prior exception destruction in `ThreadPool::worker` and virtual task destruction on executor exception paths introduce similar ownership boundaries. This does not assert that every existing capture emits metrics; it means the interfaces do not enforce a bounded event set.

The existing `DENY_ALLOCATIONS_IN_SCOPE` guard is disabled in Release and on macOS. The process-storage gate therefore applies independently of that guard. Re-enabling paged process storage requires auditing normal and exceptional cleanup paths, resolving dynamic publishers, and validating actual allocator/tracing/parent behavior in a linked server. Adding only the latest reported event names is insufficient.

## Run the standalone validator

No server build is required:

```
python3 utils/profile-events-paged-experiment/validate_adapter.py \
    --repo /absolute/checkout --output /absolute/new-build-directory --actual-catalogue
```

The validator defaults to ASan/UBSan. It records build/per-case logs and source, generated-fixture and binary hashes. `--actual-catalogue` resolves the pinned 1563 builtin IDs from `ProfileEvents.cpp`, checks the 40-name reservation subset and the required CPU/memory/IO scheduler families, and uses the bundled rank. Unsupported catalogue formats, missing names and malformed ranks fail validation. Omitting the flag retains an explicitly synthetic ten-event fixture.

The cases cover values across all event IDs, overflow, zero updates, retained reset, modeled ownership transfer, concurrent publication/snapshots and diagnostics. Reservation cases check stable ordering and inverse mapping at hot budgets 40 and 128, updates with aligned allocations forced to fail, and cold-allocation failure/retry. Budget 39 is rejected in actual-catalogue mode. Process-storage cases call the same gate as production allocation: explicit/default dense is accepted, while paged hot128 and all-hot1563 are rejected before constructing the modeled owner.

These tests exercise the actual adapter with modeled owners. They do not execute production `Counters` parent traversal, real signal delivery, scheduler timers or allocator hooks. The process constructor always uses dense allocation and no longer calls the compact adapter factory.

## Standalone treatment configuration

Environment variables configure the standalone adapter; the validator supplies isolated values for each case. Enabled-server diagnostics accept dense mode only.

| Variable | Meaning |
| --- | --- |
| `CH_COUNTER_STORAGE` | `dense` or `paged`; default `dense`. Paged is standalone-only. |
| `CH_COUNTER_LAYOUT` | Absolute file containing a complete permutation of the 1563 numeric event IDs; required for standalone paged mode. |
| `CH_COUNTER_HOT` | Total hot cells including reservations; default 128, minimum 40 with the real catalogue. |
| `CH_COUNTER_PAGE` | Cold-page cells: 8, 16, 32, 64 or 128; default 32. |
| `CH_COUNTER_DIAGNOSTICS` | Optional absolute path for a new exclusive diagnostics file; leave unset for timing runs. |

Configuration moves mandatory events ahead of the input frequency rank, preserving relative order within both groups. It rejects insufficient capacity. Named event objects supply production catalogue IDs; the runtime reservation list does not hardcode numeric IDs.

The public-only input rank `layout_calls128.txt` has SHA256 `d847bed0e7ef17cb182ab23bbccf80b80eff0da8fae163bf83c067250acc6808`. It was ranked from synthetic functional-test usage with the original ten reservations protected; it contains no customer/cloud data and is not claimed to be production-optimal. The rank preserves the original event-name ordering across the master catalogue update and appends the new `AdaptiveAggregationSpillBacklogSheds` event last. Record both rank and reservation catalogue: normalization against 40 names changes the effective layout without changing the input file.

`Counters::incrementNonAllocating` accepts a token made by `nonAllocatingEvent` for a catalogued named object. Factory instantiation rejects an unclassified event; classification uses object identity, not its numeric value. The first migrated caller is the CPU scheduler's pending wait-time destructor. This checks event eligibility, not arbitrary calls under mutexes. The method preserves parent propagation and tracing, uses the debug allocation-denial scope, and is `noexcept`; an escaping exception terminates. Dense process backing preserves the existing storage behavior for all events, including uncatalogued publishers.

## Standalone semantics and limitations

The hot array is allocated at construction. Cold pages are zero-initialized and published by release CAS; losing candidates are freed. Readers acquire pointers and values use relaxed 64-bit atomics with modular overflow. Reset zeros retained storage; broad-event workloads can lose the memory advantage. Snapshot output remains dense.

First cold increments allocate and can throw. A recursive nonzero cold update exits with status 79; an unsupported nonzero cold signal update exits with status 80. These explicit research restrictions are not acceptable production recovery. Pointer tagging, destruction, moves and concurrent ownership still require external lifetime discipline. No signal, tracing or parent-propagation feature is silently disabled to manufacture a speedup.

Diagnostics use the version-1 layout of 24 native-endian unsigned 64-bit words (192 bytes), declared in `ProfileEventsPagedExperiment/adapter.h`. They report construction/destruction counts, live/peak object counts, requested and allocator-usable backing sums/maxima, backend-wrapper bytes and enclosing-object size. Published-allocation counts exclude losing CAS candidates. Final sums exclude surviving objects; live reads are nontransactional. These are not RSS, peak live backing or query-specific memory measurements. Keep telemetry off for timing and collect matched memory runs separately.
