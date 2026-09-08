# Paged process-counter experiment

**Draft, not mergeable or suitable for production.** This opt-in experiment reduces the backing footprint of `ProfileEvents::Counters` at `VariableContext::Process` and visits only hot cells and published cold pages when producing a snapshot. It is intended for reproducible server comparisons and design review. The signal and allocation-failure limitations below are unresolved correctness blockers.

Ordinary builds leave `ENABLE_PROFILE_EVENTS_PAGED_EXPERIMENT=OFF`. Paged storage, dispatch checks, and destruction changes are excluded by preprocessing; the original dense storage path and `Counters` layout remain. The named nonallocating-event API is available in both builds. Enabled builds are restricted to Linux x86-64/AArch64 and require lock-free 64-bit atomics. No census instrumentation is required.

## Build and select a treatment

Configure with `-DENABLE_PROFILE_EVENTS_PAGED_EXPERIMENT=ON`, then build the usual `clickhouse` target. The enabled binary uses the original dense representation by default. Environment variables are read once, before the first process-counter construction publishes its storage:

| Variable | Meaning |
| --- | --- |
| `CH_COUNTER_STORAGE` | `dense` or `paged`; default `dense` |
| `CH_COUNTER_LAYOUT` | Absolute path containing a complete permutation of the 1562 global numeric event IDs; required for `paged` |
| `CH_COUNTER_HOT` | Total hot count, including mandatory events; minimum 40 for paged storage, default 128 |
| `CH_COUNTER_PAGE` | Cold-page cells; 8, 16, 32, 64, or 128; default 32 |
| `CH_COUNTER_DIAGNOSTICS` | Optional absolute path for a new, exclusive diagnostics file; leave unset for timing runs |

The event catalogue is deliberately pinned for this draft. An enabled-build assertion checks its length. Before publishing compact storage, configuration resolves the named events in `ProfileEventsNonAllocatingEventList.h` and moves them ahead of the frequency rank, preserving relative order within each group. It rejects a hot budget smaller than the mandatory set. Numeric event IDs are not hardcoded in the reservation list.

The initial audited set contains **40 events**: 5 signal events, 5 allocator events, 7 CPU scheduler events, 6 memory-reservation events, 6 IO resource-guard events and 11 other literal timer events. This is an initial subset; dynamic publishers and the broader `ProfileEventTimeIncrement` catalogue remain unaudited.

A public-only example input rank is included as `layout_calls128.txt` (SHA256 `ce68dd745c71a1f6de8e0aefcdb4865598f93358af3d0dc922b4851a8c3af652`). It was ranked from synthetic functional-test usage, with the original ten required events protected; it contains no customer/cloud data and is not claimed to be production-optimal. Configuration now normalizes this input against the 40-event catalogue. Record both the input rank and reservation catalogue when comparing results: the unchanged rank file produces a different effective layout.

`Counters::incrementNonAllocating` accepts a token created by `nonAllocatingEvent` from a catalogued named event. An unclassified event fails compilation when the factory is instantiated. Classification uses the external event object's identity, so another object with the same numeric ID does not qualify. The first migrated caller is the CPU scheduler's pending wait-time destructor. This does not prohibit ordinary `increment` under arbitrary mutexes.

The method retains parent propagation and tracing, is `noexcept`, and wraps the update in `DENY_ALLOCATIONS_IN_SCOPE`. This existing guard detects tracked allocation attempts in supported debug builds; it is disabled in Release and on macOS. An exception escaping the method terminates. Reserved backing is therefore the primary guarantee for these counters; the guard is a diagnostic, not allocation-failure recovery. A linked server run is still needed to validate the complete allocator/tracing path.

After building the enabled binary, start an owned test server with an explicit test configuration:

```sh
CH_COUNTER_LAYOUT="$PWD/utils/profile-events-paged-experiment/layout_calls128.txt" \
CH_COUNTER_STORAGE=paged CH_COUNTER_HOT=128 CH_COUNTER_PAGE=32 \
    build/programs/clickhouse server --config-file=/absolute/owned-test/config.xml
```

All process-level groups use the selected representation, including descendant and background groups. Thread, user, and global representations remain dense; parent propagation and event tracing retain their existing control flow. Zero amounts skip compact backing work only. An enabled dense control still pays experiment dispatch checks; it is not identical to an ordinary build and must be measured separately.

The hot array is allocated at construction. Cold pages are zero-initialized and published by release CAS. Readers acquire the page pointer; individual values use relaxed 64-bit atomic operations and preserve modular overflow. A losing page candidate is released. Reset atomically zeros retained storage without removing published pages. Snapshots still allocate a full dense destination using the original `Snapshot` constructor; this experiment does not compact query-log or protocol snapshot objects.

## Unresolved production blockers

- A first cold increment allocates through tracked aligned global `operator new`. Previously nonallocating increment paths can now throw. The mandatory catalogue covers the initial audited set only. Remaining timer destructors, `noexcept` callers, allocation failure, and allocator-hook reentrancy need a complete audit; the typed API does not make unmigrated callers safe.
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

It defaults to ASan/UBSan and stores unique build/per-case logs plus source/binary hashes. The tests cover dense/paged hot128 and all-hot1562, global-ID values, overflow, zero-update nonallocation, retained reset, modeled ownership transfer, concurrent first publication/snapshots, diagnostics, and explicit unsupported-path/configuration exits. Additional cases check stable reservation ordering, inverse mapping, unchanged rejection of invalid reservations, and reserved updates through two modeled owners before/after reset with aligned allocations forced to fail. A first cold-page allocation is forced to throw; the tests verify no publication, successful reserved updates afterward, and a successful explicit retry. The mandatory-event function uses an explicitly synthetic ten-event reservation stub in these standalone checks. They model the adapter's caller; they do not test actual `Counters` moves, real signal delivery, server allocator hooks, or the full server. An enabled full-server build and profiler-enabled functional/stress validation remain necessary before drawing integration conclusions.


To check the real public catalogue and bundled rank, add `--actual-catalogue`:

```
python3 utils/profile-events-paged-experiment/validate_adapter.py \
    --repo /absolute/checkout --output /absolute/new-build-directory --actual-catalogue
```

This mode resolves the pinned 1562 builtin IDs from `ProfileEvents.cpp`, checks that the 40-event mandatory catalogue includes all seven CPU scheduler events plus the memory-reservation and IO-resource event sets, and generates the adapter's reservation fixture from those IDs. Unsupported catalogue formats, missing names and malformed ranks fail validation. It checks the shipped layout at hot budgets 40 and 128, including normalized order, reserved updates with aligned allocations forced to fail, and cold-allocation failure/retry; budget 39 is rejected. The default mode retains its synthetic ten-event fixture. Both modes exercise the actual adapter with modeled owners; neither links the production `Counters`, scheduler timers or allocator hooks. The generated fixture and source hashes are recorded in the receipt.
