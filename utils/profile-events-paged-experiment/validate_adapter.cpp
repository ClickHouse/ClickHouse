#include <Common/ProfileEventsPagedExperiment/adapter.h>

#include <barrier>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <string_view>
#include <thread>
#include <vector>

/// Test-only interception of the aligned allocation API used by the adapter arrays.
/// This does not model production allocator hooks, tracking, or failure reporting.
namespace AllocationFailureTest
{
std::atomic<size_t> attempts{0};
std::atomic<bool> fail{false};
}

void * operator new(size_t size, std::align_val_t alignment)
{
    AllocationFailureTest::attempts.fetch_add(1, std::memory_order_relaxed);
    if (AllocationFailureTest::fail.load(std::memory_order_relaxed))
        throw std::bad_alloc();
    void * pointer = nullptr;
    if (posix_memalign(&pointer, static_cast<size_t>(alignment), size ? size : 1))
        throw std::bad_alloc();
    return pointer;
}

void operator delete(void * pointer, std::align_val_t) noexcept
{
    std::free(pointer);
}

namespace Experiment = ProfileEvents::PagedExperiment;
using ProfileEvents::PagedExperimentStorage::Event;
using ProfileEvents::PagedExperimentStorage::EventCount;
using Count = uint64_t;

/// Standalone-only synthetic reservation IDs; production resolves the named global events.
std::span<const uint16_t> ProfileEvents::PagedExperiment::requiredHotEvents() noexcept
{
    static constexpr std::array<uint16_t, 10> events{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    return events;
}


void require(bool condition, const char * message)
{
    if (!condition)
    {
        std::fprintf(stderr, "FAIL: %s\n", message);
        std::abort();
    }
}

/// Models the adapter's caller ownership only; this is not the production Counters class.
struct Owner
{
    ProfileEvents::PagedExperimentStorage::Array<Count> dense;
    Count * pointer = nullptr;

    Owner()
    {
        if (Experiment::configuration().mode == Experiment::Mode::Dense)
        {
            dense = ProfileEvents::PagedExperimentStorage::Array<Count>(EventCount);
            pointer = dense.data();
        }
        else
            pointer = Experiment::create();
        Experiment::constructed(pointer, sizeof(Owner));
    }

    Owner(Owner && other) noexcept
        : dense(std::move(other.dense)), pointer(std::exchange(other.pointer, nullptr))
    {
    }
    Owner(const Owner &) = delete;
    Owner & operator=(const Owner &) = delete;

    ~Owner()
    {
        if (!pointer)
            return;
        Experiment::destroyed(pointer);
        if (Experiment::kind(pointer) != Experiment::Mode::Dense)
            Experiment::destroy(std::exchange(pointer, nullptr));
    }

    void add(Event event, Count amount)
    {
        if (Experiment::kind(pointer) == Experiment::Mode::Dense)
            std::atomic_ref<Count>(pointer[event]).fetch_add(amount, std::memory_order_relaxed);
        else
            Experiment::add(pointer, event, amount);
    }

    void signal(Event event, Count amount)
    {
        if (Experiment::kind(pointer) == Experiment::Mode::Dense)
            add(event, amount);
        else
            Experiment::addSignalSafe(pointer, event, amount);
    }

    Count load(Event event) const
    {
        if (Experiment::kind(pointer) == Experiment::Mode::Dense)
            return std::atomic_ref<Count>(pointer[event]).load(std::memory_order_relaxed);
        return Experiment::load(pointer, event);
    }

    void snapshot(Count * output) const
    {
        if (Experiment::kind(pointer) == Experiment::Mode::Dense)
        {
            for (size_t event = 0; event < EventCount; ++event)
                output[event] = load(static_cast<Event>(event));
        }
        else
            Experiment::snapshot(pointer, output);
    }

    void reset()
    {
        if (Experiment::kind(pointer) == Experiment::Mode::Dense)
        {
            for (size_t event = 0; event < EventCount; ++event)
                std::atomic_ref<Count>(pointer[event]).store(0, std::memory_order_relaxed);
        }
        else
            Experiment::reset(pointer);
    }
};

bool sameMemory(const ProfileEvents::PagedExperimentStorage::MemoryUsage & a, const ProfileEvents::PagedExperimentStorage::MemoryUsage & b)
{
    return a.requested == b.requested && a.usable == b.usable && a.allocations == b.allocations;
}

void testReservations()
{
    using ProfileEvents::PagedExperimentStorage::Layout;
    std::vector<Event> rank;
    for (size_t i = EventCount; i > 0; --i)
        rank.push_back(static_cast<Event>(i - 1));
    const std::array<Event, 3> required{1, static_cast<Event>(EventCount - 1), 7};
    Layout layout(128, rank);
    require(layout.reserveHotEvents(required), "valid reservation rejected");
    std::vector<Event> expected;
    for (bool selected : {true, false})
        for (const Event event : rank)
            if ((std::find(required.begin(), required.end(), event) != required.end()) == selected)
                expected.push_back(event);
    for (size_t slot = 0; slot < EventCount; ++slot)
    {
        require(layout.event_at_slot[slot] == expected[slot], "reservation did not preserve rank order");
        require(layout.slot_of[expected[slot]] == slot, "reservation inverse differs");
    }
    const Layout reserved = layout;
    require(layout.reserveHotEvents(required), "repeat reservation rejected");
    require(layout.event_at_slot == reserved.event_at_slot && layout.slot_of == reserved.slot_of, "reservation not idempotent");
    require(layout.reserveHotEvents({}), "empty reservation rejected");
    require(layout.event_at_slot == reserved.event_at_slot && layout.slot_of == reserved.slot_of, "empty reservation changed rank");
    const auto reject = [](Layout & candidate, std::span<const Event> events)
    {
        const Layout before = candidate;
        require(!candidate.reserveHotEvents(events), "invalid reservation accepted");
        require(candidate.hot_count == before.hot_count && candidate.event_at_slot == before.event_at_slot
            && candidate.slot_of == before.slot_of, "invalid reservation mutated layout");
    };
    const std::array<Event, 2> duplicate{1, 1};
    const std::array<Event, 2> invalid{1, static_cast<Event>(EventCount)};
    reject(layout, duplicate);
    reject(layout, invalid);
    Layout insufficient(2, rank);
    reject(insufficient, required);
    Layout exact(required.size(), rank);
    require(exact.reserveHotEvents(required), "exact budget rejected");
    for (const Event event : required)
        require(exact.slot_of[event] < exact.hot_count, "reserved event outside hot budget");
    std::puts("PASS: stable reservation, full inverse, idempotence, empty/exact budget, unchanged rejection");
}

void testReservedUpdates(bool inject_cold_failure)
{
    /// Two already constructed owners model explicit parent propagation only.
    /// This is not the production parent traversal or reparenting protocol.
    Owner child;
    Owner parent;
    const auto child_empty = Experiment::backing(child.pointer);
    const auto parent_empty = Experiment::backing(parent.pointer);
    const auto attempts_before = AllocationFailureTest::attempts.load();
    AllocationFailureTest::fail.store(true);
    const auto update_reserved = [&]
    {
        for (const Event event : Experiment::requiredHotEvents())
        {
            child.add(event, 3);
            parent.add(event, 3);
            child.signal(event, 2);
            parent.signal(event, 2);
            require(child.load(event) == 5 && parent.load(event) == 5, "reserved modeled parent total differs");
        }
    };
    update_reserved();
    child.reset();
    parent.reset();
    update_reserved();
    require(AllocationFailureTest::attempts.load() == attempts_before, "reserved update/reset attempted aligned allocation");
    require(sameMemory(child_empty, Experiment::backing(child.pointer))
        && sameMemory(parent_empty, Experiment::backing(parent.pointer)), "reserved update allocated backing");
    if (inject_cold_failure)
    {
        const auto & config = Experiment::configuration();
        require(config.mode == Experiment::Mode::Paged && config.layout.hot_count < EventCount, "cold failure requires partial paged storage");
        const Event cold = config.layout.event_at_slot[config.layout.hot_count];
        bool observed = false;
        try
        {
            child.add(cold, 11);
        }
        catch (const std::bad_alloc &)
        {
            observed = true;
        }
        require(observed && AllocationFailureTest::attempts.load() == attempts_before + 1, "cold failure not observed exactly once");
        require(child.load(cold) == 0 && sameMemory(child_empty, Experiment::backing(child.pointer)), "failed cold allocation published state");
        child.reset();
        parent.reset();
        update_reserved();
        require(AllocationFailureTest::attempts.load() == attempts_before + 1, "reserved updates allocated after cold failure");
        AllocationFailureTest::fail.store(false);
        child.add(cold, 11);
        require(child.load(cold) == 11 && AllocationFailureTest::attempts.load() == attempts_before + 2, "cold guard did not unwind or retry failed");
    }
    AllocationFailureTest::fail.store(false);
    std::puts("PASS: reserved synthetic ten events, modeled parents, retained reset, aligned allocation attempts and optional cold failure");
}

int main(int argc, char ** argv)
{
    const std::string_view test = argc == 2 ? argv[1] : "normal";
    if (test == "reservations")
    {
        testReservations();
        return 0;
    }
    const auto & config = Experiment::configuration();
    if (test == "reserved_updates" || test == "allocation_failure")
    {
        testReservedUpdates(test == "allocation_failure");
        return 0;
    }
    if (test == "configuration")
        return 0;
    Owner original;
    Owner owner(std::move(original));
    require(original.pointer == nullptr, "moved owner retained pointer");
    const Event cold = config.layout.event_at_slot[config.layout.hot_count < EventCount ? config.layout.hot_count : 0];
    if (test == "cold_signal")
    {
        owner.signal(cold, 1);
        return 91;
    }
    if (test == "recursive_cold")
    {
        Experiment::ColdUpdateGuard guard;
        owner.add(cold, 1);
        return 92;
    }
    std::array<Count, EventCount> expected{};
    std::array<Count, EventCount> output{};
    const auto empty = Experiment::backing(owner.pointer);
    for (size_t event = 0; event < EventCount; ++event)
        owner.add(static_cast<Event>(event), 0);
    owner.signal(cold, 0);
    require(sameMemory(empty, Experiment::backing(owner.pointer)), "zero allocated backing");
    for (size_t event = 0; event < EventCount; ++event)
    {
        expected[event] = event * 17 + 3;
        owner.add(static_cast<Event>(event), expected[event]);
    }
    owner.add(cold, std::numeric_limits<Count>::max());
    expected[cold] += std::numeric_limits<Count>::max();
    for (const Event event : Experiment::requiredHotEvents())
    {
        owner.signal(event, 5);
        expected[event] += 5;
    }
    owner.snapshot(output.data());
    for (size_t event = 0; event < EventCount; ++event)
        require(output[event] == expected[event] && owner.load(static_cast<Event>(event)) == expected[event], "serial/overflow/signal mismatch");
    const auto populated = Experiment::backing(owner.pointer);
    owner.reset();
    require(sameMemory(populated, Experiment::backing(owner.pointer)), "reset changed ownership");
    owner.snapshot(output.data());
    for (Count value : output)
        require(value == 0, "reset left nonzero value");

    /// Fresh backing exercises concurrent first publication, not only existing-cell updates.
    {
        Owner concurrent;
        constexpr size_t Threads = 8;
        constexpr size_t Iterations = 12000;
        std::barrier start(Threads + 1);
        std::atomic<size_t> finished = 0;
        std::vector<std::thread> workers;
        for (size_t thread = 0; thread < Threads; ++thread)
        {
            workers.emplace_back([&, thread]
            {
                start.arrive_and_wait();
                for (size_t i = 0; i < Iterations; ++i)
                {
                    concurrent.add(static_cast<Event>((i + thread * 31) % EventCount), 1);
                    concurrent.add(cold, 1);
                    concurrent.signal(Experiment::requiredHotEvents()[thread % Experiment::requiredHotEvents().size()], 1);
                }
                finished.fetch_add(1, std::memory_order_release);
            });
        }
        start.arrive_and_wait();
        std::array<Count, EventCount> previous{};
        while (finished.load(std::memory_order_acquire) != Threads)
        {
            concurrent.snapshot(output.data());
            for (size_t event = 0; event < EventCount; ++event)
                require(output[event] >= previous[event], "concurrent snapshot decreased");
            previous = output;
        }
        for (auto & worker : workers)
            worker.join();
        expected.fill(0);
        for (size_t thread = 0; thread < Threads; ++thread)
        {
            for (size_t i = 0; i < Iterations; ++i)
            {
                ++expected[(i + thread * 31) % EventCount];
                ++expected[cold];
                ++expected[Experiment::requiredHotEvents()[thread % Experiment::requiredHotEvents().size()]];
            }
        }
        concurrent.snapshot(output.data());
        require(output == expected, "concurrent exact totals mismatch");
    }
    std::puts("PASS: serial all IDs, modulo overflow, protected signals, zero allocation, retained reset, owner move, concurrent publication and snapshots");
}
