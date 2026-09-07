#include "ProfileEventsPagedExperiment/adapter.h"

#include <barrier>
#include <cstdio>
#include <limits>
#include <string_view>
#include <thread>
#include <vector>

namespace Experiment = ProfileEvents::PagedExperiment;
using ProfileEvents::PagedExperimentStorage::Event;
using ProfileEvents::PagedExperimentStorage::EventCount;
using Count = uint64_t;

/// Standalone-only synthetic reservation IDs; production resolves the named global events.
std::array<uint16_t, 10> ProfileEvents::PagedExperiment::requiredHotEvents() noexcept
{
    return {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
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

int main(int argc, char ** argv)
{
    const std::string_view test = argc == 2 ? argv[1] : "normal";
    const auto & config = Experiment::configuration();
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
