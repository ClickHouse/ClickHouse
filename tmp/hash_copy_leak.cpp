// Reproducer: libc++ std::unordered_map (__hash_table) copy CONSTRUCTOR leaks already-linked
// nodes if a node allocation throws partway through the copy.
//
// __hash_table(const __hash_table&) calls __copy_construct(), which allocates nodes one by one
// and links them onto __first_node_.__next_ with NO scope guard. If a later node allocation
// throws, the __hash_table constructor itself throws, so ~__hash_table never runs and the
// already-linked nodes leak. (The erase/rehash/assign paths DO guard the partial chain with
// std::__make_scope_guard([&]{ __deallocate_node_list(__cache); }); the copy ctor does not.)
//
// Copy ASSIGNMENT into an already-constructed map does not leak: the destination is a fully
// constructed object whose destructor frees its nodes during unwind.
//
// Build:
//   clang++-21 -std=c++23 -stdlib=libc++ -fsanitize=address -g -O0 hash_copy_leak.cpp -o hash_copy_leak
// Run:
//   ASAN_OPTIONS=detect_leaks=1 ./hash_copy_leak <mode>     # mode = ctor | assign

#include <unordered_map>
#include <string>
#include <new>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace
{

/// Throws std::bad_alloc on the Nth allocation to emulate MEMORY_LIMIT_EXCEEDED landing inside
/// the hash-table copy. -1 disables the fault.
int alloc_countdown = -1;

template <class T>
struct FaultyAllocator
{
    using value_type = T;

    FaultyAllocator() = default;
    template <class U>
    FaultyAllocator(const FaultyAllocator<U> &) noexcept {}

    T * allocate(std::size_t n)
    {
        if (alloc_countdown == 0)
        {
            std::fprintf(stderr, "[fault fired]\n");
            throw std::bad_alloc();
        }
        if (alloc_countdown > 0)
            --alloc_countdown;
        return static_cast<T *>(::malloc(n * sizeof(T)));
    }

    void deallocate(T * p, std::size_t) noexcept { ::free(p); }

    template <class U>
    bool operator==(const FaultyAllocator<U> &) const noexcept { return true; }
    template <class U>
    bool operator!=(const FaultyAllocator<U> &) const noexcept { return false; }
};

using Map = std::unordered_map<
    std::string,
    std::size_t,
    std::hash<std::string>,
    std::equal_to<>,
    FaultyAllocator<std::pair<const std::string, std::size_t>>>;

Map makeSource()
{
    Map src;
    for (int i = 0; i < 8; ++i)
        /// Long names force a separate heap allocation per string, mirroring real column names.
        src.emplace(std::string("this_is_a_fairly_long_column_name_number_") + std::to_string(i), i);
    return src;
}

}

int main(int argc, char ** argv)
{
    const char * mode = argc > 1 ? argv[1] : "ctor";
    const int countdown = argc > 2 ? std::atoi(argv[2]) : 5;

    Map src = makeSource();
    bool threw = false;

    if (std::strcmp(mode, "ctor") == 0)
    {
        /// BUGGY path: copy-construction. Let the bucket array + a few nodes succeed, then throw.
        try
        {
            alloc_countdown = countdown;
            Map copy(src); /// throws midway -> linked nodes leak
            (void)copy;
        }
        catch (...) { threw = true; }
        alloc_countdown = -1;
    }
    else
    {
        /// FIXED path (what DB::Block's copy ctor now does): assign into a constructed map.
        try
        {
            Map dst;
            alloc_countdown = countdown;
            dst = src; /// throws midway -> dst's destructor frees partial state, no leak
            alloc_countdown = -1;
            (void)dst;
        }
        catch (...) { threw = true; }
        alloc_countdown = -1;
    }

    std::fprintf(stderr, "[mode=%s countdown=%d threw=%s]\n", mode, countdown, threw ? "yes" : "no");

    return 0;
}
