#include <memory>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <random>
#include <iostream>
#include <iomanip>
#include <thread>
#include <atomic>

#include <dlfcn.h>

#include <pcg_random.hpp>
#include <Common/thread_local_rng.h>

#include <common/defines.h>

#include <Common/Stopwatch.h>

#include <emmintrin.h>
#include <immintrin.h>

#include <boost/program_options.hpp>
#include <fmt/format.h>

#include <cpuid.h>


template <typename F, typename MemcpyImpl>
void NO_INLINE loop(uint8_t * dst, uint8_t * src, size_t size, F && chunk_size_distribution, MemcpyImpl && impl)
{
    while (size)
    {
        size_t bytes_to_copy = std::min<size_t>(size, chunk_size_distribution());

        impl(dst, src, bytes_to_copy);

        dst += bytes_to_copy;
        src += bytes_to_copy;
        size -= bytes_to_copy;

        /// Execute at least one SSE instruction as a penalty after running AVX code.
        __asm__ __volatile__ ("pxor %%xmm15, %%xmm15" ::: "xmm15");
    }
}


using RNG = pcg32_fast;

template <size_t N>
size_t generatorUniform(RNG & rng) { return rng() % N; };


template <typename F, typename MemcpyImpl>
uint64_t test(uint8_t * dst, uint8_t * src, size_t size, size_t iterations, size_t num_threads, F && generator, MemcpyImpl && impl, const char * name)
{
    Stopwatch watch;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (size_t thread_num = 0; thread_num < num_threads; ++thread_num)
    {
        size_t begin = size * thread_num / num_threads;
        size_t end = size * (thread_num + 1) / num_threads;

        threads.emplace_back([begin, end, iterations, &src, &dst, &generator, &impl]
        {
            for (size_t iteration = 0; iteration < iterations; ++iteration)
            {
                loop(
                    iteration % 2 ? &src[begin] : &dst[begin],
                    iteration % 2 ? &dst[begin] : &src[begin],
                    end - begin,
                    [rng = RNG(), &generator]() mutable { return generator(rng); },
                    std::forward<MemcpyImpl>(impl));
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    uint64_t elapsed_ns = watch.elapsed();

    /// Validation
    for (size_t i = 0; i < size; ++i)
        if (dst[i] != uint8_t(i))
            throw std::logic_error("Incorrect result");

    std::cout << name;
    return elapsed_ns;
}


using memcpy_type = void * (*)(void * __restrict, const void * __restrict, size_t);


static void * memcpy_erms(void * dst, const void * src, size_t size)
{
    asm volatile (
        "rep movsb"
        : "=D"(dst), "=S"(src), "=c"(size)
        : "0"(dst), "1"(src), "2"(size)
        : "memory");
    return dst;
}

static void * memcpy_trivial(void * __restrict dst_, const void * __restrict src_, size_t size)
{
    char * __restrict dst = reinterpret_cast<char * __restrict>(dst_);
    const char * __restrict src = reinterpret_cast<const char * __restrict>(src_);
    void * ret = dst;

    while (size > 0)
    {
        *dst = *src;
        ++dst;
        ++src;
        --size;
    }

    return ret;
}

extern "C" void * memcpy_jart(void * dst, const void * src, size_t size);
extern "C" void * MemCpy(void * dst, const void * src, size_t size);

void * memcpy_fast_sse(void * dst, const void * src, size_t size);
void * memcpy_fast_avx(void * dst, const void * src, size_t size);
void * memcpy_tiny(void * dst, const void * src, size_t size);

extern "C" void * __memcpy_erms(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_sse2_unaligned(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_ssse3(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_ssse3_back(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_avx_unaligned(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_avx_unaligned_erms(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_avx512_unaligned(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_avx512_unaligned_erms(void * __restrict destination, const void * __restrict source, size_t size);
extern "C" void * __memcpy_avx512_no_vzeroupper(void * __restrict destination, const void * __restrict source, size_t size);


#include "memcpy_medium_every_unroll_vec_type.inl.h"


static ALWAYS_INLINE UInt32 rdtsc()
{
    UInt32 low;
    UInt32 high;
    __asm__ __volatile__ ("rdtsc" : "=a"(low), "=d"(high));
    return low;
}


struct VariantWithStatistics
{
    std::atomic<memcpy_type> func{nullptr};
    std::atomic<size_t> time = 0;
    std::atomic<size_t> bytes = 0;
    std::atomic<size_t> count = 0;
    std::atomic<size_t> kind = 0;

    /// Less is better.
    double score() const
    {
        double loaded_time = time.load(std::memory_order_relaxed);
        double loaded_bytes = bytes.load(std::memory_order_relaxed);
        double loaded_count = count.load(std::memory_order_relaxed);

        double mean = loaded_time / loaded_bytes;
        double sigma = mean / sqrt(loaded_count);

        return mean + sigma;
    }

    bool operator< (const VariantWithStatistics & rhs) const
    {
        return score() < rhs.score();
    }

    void smooth()
    {
        time.store(1 + time.load(std::memory_order_relaxed) / 2, std::memory_order_relaxed);
        bytes.store(1 + bytes.load(std::memory_order_relaxed) / 2, std::memory_order_relaxed);
        count.store(1 + count.load(std::memory_order_relaxed) / 2, std::memory_order_relaxed);
    }

    constexpr VariantWithStatistics(size_t kind_, memcpy_type func_) : func(func_), kind(kind_) {}
    constexpr VariantWithStatistics() : func(memcpy), kind(0) {}

    void set(size_t kind_, memcpy_type func_)
    {
        func.store(func_, std::memory_order_relaxed);
        kind.store(kind_, std::memory_order_relaxed);
    }
};

/// NOTE: I would like to use Thompson Sampling, but it's too heavy.

struct MultipleVariantsWithStatistics
{
    std::atomic<memcpy_type> selected_variant{memcpy};

    static constexpr size_t num_cells = 256;

    VariantWithStatistics variants[num_cells]{};

    std::atomic<size_t> count = 0;
    std::atomic<size_t> exploration_count = 0;

    static constexpr size_t probability_distribution_buckets = 256;
    std::atomic<size_t> exploration_probability_threshold = 0;

    static constexpr size_t num_explorations_before_optimize = 256;

    MultipleVariantsWithStatistics()
    {
        init();
    }


    ALWAYS_INLINE void * call(void * __restrict dst, const void * __restrict src, size_t size)
    {
        size_t current_count = count++;

        if (likely(current_count % probability_distribution_buckets < exploration_probability_threshold))
        {
            /// Exploitation mode.
            return selected_variant.load(std::memory_order_relaxed)(dst, src, size);
        }
        else
        {
            /// Exploration mode.
            return explore(dst, src, size);
        }
    }

    void * explore(void * __restrict dst, const void * __restrict src, size_t size)
    {
        size_t current_exploration_count = exploration_count++;

        size_t hash = current_exploration_count;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;

        VariantWithStatistics & variant = variants[hash % num_cells];

        UInt32 time1 = rdtsc();
        void * ret = variant.func.load(std::memory_order_relaxed)(dst, src, size);
        UInt32 time2 = rdtsc();
        UInt32 time = time2 - time1;

        if (time < size)
        {
            ++variant.count;
            variant.bytes += size;
            variant.time += time;
        }

        if (current_exploration_count == num_explorations_before_optimize)
        {
            double exploration_probability = 1.0 - double(exploration_probability_threshold.load(std::memory_order_relaxed))
                / probability_distribution_buckets;

            exploration_probability /= 1.5;

            exploration_probability_threshold.store(
                std::min<size_t>(probability_distribution_buckets - 1,
                    probability_distribution_buckets * (1.0 - exploration_probability)),
                std::memory_order_relaxed);

            exploration_count.store(0, std::memory_order_relaxed);

            size_t best_variant = 0;
            double best_score = 1e9;

            for (size_t i = 0; i < num_cells; ++i)
            {
                double score = variants[i].score();
                variants[i].smooth();

                if (score < best_score)
                {
                    best_score = score;
                    best_variant = i;
                }
            }
            selected_variant.store(variants[best_variant].func.load(std::memory_order_relaxed));

            std::cerr << variants[best_variant].kind << " ";
        }

        return ret;
    }

    void init()
    {
        UInt32 eax;
        UInt32 ebx;
        UInt32 ecx;
        UInt32 edx;

        __cpuid(1, eax, ebx, ecx, edx);
        bool have_avx = ecx & bit_AVX;

        bool have_avx512 = false;
        if (have_avx)
        {
            __cpuid(0, eax, ebx, ecx, edx);
            bool have_leaf7 = eax >= 7;

            if (have_leaf7)
            {
                __cpuid(7, eax, ebx, ecx, edx);
                have_avx512 = ebx & bit_AVX512F;
            }
        }

        size_t idx = 0;

        variants[idx].set(idx, memcpy_erms);

        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled1_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled2_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled3_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled4_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled5_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled6_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled7_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled8_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled9_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled10_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled11_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled12_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled13_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled14_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled15_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled16_sse);

        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled1_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled2_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled3_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled4_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled5_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled6_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled7_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled8_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled9_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled10_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled11_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled12_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled13_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled14_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled15_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled16_sse);

        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled1_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled2_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled3_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled4_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled5_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled6_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled7_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled8_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled9_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled10_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled11_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled12_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled13_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled14_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled15_sse);
        ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled16_sse);

        if (have_avx)
        {
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled1_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled2_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled3_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled4_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled5_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled6_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled7_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled8_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled9_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled10_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled11_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled12_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled13_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled14_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled15_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled16_avx);

            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled1_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled2_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled3_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled4_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled5_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled6_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled7_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled8_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled9_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled10_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled11_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled12_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled13_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled14_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled15_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled16_avx);

            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled1_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled2_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled3_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled4_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled5_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled6_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled7_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled8_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled9_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled10_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled11_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled12_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled13_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled14_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled15_avx);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled16_avx);
        }

        if (have_avx512)
        {
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled1_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled2_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled3_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled4_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled5_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled6_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled7_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled8_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled9_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled10_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled11_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled12_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled13_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled14_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled15_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_forward_unrolled16_avx512);

            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled1_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled2_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled3_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled4_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled5_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled6_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled7_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled8_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled9_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled10_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled11_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled12_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled13_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled14_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled15_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_backward_unrolled16_avx512);

            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled1_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled2_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled3_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled4_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled5_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled6_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled7_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled8_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled9_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled10_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled11_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled12_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled13_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled14_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled15_avx512);
            ++idx; variants[idx].set(idx, memcpy_medium_twoway_unrolled16_avx512);
        }

        ++idx;
        size_t num_variants = idx;
        while (idx < num_cells)
        {
            size_t copy_from = idx % num_variants;
            variants[idx].set(copy_from, variants[copy_from].func.load(std::memory_order_relaxed));
            ++idx;
        }
    }
};



MultipleVariantsWithStatistics variants;

static uint8_t * memcpy_selftuned(uint8_t * __restrict dst, const uint8_t * __restrict src, size_t size)
{
    uint8_t * ret = dst;

    if (size <= 16)
    {
        if (size >= 8)
        {
            __builtin_memcpy(dst + size - 8, src + size - 8, 8);
            __builtin_memcpy(dst, src, 8);
        }
        else if (size >= 4)
        {
            __builtin_memcpy(dst + size - 4, src + size - 4, 4);
            __builtin_memcpy(dst, src, 4);
        }
        else if (size >= 2)
        {
            __builtin_memcpy(dst + size - 2, src + size - 2, 2);
            __builtin_memcpy(dst, src, 2);
        }
        else if (size >= 1)
        {
            *dst = *src;
        }
    }
    else if (size <= 128)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

        while (size > 16)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
            dst += 16;
            src += 16;
            size -= 16;
        }
    }
    else if (size < 30000)
    {
        /// Align destination to 16 bytes boundary.
        size_t padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

        if (padding > 0)
        {
            __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
            dst += padding;
            src += padding;
            size -= padding;
        }

        /// Aligned unrolled copy.
        __m128i c0, c1, c2, c3, c4, c5, c6, c7;

        while (size >= 128)
        {
            c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 0);
            c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 1);
            c2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 2);
            c3 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 3);
            c4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 4);
            c5 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 5);
            c6 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 6);
            c7 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 7);
            src += 128;
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 0), c0);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 1), c1);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 2), c2);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 3), c3);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 4), c4);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 5), c5);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 6), c6);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 7), c7);
            dst += 128;

            size -= 128;
        }

        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

        while (size > 16)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
            dst += 16;
            src += 16;
            size -= 16;
        }
    }
    else
    {
        return static_cast<uint8_t*>(variants.call(dst, src, size));
    }

    return ret;
}

template <memcpy_type impl>
static uint8_t * memcpy_medium(uint8_t * __restrict dst, const uint8_t * __restrict src, size_t size)
{
    uint8_t * ret = dst;

    if (size <= 16)
    {
        if (size >= 8)
        {
            __builtin_memcpy(dst + size - 8, src + size - 8, 8);
            __builtin_memcpy(dst, src, 8);
        }
        else if (size >= 4)
        {
            __builtin_memcpy(dst + size - 4, src + size - 4, 4);
            __builtin_memcpy(dst, src, 4);
        }
        else if (size >= 2)
        {
            __builtin_memcpy(dst + size - 2, src + size - 2, 2);
            __builtin_memcpy(dst, src, 2);
        }
        else if (size >= 1)
        {
            *dst = *src;
        }
    }
    else if (size <= 128)
    {
        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

        while (size > 16)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
            dst += 16;
            src += 16;
            size -= 16;
        }
    }
    else if (size < 30000)
    {
        /// Align destination to 16 bytes boundary.
        size_t padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

        if (padding > 0)
        {
            __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
            _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
            dst += padding;
            src += padding;
            size -= padding;
        }

        /// Aligned unrolled copy.
        __m128i c0, c1, c2, c3, c4, c5, c6, c7;

        while (size >= 128)
        {
            c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 0);
            c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 1);
            c2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 2);
            c3 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 3);
            c4 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 4);
            c5 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 5);
            c6 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 6);
            c7 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 7);
            src += 128;
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 0), c0);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 1), c1);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 2), c2);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 3), c3);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 4), c4);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 5), c5);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 6), c6);
            _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 7), c7);
            dst += 128;

            size -= 128;
        }

        _mm_storeu_si128(reinterpret_cast<__m128i *>(dst + size - 16), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src + size - 16)));

        while (size > 16)
        {
            _mm_storeu_si128(reinterpret_cast<__m128i *>(dst), _mm_loadu_si128(reinterpret_cast<const __m128i *>(src)));
            dst += 16;
            src += 16;
            size -= 16;
        }
    }
    else
    {
        return static_cast<uint8_t*>(impl(dst, src, size));
    }

    return ret;
}


#define VARIANT(N, NAME) \
    if (memcpy_variant == N) \
        return test(dst, src, size, iterations, num_threads, std::forward<F>(generator), NAME, #NAME);

template <typename F>
uint64_t dispatchMemcpyVariants(size_t memcpy_variant, uint8_t * dst, uint8_t * src, size_t size, size_t iterations, size_t num_threads, F && generator)
{
    memcpy_type memcpy_libc_old = reinterpret_cast<memcpy_type>(dlsym(RTLD_NEXT, "memcpy"));

    VARIANT(1, memcpy)
    VARIANT(2, memcpy_trivial)
    VARIANT(3, memcpy_libc_old)
    VARIANT(4, memcpy_erms)
    VARIANT(5, MemCpy)
    VARIANT(10, memcpy_fast_sse)
    VARIANT(11, memcpy_fast_avx)

    VARIANT(21, __memcpy_erms)
    VARIANT(22, __memcpy_sse2_unaligned)
    VARIANT(23, __memcpy_ssse3)
    VARIANT(24, __memcpy_ssse3_back)
    VARIANT(25, __memcpy_avx_unaligned)
    VARIANT(26, __memcpy_avx_unaligned_erms)
    VARIANT(27, __memcpy_avx512_unaligned)
    VARIANT(28, __memcpy_avx512_unaligned_erms)
    VARIANT(29, __memcpy_avx512_no_vzeroupper)

    VARIANT(30, memcpy_selftuned)

    VARIANT(101, memcpy_medium<memcpy_medium_twoway_unrolled1_avx>)
    VARIANT(102, memcpy_medium<memcpy_medium_twoway_unrolled2_avx>)
    VARIANT(103, memcpy_medium<memcpy_medium_twoway_unrolled3_avx>)
    VARIANT(104, memcpy_medium<memcpy_medium_twoway_unrolled4_avx>)
    VARIANT(105, memcpy_medium<memcpy_medium_twoway_unrolled5_avx>)
    VARIANT(106, memcpy_medium<memcpy_medium_twoway_unrolled6_avx>)
    VARIANT(107, memcpy_medium<memcpy_medium_twoway_unrolled7_avx>)
    VARIANT(108, memcpy_medium<memcpy_medium_twoway_unrolled8_avx>)
    VARIANT(109, memcpy_medium<memcpy_medium_twoway_unrolled9_avx>)
    VARIANT(110, memcpy_medium<memcpy_medium_twoway_unrolled10_avx>)
    VARIANT(111, memcpy_medium<memcpy_medium_twoway_unrolled11_avx>)
    VARIANT(112, memcpy_medium<memcpy_medium_twoway_unrolled12_avx>)
    VARIANT(113, memcpy_medium<memcpy_medium_twoway_unrolled13_avx>)
    VARIANT(114, memcpy_medium<memcpy_medium_twoway_unrolled14_avx>)
    VARIANT(115, memcpy_medium<memcpy_medium_twoway_unrolled15_avx>)
    VARIANT(116, memcpy_medium<memcpy_medium_twoway_unrolled16_avx>)

    return 0;
}

uint64_t dispatchVariants(
    size_t memcpy_variant, size_t generator_variant, uint8_t * dst, uint8_t * src, size_t size, size_t iterations, size_t num_threads)
{
    if (generator_variant == 1)
        return dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<16>);
    if (generator_variant == 2)
        return dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<256>);
    if (generator_variant == 3)
        return dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<4096>);
    if (generator_variant == 4)
        return dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<65536>);
    if (generator_variant == 5)
        return dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<1048576>);

    return 0;
}


int main(int argc, char ** argv)
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message")
        ("size", boost::program_options::value<size_t>()->default_value(1000000), "Bytes to copy on every iteration")
        ("iterations", boost::program_options::value<size_t>(), "Number of iterations")
        ("threads", boost::program_options::value<size_t>()->default_value(1), "Number of copying threads")
        ("distribution", boost::program_options::value<size_t>()->default_value(4), "Distribution of chunk sizes to perform copy")
        ("variant", boost::program_options::value<size_t>(), "Variant of memcpy implementation")
        ("tsv", "Print result in tab-separated format")
        ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") || !options.count("variant"))
    {
        std::cout << R"(Usage:

for size in 4096 16384 50000 65536 100000 1000000 10000000 100000000; do
    for threads in 1 2 4 $(($(nproc) / 2)) $(nproc); do
        for distribution in 1 2 3 4 5; do
            for variant in {1..13} {21..29}; do
                for i in {1..10}; do
                    ./memcpy-bench --tsv --size $size --variant $variant --threads $threads --distribution $distribution;
                done;
            done;
        done;
    done;
done | tee result.tsv

clickhouse-local --structure '
    name String,
    size UInt64,
    iterations UInt64,
    threads UInt16,
    generator UInt8,
    memcpy UInt8,
    elapsed UInt64
' --query "
    SELECT
        size, name,
        avg(1000 * elapsed / size / iterations) AS s,
        count() AS c
    FROM table
    GROUP BY size, name
    ORDER BY size ASC, s DESC
" --output-format PrettyCompact < result.tsv

)" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    size_t size = options["size"].as<size_t>();
    size_t num_threads = options["threads"].as<size_t>();
    size_t memcpy_variant = options["variant"].as<size_t>();
    size_t generator_variant = options["distribution"].as<size_t>();

    size_t iterations;
    if (options.count("iterations"))
    {
        iterations = options["iterations"].as<size_t>();
    }
    else
    {
        iterations = 10000000000ULL / size;

        if (generator_variant == 1)
            iterations /= 10;
    }

    std::unique_ptr<uint8_t[]> src(new uint8_t[size]);
    std::unique_ptr<uint8_t[]> dst(new uint8_t[size]);

    /// Fill src with some pattern for validation.
    for (size_t i = 0; i < size; ++i)
        src[i] = i;

    /// Fill dst to avoid page faults.
    memset(dst.get(), 0, size);

    uint64_t elapsed_ns = dispatchVariants(memcpy_variant, generator_variant, dst.get(), src.get(), size, iterations, num_threads);

    std::cout << std::fixed << std::setprecision(3);

    if (options.count("tsv"))
    {
        std::cout
            << '\t' << size
            << '\t' << iterations
            << '\t' << num_threads
            << '\t' << generator_variant
            << '\t' << memcpy_variant
            << '\t' << elapsed_ns
            << '\n';
    }
    else
    {
        std::cout << ": " << num_threads << " threads, " << "size: " << size << ", distribution " << generator_variant
            << ", processed in " << (elapsed_ns / 1e9) << " sec, " << (size * iterations * 1.0 / elapsed_ns) << " GB/sec\n";
    }

    return 0;
}
