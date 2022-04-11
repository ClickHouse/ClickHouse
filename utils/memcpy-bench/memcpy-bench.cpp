#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include <memory>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <random>
#include <iostream>
#include <iomanip>
#include <thread>

#include <dlfcn.h>

#include <pcg_random.hpp>

#include <base/defines.h>

#include <Common/Stopwatch.h>

#include <emmintrin.h>
#include <immintrin.h>

#include <boost/program_options.hpp>


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


using memcpy_type = void * (*)(const void * __restrict, void * __restrict, size_t);


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
extern "C" void MemCpy(void * dst, const void * src, size_t size);

void * memcpy_fast_sse(void * dst, const void * src, size_t size);
void * memcpy_fast_avx(void * dst, const void * src, size_t size);
void * memcpy_tiny(void * dst, const void * src, size_t size);


static void * memcpySSE2(void * __restrict destination, const void * __restrict source, size_t size)
{
    unsigned char *dst = reinterpret_cast<unsigned char *>(destination);
    const unsigned char *src = reinterpret_cast<const unsigned char *>(source);
    size_t padding;

    // small memory copy
    if (size <= 16)
        return memcpy_tiny(dst, src, size);

    // align destination to 16 bytes boundary
    padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

    if (padding > 0)
    {
        __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
        dst += padding;
        src += padding;
        size -= padding;
    }

    // medium size copy
    __m128i c0;

    for (; size >= 16; size -= 16)
    {
        c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
        src += 16;
        _mm_store_si128((reinterpret_cast<__m128i*>(dst)), c0);
        dst += 16;
    }

    memcpy_tiny(dst, src, size);
    return destination;
}

static void * memcpySSE2Unrolled2(void * __restrict destination, const void * __restrict source, size_t size)
{
    unsigned char *dst = reinterpret_cast<unsigned char *>(destination);
    const unsigned char *src = reinterpret_cast<const unsigned char *>(source);
    size_t padding;

    // small memory copy
    if (size <= 32)
        return memcpy_tiny(dst, src, size);

    // align destination to 16 bytes boundary
    padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

    if (padding > 0)
    {
        __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
        dst += padding;
        src += padding;
        size -= padding;
    }

    // medium size copy
    __m128i c0, c1;

    for (; size >= 32; size -= 32)
    {
        c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 0);
        c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 1);
        src += 32;
        _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 0), c0);
        _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 1), c1);
        dst += 32;
    }

    memcpy_tiny(dst, src, size);
    return destination;
}

static void * memcpySSE2Unrolled4(void * __restrict destination, const void * __restrict source, size_t size)
{
    unsigned char *dst = reinterpret_cast<unsigned char *>(destination);
    const unsigned char *src = reinterpret_cast<const unsigned char *>(source);
    size_t padding;

    // small memory copy
    if (size <= 64)
        return memcpy_tiny(dst, src, size);

    // align destination to 16 bytes boundary
    padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

    if (padding > 0)
    {
        __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
        dst += padding;
        src += padding;
        size -= padding;
    }

    // medium size copy
    __m128i c0, c1, c2, c3;

    for (; size >= 64; size -= 64)
    {
        c0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 0);
        c1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 1);
        c2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 2);
        c3 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src) + 3);
        src += 64;
        _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 0), c0);
        _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 1), c1);
        _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 2), c2);
        _mm_store_si128((reinterpret_cast<__m128i*>(dst) + 3), c3);
        dst += 64;
    }

    memcpy_tiny(dst, src, size);
    return destination;
}


static void * memcpySSE2Unrolled8(void * __restrict destination, const void * __restrict source, size_t size)
{
    unsigned char *dst = reinterpret_cast<unsigned char *>(destination);
    const unsigned char *src = reinterpret_cast<const unsigned char *>(source);
    size_t padding;

    // small memory copy
    if (size <= 128)
        return memcpy_tiny(dst, src, size);

    // align destination to 16 bytes boundary
    padding = (16 - (reinterpret_cast<size_t>(dst) & 15)) & 15;

    if (padding > 0)
    {
        __m128i head = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst), head);
        dst += padding;
        src += padding;
        size -= padding;
    }

    // medium size copy
    __m128i c0, c1, c2, c3, c4, c5, c6, c7;

    for (; size >= 128; size -= 128)
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
    }

    memcpy_tiny(dst, src, size);
    return destination;
}


//static __attribute__((__always_inline__, __target__("sse2")))
__attribute__((__always_inline__)) inline void
memcpy_my_medium_sse(uint8_t * __restrict & dst, const uint8_t * __restrict & src, size_t & size)
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
}

__attribute__((__target__("avx")))
void memcpy_my_medium_avx(uint8_t * __restrict & __restrict dst, const uint8_t * __restrict & __restrict src, size_t & __restrict size)
{
    size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

    if (padding > 0)
    {
        __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), head);
        dst += padding;
        src += padding;
        size -= padding;
    }

    __m256i c0, c1, c2, c3, c4, c5, c6, c7;

    while (size >= 256)
    {
        c0 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 0);
        c1 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 1);
        c2 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 2);
        c3 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 3);
        c4 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 4);
        c5 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 5);
        c6 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 6);
        c7 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 7);
        src += 256;
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 0), c0);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 1), c1);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 2), c2);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 3), c3);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 4), c4);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 5), c5);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 6), c6);
        _mm256_store_si256(((reinterpret_cast<__m256i*>(dst)) + 7), c7);
        dst += 256;

        size -= 256;
    }
}

bool have_avx = true;


static uint8_t * memcpy_my(uint8_t * __restrict dst, const uint8_t * __restrict src, size_t size)
{
    uint8_t * ret = dst;

tail:
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
    else if (have_avx)
    {
        if (size <= 32)
        {
            __builtin_memcpy(dst, src, 8);
            __builtin_memcpy(dst + 8, src + 8, 8);

            dst += 16;
            src += 16;
            size -= 16;

            goto tail;
        }

        if (size <= 256)
        {
            __asm__(
                "vmovups    -0x20(%[s],%[size],1), %%ymm0\n"
                "vmovups    %%ymm0, -0x20(%[d],%[size],1)\n"
                : [d]"+r"(dst), [s]"+r"(src)
                : [size]"r"(size)
                : "ymm0", "memory");

            while (size > 32)
            {
                __asm__(
                    "vmovups    (%[s]), %%ymm0\n"
                    "vmovups    %%ymm0, (%[d])\n"
                    : [d]"+r"(dst), [s]"+r"(src)
                    :
                    : "ymm0", "memory");

                dst += 32;
                src += 32;
                size -= 32;
            }
        }
        else
        {
            size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

            if (padding > 0)
            {
                __asm__(
                    "vmovups    (%[s]), %%ymm0\n"
                    "vmovups    %%ymm0, (%[d])\n"
                    : [d]"+r"(dst), [s]"+r"(src)
                    :
                    : "ymm0", "memory");

                dst += padding;
                src += padding;
                size -= padding;
            }

            while (size >= 256)
            {
                __asm__(
                    "vmovups    (%[s]), %%ymm0\n"
                    "vmovups    0x20(%[s]), %%ymm1\n"
                    "vmovups    0x40(%[s]), %%ymm2\n"
                    "vmovups    0x60(%[s]), %%ymm3\n"
                    "vmovups    0x80(%[s]), %%ymm4\n"
                    "vmovups    0xa0(%[s]), %%ymm5\n"
                    "vmovups    0xc0(%[s]), %%ymm6\n"
                    "vmovups    0xe0(%[s]), %%ymm7\n"
                    "add        $0x100,%[s]\n"
                    "vmovaps    %%ymm0, (%[d])\n"
                    "vmovaps    %%ymm1, 0x20(%[d])\n"
                    "vmovaps    %%ymm2, 0x40(%[d])\n"
                    "vmovaps    %%ymm3, 0x60(%[d])\n"
                    "vmovaps    %%ymm4, 0x80(%[d])\n"
                    "vmovaps    %%ymm5, 0xa0(%[d])\n"
                    "vmovaps    %%ymm6, 0xc0(%[d])\n"
                    "vmovaps    %%ymm7, 0xe0(%[d])\n"
                    "add        $0x100, %[d]\n"
                    : [d]"+r"(dst), [s]"+r"(src)
                    :
                    : "ymm0", "ymm1", "ymm2", "ymm3", "ymm4", "ymm5", "ymm6", "ymm7", "memory");

                size -= 256;
            }

            goto tail;
        }
    }
    else
    {
        if (size <= 128)
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
        else
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

            goto tail;
        }
    }

    return ret;
}


static uint8_t * memcpy_my2(uint8_t * __restrict dst, const uint8_t * __restrict src, size_t size)
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
    else if (size < 30000 || !have_avx)
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
        size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

        if (padding > 0)
        {
            __asm__(
                "vmovups    (%[s]), %%ymm0\n"
                "vmovups    %%ymm0, (%[d])\n"
                : [d]"+r"(dst), [s]"+r"(src)
                :
                : "ymm0", "memory");

            dst += padding;
            src += padding;
            size -= padding;
        }

        while (size >= 512) /// NOLINT
        {
            __asm__(
                "vmovups    (%[s]), %%ymm0\n"
                "vmovups    0x20(%[s]), %%ymm1\n"
                "vmovups    0x40(%[s]), %%ymm2\n"
                "vmovups    0x60(%[s]), %%ymm3\n"
                "vmovups    0x80(%[s]), %%ymm4\n"
                "vmovups    0xa0(%[s]), %%ymm5\n"
                "vmovups    0xc0(%[s]), %%ymm6\n"
                "vmovups    0xe0(%[s]), %%ymm7\n"
                "vmovups    0x100(%[s]), %%ymm8\n"
                "vmovups    0x120(%[s]), %%ymm9\n"
                "vmovups    0x140(%[s]), %%ymm10\n"
                "vmovups    0x160(%[s]), %%ymm11\n"
                "vmovups    0x180(%[s]), %%ymm12\n"
                "vmovups    0x1a0(%[s]), %%ymm13\n"
                "vmovups    0x1c0(%[s]), %%ymm14\n"
                "vmovups    0x1e0(%[s]), %%ymm15\n"
                "add        $0x200, %[s]\n"
                "sub        $0x200, %[size]\n"
                "vmovaps    %%ymm0, (%[d])\n"
                "vmovaps    %%ymm1, 0x20(%[d])\n"
                "vmovaps    %%ymm2, 0x40(%[d])\n"
                "vmovaps    %%ymm3, 0x60(%[d])\n"
                "vmovaps    %%ymm4, 0x80(%[d])\n"
                "vmovaps    %%ymm5, 0xa0(%[d])\n"
                "vmovaps    %%ymm6, 0xc0(%[d])\n"
                "vmovaps    %%ymm7, 0xe0(%[d])\n"
                "vmovaps    %%ymm8, 0x100(%[d])\n"
                "vmovaps    %%ymm9, 0x120(%[d])\n"
                "vmovaps    %%ymm10, 0x140(%[d])\n"
                "vmovaps    %%ymm11, 0x160(%[d])\n"
                "vmovaps    %%ymm12, 0x180(%[d])\n"
                "vmovaps    %%ymm13, 0x1a0(%[d])\n"
                "vmovaps    %%ymm14, 0x1c0(%[d])\n"
                "vmovaps    %%ymm15, 0x1e0(%[d])\n"
                "add        $0x200, %[d]\n"
                : [d]"+r"(dst), [s]"+r"(src), [size]"+r"(size)
                :
                : "ymm0", "ymm1", "ymm2", "ymm3", "ymm4", "ymm5", "ymm6", "ymm7",
                  "ymm8", "ymm9", "ymm10", "ymm11", "ymm12", "ymm13", "ymm14", "ymm15",
                  "memory");
        }

        /*while (size >= 256)
        {
            __asm__(
                "vmovups    (%[s]), %%ymm0\n"
                "vmovups    0x20(%[s]), %%ymm1\n"
                "vmovups    0x40(%[s]), %%ymm2\n"
                "vmovups    0x60(%[s]), %%ymm3\n"
                "vmovups    0x80(%[s]), %%ymm4\n"
                "vmovups    0xa0(%[s]), %%ymm5\n"
                "vmovups    0xc0(%[s]), %%ymm6\n"
                "vmovups    0xe0(%[s]), %%ymm7\n"
                "add        $0x100,%[s]\n"
                "vmovaps    %%ymm0, (%[d])\n"
                "vmovaps    %%ymm1, 0x20(%[d])\n"
                "vmovaps    %%ymm2, 0x40(%[d])\n"
                "vmovaps    %%ymm3, 0x60(%[d])\n"
                "vmovaps    %%ymm4, 0x80(%[d])\n"
                "vmovaps    %%ymm5, 0xa0(%[d])\n"
                "vmovaps    %%ymm6, 0xc0(%[d])\n"
                "vmovaps    %%ymm7, 0xe0(%[d])\n"
                "add        $0x100, %[d]\n"
                : [d]"+r"(dst), [s]"+r"(src)
                :
                : "ymm0", "ymm1", "ymm2", "ymm3", "ymm4", "ymm5", "ymm6", "ymm7", "memory");

            size -= 256;
        }*/

        /*while (size > 128)
        {
            __asm__(
                "vmovups    (%[s]), %%ymm0\n"
                "vmovups    0x20(%[s]), %%ymm1\n"
                "vmovups    0x40(%[s]), %%ymm2\n"
                "vmovups    0x60(%[s]), %%ymm3\n"
                "add        $0x80, %[s]\n"
                "sub        $0x80, %[size]\n"
                "vmovaps    %%ymm0, (%[d])\n"
                "vmovaps    %%ymm1, 0x20(%[d])\n"
                "vmovaps    %%ymm2, 0x40(%[d])\n"
                "vmovaps    %%ymm3, 0x60(%[d])\n"
                "add        $0x80, %[d]\n"
                : [d]"+r"(dst), [s]"+r"(src), [size]"+r"(size)
                :
                : "ymm0", "ymm1", "ymm2", "ymm3", "memory");
        }*/

        __asm__(
            "vmovups    -0x20(%[s],%[size],1), %%ymm0\n"
            "vmovups    %%ymm0, -0x20(%[d],%[size],1)\n"
            : [d]"+r"(dst), [s]"+r"(src)
            : [size]"r"(size)
            : "ymm0", "memory");

        while (size > 32)
        {
            __asm__(
                "vmovups    (%[s]), %%ymm0\n"
                "vmovups    %%ymm0, (%[d])\n"
                : [d]"+r"(dst), [s]"+r"(src)
                :
                : "ymm0", "memory");

            dst += 32;
            src += 32;
            size -= 32;
        }

        __asm__ __volatile__ ("vzeroupper"
            ::: "ymm0", "ymm1", "ymm2", "ymm3", "ymm4", "ymm5", "ymm6", "ymm7",
                "ymm8", "ymm9", "ymm10", "ymm11", "ymm12", "ymm13", "ymm14", "ymm15");
    }

    return ret;
}

extern "C" void * __memcpy_erms(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_sse2_unaligned(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_ssse3(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_ssse3_back(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_avx_unaligned(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_avx_unaligned_erms(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_avx512_unaligned(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_avx512_unaligned_erms(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT
extern "C" void * __memcpy_avx512_no_vzeroupper(void * __restrict destination, const void * __restrict source, size_t size); /// NOLINT


#define VARIANT(N, NAME) \
    if (memcpy_variant == (N)) \
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
    VARIANT(6, memcpySSE2)
    VARIANT(7, memcpySSE2Unrolled2)
    VARIANT(8, memcpySSE2Unrolled4)
    VARIANT(9, memcpySSE2Unrolled8)
    VARIANT(10, memcpy_fast_sse)
    VARIANT(11, memcpy_fast_avx)
    VARIANT(12, memcpy_my)
    VARIANT(13, memcpy_my2)

    VARIANT(21, __memcpy_erms)
    VARIANT(22, __memcpy_sse2_unaligned)
    VARIANT(23, __memcpy_ssse3)
    VARIANT(24, __memcpy_ssse3_back)
    VARIANT(25, __memcpy_avx_unaligned)
    VARIANT(26, __memcpy_avx_unaligned_erms)
    VARIANT(27, __memcpy_avx512_unaligned)
    VARIANT(28, __memcpy_avx512_unaligned_erms)
    VARIANT(29, __memcpy_avx512_no_vzeroupper)

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
