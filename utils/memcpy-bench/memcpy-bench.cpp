#include <memory>
#include <cstddef>
#include <string>
#include <random>
#include <iostream>
#include <iomanip>
#include <thread>

#include <dlfcn.h>

#include <pcg_random.hpp>

#include <common/defines.h>

#include <Common/Stopwatch.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"
#pragma GCC diagnostic ignored "-Wcast-align"
#pragma GCC diagnostic ignored "-Wcast-qual"
#include "FastMemcpy.h"
//#include "FastMemcpy_Avx.h"

#include <emmintrin.h>
#include <immintrin.h>


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
    }
}


using RNG = pcg32_fast;

template <size_t N>
size_t generatorUniform(RNG & rng) { return rng() % N; };


template <typename F, typename MemcpyImpl>
void test(uint8_t * dst, uint8_t * src, size_t size, size_t iterations, size_t num_threads, F && generator, MemcpyImpl && impl)
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

    double elapsed_ns = watch.elapsed();

    /// Validation
    size_t sum = 0;
    for (size_t i = 0; i < size; ++i)
        sum += dst[i];

    std::cerr << std::fixed << std::setprecision(3)
        << "Processed in " << (elapsed_ns / 1e9) << "sec, " << (size * iterations * 1.0 / elapsed_ns) << " GB/sec (sum = " << sum << ")\n";
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

extern "C" void * memcpy_jart(void * dst, const void * src, size_t size);
extern "C" void MemCpy(void * dst, const void * src, size_t size);


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
__attribute__((__always_inline__))
void memcpy_my_medium_sse(uint8_t * __restrict & dst, const uint8_t * __restrict & src, size_t & size)
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
        _mm256_storeu_si256((__m256i*)dst, head);
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


template <typename F>
void dispatchMemcpyVariants(size_t memcpy_variant, uint8_t * dst, uint8_t * src, size_t size, size_t iterations, size_t num_threads, F && generator)
{
    memcpy_type memcpy_libc = reinterpret_cast<memcpy_type>(dlsym(RTLD_NEXT, "memcpy"));

    if (memcpy_variant == 1)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpy);
    if (memcpy_variant == 2)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpy_libc);
    if (memcpy_variant == 3)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpy_erms);
    if (memcpy_variant == 4)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), MemCpy);
    if (memcpy_variant == 5)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpySSE2);
    if (memcpy_variant == 6)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpySSE2Unrolled2);
    if (memcpy_variant == 7)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpySSE2Unrolled4);
    if (memcpy_variant == 8)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpySSE2Unrolled8);
//    if (memcpy_variant == 9)
//        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpy_fast_avx);
    if (memcpy_variant == 10)
        test(dst, src, size, iterations, num_threads, std::forward<F>(generator), memcpy_my);
}

void dispatchVariants(size_t memcpy_variant, size_t generator_variant, uint8_t * dst, uint8_t * src, size_t size, size_t iterations, size_t num_threads)
{
    if (generator_variant == 1)
        dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<16>);
    if (generator_variant == 2)
        dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<256>);
    if (generator_variant == 3)
        dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<4096>);
    if (generator_variant == 4)
        dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<65536>);
    if (generator_variant == 5)
        dispatchMemcpyVariants(memcpy_variant, dst, src, size, iterations, num_threads, generatorUniform<1048576>);
}


int main(int argc, char ** argv)
{
    size_t size = 1000000000;
    if (argc >= 2)
        size = std::stoull(argv[1]);

    size_t iterations = 10;
    if (argc >= 3)
        iterations = std::stoull(argv[2]);

    size_t num_threads = 1;
    if (argc >= 4)
        num_threads = std::stoull(argv[3]);

    size_t memcpy_variant = 1;
    if (argc >= 5)
        memcpy_variant = std::stoull(argv[4]);

    size_t generator_variant = 1;
    if (argc >= 6)
        generator_variant = std::stoull(argv[5]);

    std::unique_ptr<uint8_t[]> src(new uint8_t[size]);
    std::unique_ptr<uint8_t[]> dst(new uint8_t[size]);

    /// Fill src with some pattern for validation.
    for (size_t i = 0; i < size; ++i)
        src[i] = i;

    /// Fill dst to avoid page faults.
    memset(dst.get(), 0, size);

    dispatchVariants(memcpy_variant, generator_variant, dst.get(), src.get(), size, iterations, num_threads);

    return 0;
}
