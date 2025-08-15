#pragma once

//=====================================================================
//
// FastMemcpy.c - skywind3000@163.com, 2015
//
// feature:
// 50% speed up in avg. vs standard memcpy (tested in vc2012/gcc5.1)
//
//=====================================================================

#include <stddef.h>
#include <stdint.h>
#include <immintrin.h>


//---------------------------------------------------------------------
// force inline for compilers
//---------------------------------------------------------------------
#ifndef INLINE
#ifdef __GNUC__
#if (__GNUC__ > 3) || ((__GNUC__ == 3) && (__GNUC_MINOR__ >= 1))
    #define INLINE         __inline__ __attribute__((always_inline))
#else
    #define INLINE         __inline__
#endif
#elif defined(_MSC_VER)
    #define INLINE __forceinline
#elif (defined(__BORLANDC__) || defined(__WATCOMC__))
    #define INLINE __inline
#else
    #define INLINE
#endif
#endif


//---------------------------------------------------------------------
// fast copy for different sizes
//---------------------------------------------------------------------
static INLINE void memcpy_avx_16(void * __restrict dst, const void * __restrict src)
{
#if 1
    __m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
    _mm_storeu_si128(((__m128i*)dst) + 0, m0);
#else
    *((uint64_t*)((char*)dst + 0)) = *((uint64_t*)((const char*)src + 0));
    *((uint64_t*)((char*)dst + 8)) = *((uint64_t*)((const char*)src + 8));
#endif
}

static INLINE void memcpy_avx_32(void *dst, const void *src)
{
    __m256i m0 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 0);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 0, m0);
}

static INLINE void memcpy_avx_64(void *dst, const void *src)
{
    __m256i m0 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 0);
    __m256i m1 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 1);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 0, m0);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 1, m1);
}

static INLINE void memcpy_avx_128(void *dst, const void *src)
{
    __m256i m0 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 0);
    __m256i m1 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 1);
    __m256i m2 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 2);
    __m256i m3 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 3);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 0, m0);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 1, m1);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 2, m2);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 3, m3);
}

static INLINE void memcpy_avx_256(void *dst, const void *src)
{
    __m256i m0 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 0);
    __m256i m1 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 1);
    __m256i m2 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 2);
    __m256i m3 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 3);
    __m256i m4 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 4);
    __m256i m5 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 5);
    __m256i m6 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 6);
    __m256i m7 = _mm256_loadu_si256((reinterpret_cast<const __m256i*>(src)) + 7);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 0, m0);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 1, m1);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 2, m2);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 3, m3);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 4, m4);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 5, m5);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 6, m6);
    _mm256_storeu_si256((reinterpret_cast<__m256i*>(dst)) + 7, m7);
}


//---------------------------------------------------------------------
// tiny memory copy with jump table optimized
//---------------------------------------------------------------------
static INLINE void *memcpy_tiny_avx(void * __restrict dst, const void * __restrict src, size_t size)
{
    unsigned char *dd = reinterpret_cast<unsigned char *>(dst) + size;
    const unsigned char *ss = reinterpret_cast<const unsigned char*>(src) + size;

    switch (size) /// NOLINT(bugprone-switch-missing-default-case)
    {
    case 128: memcpy_avx_128(dd - 128, ss - 128); [[fallthrough]];
    case 0:  break;
    case 129: memcpy_avx_128(dd - 129, ss - 129); [[fallthrough]];
    case 1: dd[-1] = ss[-1]; break;
    case 130: memcpy_avx_128(dd - 130, ss - 130); [[fallthrough]];
    case 2: *((uint16_t*)(dd - 2)) = *((uint16_t*)(ss - 2)); break;
    case 131: memcpy_avx_128(dd - 131, ss - 131); [[fallthrough]];
    case 3: *((uint16_t*)(dd - 3)) = *((uint16_t*)(ss - 3)); dd[-1] = ss[-1]; break;
    case 132: memcpy_avx_128(dd - 132, ss - 132); [[fallthrough]];
    case 4: *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 133: memcpy_avx_128(dd - 133, ss - 133); [[fallthrough]];
    case 5: *((uint32_t*)(dd - 5)) = *((uint32_t*)(ss - 5)); dd[-1] = ss[-1]; break;
    case 134: memcpy_avx_128(dd - 134, ss - 134); [[fallthrough]];
    case 6: *((uint32_t*)(dd - 6)) = *((uint32_t*)(ss - 6)); *((uint16_t*)(dd - 2)) = *((uint16_t*)(ss - 2)); break;
    case 135: memcpy_avx_128(dd - 135, ss - 135); [[fallthrough]];
    case 7: *((uint32_t*)(dd - 7)) = *((uint32_t*)(ss - 7)); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 136: memcpy_avx_128(dd - 136, ss - 136); [[fallthrough]];
    case 8: *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 137: memcpy_avx_128(dd - 137, ss - 137); [[fallthrough]];
    case 9: *((uint64_t*)(dd - 9)) = *((uint64_t*)(ss - 9)); dd[-1] = ss[-1]; break;
    case 138: memcpy_avx_128(dd - 138, ss - 138); [[fallthrough]];
    case 10: *((uint64_t*)(dd - 10)) = *((uint64_t*)(ss - 10)); *((uint16_t*)(dd - 2)) = *((uint16_t*)(ss - 2)); break;
    case 139: memcpy_avx_128(dd - 139, ss - 139); [[fallthrough]];
    case 11: *((uint64_t*)(dd - 11)) = *((uint64_t*)(ss - 11)); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 140: memcpy_avx_128(dd - 140, ss - 140); [[fallthrough]];
    case 12: *((uint64_t*)(dd - 12)) = *((uint64_t*)(ss - 12)); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 141: memcpy_avx_128(dd - 141, ss - 141); [[fallthrough]];
    case 13: *((uint64_t*)(dd - 13)) = *((uint64_t*)(ss - 13)); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 142: memcpy_avx_128(dd - 142, ss - 142); [[fallthrough]];
    case 14: *((uint64_t*)(dd - 14)) = *((uint64_t*)(ss - 14)); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 143: memcpy_avx_128(dd - 143, ss - 143); [[fallthrough]];
    case 15: *((uint64_t*)(dd - 15)) = *((uint64_t*)(ss - 15)); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 144: memcpy_avx_128(dd - 144, ss - 144); [[fallthrough]];
    case 16: memcpy_avx_16(dd - 16, ss - 16); break;
    case 145: memcpy_avx_128(dd - 145, ss - 145); [[fallthrough]];
    case 17: memcpy_avx_16(dd - 17, ss - 17); dd[-1] = ss[-1]; break;
    case 146: memcpy_avx_128(dd - 146, ss - 146); [[fallthrough]];
    case 18: memcpy_avx_16(dd - 18, ss - 18); *((uint16_t*)(dd - 2)) = *((uint16_t*)(ss - 2)); break;
    case 147: memcpy_avx_128(dd - 147, ss - 147); [[fallthrough]];
    case 19: memcpy_avx_16(dd - 19, ss - 19); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 148: memcpy_avx_128(dd - 148, ss - 148); [[fallthrough]];
    case 20: memcpy_avx_16(dd - 20, ss - 20); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 149: memcpy_avx_128(dd - 149, ss - 149); [[fallthrough]];
    case 21: memcpy_avx_16(dd - 21, ss - 21); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 150: memcpy_avx_128(dd - 150, ss - 150); [[fallthrough]];
    case 22: memcpy_avx_16(dd - 22, ss - 22); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 151: memcpy_avx_128(dd - 151, ss - 151); [[fallthrough]];
    case 23: memcpy_avx_16(dd - 23, ss - 23); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 152: memcpy_avx_128(dd - 152, ss - 152); [[fallthrough]];
    case 24: memcpy_avx_16(dd - 24, ss - 24); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 153: memcpy_avx_128(dd - 153, ss - 153); [[fallthrough]];
    case 25: memcpy_avx_16(dd - 25, ss - 25); memcpy_avx_16(dd - 16, ss - 16); break;
    case 154: memcpy_avx_128(dd - 154, ss - 154); [[fallthrough]];
    case 26: memcpy_avx_16(dd - 26, ss - 26); memcpy_avx_16(dd - 16, ss - 16); break;
    case 155: memcpy_avx_128(dd - 155, ss - 155); [[fallthrough]];
    case 27: memcpy_avx_16(dd - 27, ss - 27); memcpy_avx_16(dd - 16, ss - 16); break;
    case 156: memcpy_avx_128(dd - 156, ss - 156); [[fallthrough]];
    case 28: memcpy_avx_16(dd - 28, ss - 28); memcpy_avx_16(dd - 16, ss - 16); break;
    case 157: memcpy_avx_128(dd - 157, ss - 157); [[fallthrough]];
    case 29: memcpy_avx_16(dd - 29, ss - 29); memcpy_avx_16(dd - 16, ss - 16); break;
    case 158: memcpy_avx_128(dd - 158, ss - 158); [[fallthrough]];
    case 30: memcpy_avx_16(dd - 30, ss - 30); memcpy_avx_16(dd - 16, ss - 16); break;
    case 159: memcpy_avx_128(dd - 159, ss - 159); [[fallthrough]];
    case 31: memcpy_avx_16(dd - 31, ss - 31); memcpy_avx_16(dd - 16, ss - 16); break;
    case 160: memcpy_avx_128(dd - 160, ss - 160); [[fallthrough]];
    case 32: memcpy_avx_32(dd - 32, ss - 32); break;
    case 161: memcpy_avx_128(dd - 161, ss - 161); [[fallthrough]];
    case 33: memcpy_avx_32(dd - 33, ss - 33); dd[-1] = ss[-1]; break;
    case 162: memcpy_avx_128(dd - 162, ss - 162); [[fallthrough]];
    case 34: memcpy_avx_32(dd - 34, ss - 34); *((uint16_t*)(dd - 2)) = *((uint16_t*)(ss - 2)); break;
    case 163: memcpy_avx_128(dd - 163, ss - 163); [[fallthrough]];
    case 35: memcpy_avx_32(dd - 35, ss - 35); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 164: memcpy_avx_128(dd - 164, ss - 164); [[fallthrough]];
    case 36: memcpy_avx_32(dd - 36, ss - 36); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 165: memcpy_avx_128(dd - 165, ss - 165); [[fallthrough]];
    case 37: memcpy_avx_32(dd - 37, ss - 37); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 166: memcpy_avx_128(dd - 166, ss - 166); [[fallthrough]];
    case 38: memcpy_avx_32(dd - 38, ss - 38); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 167: memcpy_avx_128(dd - 167, ss - 167); [[fallthrough]];
    case 39: memcpy_avx_32(dd - 39, ss - 39); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 168: memcpy_avx_128(dd - 168, ss - 168); [[fallthrough]];
    case 40: memcpy_avx_32(dd - 40, ss - 40); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 169: memcpy_avx_128(dd - 169, ss - 169); [[fallthrough]];
    case 41: memcpy_avx_32(dd - 41, ss - 41); memcpy_avx_16(dd - 16, ss - 16); break;
    case 170: memcpy_avx_128(dd - 170, ss - 170); [[fallthrough]];
    case 42: memcpy_avx_32(dd - 42, ss - 42); memcpy_avx_16(dd - 16, ss - 16); break;
    case 171: memcpy_avx_128(dd - 171, ss - 171); [[fallthrough]];
    case 43: memcpy_avx_32(dd - 43, ss - 43); memcpy_avx_16(dd - 16, ss - 16); break;
    case 172: memcpy_avx_128(dd - 172, ss - 172); [[fallthrough]];
    case 44: memcpy_avx_32(dd - 44, ss - 44); memcpy_avx_16(dd - 16, ss - 16); break;
    case 173: memcpy_avx_128(dd - 173, ss - 173); [[fallthrough]];
    case 45: memcpy_avx_32(dd - 45, ss - 45); memcpy_avx_16(dd - 16, ss - 16); break;
    case 174: memcpy_avx_128(dd - 174, ss - 174); [[fallthrough]];
    case 46: memcpy_avx_32(dd - 46, ss - 46); memcpy_avx_16(dd - 16, ss - 16); break;
    case 175: memcpy_avx_128(dd - 175, ss - 175); [[fallthrough]];
    case 47: memcpy_avx_32(dd - 47, ss - 47); memcpy_avx_16(dd - 16, ss - 16); break;
    case 176: memcpy_avx_128(dd - 176, ss - 176); [[fallthrough]];
    case 48: memcpy_avx_32(dd - 48, ss - 48); memcpy_avx_16(dd - 16, ss - 16); break;
    case 177: memcpy_avx_128(dd - 177, ss - 177); [[fallthrough]];
    case 49: memcpy_avx_32(dd - 49, ss - 49); memcpy_avx_32(dd - 32, ss - 32); break;
    case 178: memcpy_avx_128(dd - 178, ss - 178); [[fallthrough]];
    case 50: memcpy_avx_32(dd - 50, ss - 50); memcpy_avx_32(dd - 32, ss - 32); break;
    case 179: memcpy_avx_128(dd - 179, ss - 179); [[fallthrough]];
    case 51: memcpy_avx_32(dd - 51, ss - 51); memcpy_avx_32(dd - 32, ss - 32); break;
    case 180: memcpy_avx_128(dd - 180, ss - 180); [[fallthrough]];
    case 52: memcpy_avx_32(dd - 52, ss - 52); memcpy_avx_32(dd - 32, ss - 32); break;
    case 181: memcpy_avx_128(dd - 181, ss - 181); [[fallthrough]];
    case 53: memcpy_avx_32(dd - 53, ss - 53); memcpy_avx_32(dd - 32, ss - 32); break;
    case 182: memcpy_avx_128(dd - 182, ss - 182); [[fallthrough]];
    case 54: memcpy_avx_32(dd - 54, ss - 54); memcpy_avx_32(dd - 32, ss - 32); break;
    case 183: memcpy_avx_128(dd - 183, ss - 183); [[fallthrough]];
    case 55: memcpy_avx_32(dd - 55, ss - 55); memcpy_avx_32(dd - 32, ss - 32); break;
    case 184: memcpy_avx_128(dd - 184, ss - 184); [[fallthrough]];
    case 56: memcpy_avx_32(dd - 56, ss - 56); memcpy_avx_32(dd - 32, ss - 32); break;
    case 185: memcpy_avx_128(dd - 185, ss - 185); [[fallthrough]];
    case 57: memcpy_avx_32(dd - 57, ss - 57); memcpy_avx_32(dd - 32, ss - 32); break;
    case 186: memcpy_avx_128(dd - 186, ss - 186); [[fallthrough]];
    case 58: memcpy_avx_32(dd - 58, ss - 58); memcpy_avx_32(dd - 32, ss - 32); break;
    case 187: memcpy_avx_128(dd - 187, ss - 187); [[fallthrough]];
    case 59: memcpy_avx_32(dd - 59, ss - 59); memcpy_avx_32(dd - 32, ss - 32); break;
    case 188: memcpy_avx_128(dd - 188, ss - 188); [[fallthrough]];
    case 60: memcpy_avx_32(dd - 60, ss - 60); memcpy_avx_32(dd - 32, ss - 32); break;
    case 189: memcpy_avx_128(dd - 189, ss - 189); [[fallthrough]];
    case 61: memcpy_avx_32(dd - 61, ss - 61); memcpy_avx_32(dd - 32, ss - 32); break;
    case 190: memcpy_avx_128(dd - 190, ss - 190); [[fallthrough]];
    case 62: memcpy_avx_32(dd - 62, ss - 62); memcpy_avx_32(dd - 32, ss - 32); break;
    case 191: memcpy_avx_128(dd - 191, ss - 191); [[fallthrough]];
    case 63: memcpy_avx_32(dd - 63, ss - 63); memcpy_avx_32(dd - 32, ss - 32); break;
    case 192: memcpy_avx_128(dd - 192, ss - 192); [[fallthrough]];
    case 64: memcpy_avx_64(dd - 64, ss - 64); break;
    case 193: memcpy_avx_128(dd - 193, ss - 193); [[fallthrough]];
    case 65: memcpy_avx_64(dd - 65, ss - 65); dd[-1] = ss[-1]; break;
    case 194: memcpy_avx_128(dd - 194, ss - 194); [[fallthrough]];
    case 66: memcpy_avx_64(dd - 66, ss - 66); *((uint16_t*)(dd - 2)) = *((uint16_t*)(ss - 2)); break;
    case 195: memcpy_avx_128(dd - 195, ss - 195); [[fallthrough]];
    case 67: memcpy_avx_64(dd - 67, ss - 67); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 196: memcpy_avx_128(dd - 196, ss - 196); [[fallthrough]];
    case 68: memcpy_avx_64(dd - 68, ss - 68); *((uint32_t*)(dd - 4)) = *((uint32_t*)(ss - 4)); break;
    case 197: memcpy_avx_128(dd - 197, ss - 197); [[fallthrough]];
    case 69: memcpy_avx_64(dd - 69, ss - 69); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 198: memcpy_avx_128(dd - 198, ss - 198); [[fallthrough]];
    case 70: memcpy_avx_64(dd - 70, ss - 70); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 199: memcpy_avx_128(dd - 199, ss - 199); [[fallthrough]];
    case 71: memcpy_avx_64(dd - 71, ss - 71); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 200: memcpy_avx_128(dd - 200, ss - 200); [[fallthrough]];
    case 72: memcpy_avx_64(dd - 72, ss - 72); *((uint64_t*)(dd - 8)) = *((uint64_t*)(ss - 8)); break;
    case 201: memcpy_avx_128(dd - 201, ss - 201); [[fallthrough]];
    case 73: memcpy_avx_64(dd - 73, ss - 73); memcpy_avx_16(dd - 16, ss - 16); break;
    case 202: memcpy_avx_128(dd - 202, ss - 202); [[fallthrough]];
    case 74: memcpy_avx_64(dd - 74, ss - 74); memcpy_avx_16(dd - 16, ss - 16); break;
    case 203: memcpy_avx_128(dd - 203, ss - 203); [[fallthrough]];
    case 75: memcpy_avx_64(dd - 75, ss - 75); memcpy_avx_16(dd - 16, ss - 16); break;
    case 204: memcpy_avx_128(dd - 204, ss - 204); [[fallthrough]];
    case 76: memcpy_avx_64(dd - 76, ss - 76); memcpy_avx_16(dd - 16, ss - 16); break;
    case 205: memcpy_avx_128(dd - 205, ss - 205); [[fallthrough]];
    case 77: memcpy_avx_64(dd - 77, ss - 77); memcpy_avx_16(dd - 16, ss - 16); break;
    case 206: memcpy_avx_128(dd - 206, ss - 206); [[fallthrough]];
    case 78: memcpy_avx_64(dd - 78, ss - 78); memcpy_avx_16(dd - 16, ss - 16); break;
    case 207: memcpy_avx_128(dd - 207, ss - 207); [[fallthrough]];
    case 79: memcpy_avx_64(dd - 79, ss - 79); memcpy_avx_16(dd - 16, ss - 16); break;
    case 208: memcpy_avx_128(dd - 208, ss - 208); [[fallthrough]];
    case 80: memcpy_avx_64(dd - 80, ss - 80); memcpy_avx_16(dd - 16, ss - 16); break;
    case 209: memcpy_avx_128(dd - 209, ss - 209); [[fallthrough]];
    case 81: memcpy_avx_64(dd - 81, ss - 81); memcpy_avx_32(dd - 32, ss - 32); break;
    case 210: memcpy_avx_128(dd - 210, ss - 210); [[fallthrough]];
    case 82: memcpy_avx_64(dd - 82, ss - 82); memcpy_avx_32(dd - 32, ss - 32); break;
    case 211: memcpy_avx_128(dd - 211, ss - 211); [[fallthrough]];
    case 83: memcpy_avx_64(dd - 83, ss - 83); memcpy_avx_32(dd - 32, ss - 32); break;
    case 212: memcpy_avx_128(dd - 212, ss - 212); [[fallthrough]];
    case 84: memcpy_avx_64(dd - 84, ss - 84); memcpy_avx_32(dd - 32, ss - 32); break;
    case 213: memcpy_avx_128(dd - 213, ss - 213); [[fallthrough]];
    case 85: memcpy_avx_64(dd - 85, ss - 85); memcpy_avx_32(dd - 32, ss - 32); break;
    case 214: memcpy_avx_128(dd - 214, ss - 214); [[fallthrough]];
    case 86: memcpy_avx_64(dd - 86, ss - 86); memcpy_avx_32(dd - 32, ss - 32); break;
    case 215: memcpy_avx_128(dd - 215, ss - 215); [[fallthrough]];
    case 87: memcpy_avx_64(dd - 87, ss - 87); memcpy_avx_32(dd - 32, ss - 32); break;
    case 216: memcpy_avx_128(dd - 216, ss - 216); [[fallthrough]];
    case 88: memcpy_avx_64(dd - 88, ss - 88); memcpy_avx_32(dd - 32, ss - 32); break;
    case 217: memcpy_avx_128(dd - 217, ss - 217); [[fallthrough]];
    case 89: memcpy_avx_64(dd - 89, ss - 89); memcpy_avx_32(dd - 32, ss - 32); break;
    case 218: memcpy_avx_128(dd - 218, ss - 218); [[fallthrough]];
    case 90: memcpy_avx_64(dd - 90, ss - 90); memcpy_avx_32(dd - 32, ss - 32); break;
    case 219: memcpy_avx_128(dd - 219, ss - 219); [[fallthrough]];
    case 91: memcpy_avx_64(dd - 91, ss - 91); memcpy_avx_32(dd - 32, ss - 32); break;
    case 220: memcpy_avx_128(dd - 220, ss - 220); [[fallthrough]];
    case 92: memcpy_avx_64(dd - 92, ss - 92); memcpy_avx_32(dd - 32, ss - 32); break;
    case 221: memcpy_avx_128(dd - 221, ss - 221); [[fallthrough]];
    case 93: memcpy_avx_64(dd - 93, ss - 93); memcpy_avx_32(dd - 32, ss - 32); break;
    case 222: memcpy_avx_128(dd - 222, ss - 222); [[fallthrough]];
    case 94: memcpy_avx_64(dd - 94, ss - 94); memcpy_avx_32(dd - 32, ss - 32); break;
    case 223: memcpy_avx_128(dd - 223, ss - 223); [[fallthrough]];
    case 95: memcpy_avx_64(dd - 95, ss - 95); memcpy_avx_32(dd - 32, ss - 32); break;
    case 224: memcpy_avx_128(dd - 224, ss - 224); [[fallthrough]];
    case 96: memcpy_avx_64(dd - 96, ss - 96); memcpy_avx_32(dd - 32, ss - 32); break;
    case 225: memcpy_avx_128(dd - 225, ss - 225); [[fallthrough]];
    case 97: memcpy_avx_64(dd - 97, ss - 97); memcpy_avx_64(dd - 64, ss - 64); break;
    case 226: memcpy_avx_128(dd - 226, ss - 226); [[fallthrough]];
    case 98: memcpy_avx_64(dd - 98, ss - 98); memcpy_avx_64(dd - 64, ss - 64); break;
    case 227: memcpy_avx_128(dd - 227, ss - 227); [[fallthrough]];
    case 99: memcpy_avx_64(dd - 99, ss - 99); memcpy_avx_64(dd - 64, ss - 64); break;
    case 228: memcpy_avx_128(dd - 228, ss - 228); [[fallthrough]];
    case 100: memcpy_avx_64(dd - 100, ss - 100); memcpy_avx_64(dd - 64, ss - 64); break;
    case 229: memcpy_avx_128(dd - 229, ss - 229); [[fallthrough]];
    case 101: memcpy_avx_64(dd - 101, ss - 101); memcpy_avx_64(dd - 64, ss - 64); break;
    case 230: memcpy_avx_128(dd - 230, ss - 230); [[fallthrough]];
    case 102: memcpy_avx_64(dd - 102, ss - 102); memcpy_avx_64(dd - 64, ss - 64); break;
    case 231: memcpy_avx_128(dd - 231, ss - 231); [[fallthrough]];
    case 103: memcpy_avx_64(dd - 103, ss - 103); memcpy_avx_64(dd - 64, ss - 64); break;
    case 232: memcpy_avx_128(dd - 232, ss - 232); [[fallthrough]];
    case 104: memcpy_avx_64(dd - 104, ss - 104); memcpy_avx_64(dd - 64, ss - 64); break;
    case 233: memcpy_avx_128(dd - 233, ss - 233); [[fallthrough]];
    case 105: memcpy_avx_64(dd - 105, ss - 105); memcpy_avx_64(dd - 64, ss - 64); break;
    case 234: memcpy_avx_128(dd - 234, ss - 234); [[fallthrough]];
    case 106: memcpy_avx_64(dd - 106, ss - 106); memcpy_avx_64(dd - 64, ss - 64); break;
    case 235: memcpy_avx_128(dd - 235, ss - 235); [[fallthrough]];
    case 107: memcpy_avx_64(dd - 107, ss - 107); memcpy_avx_64(dd - 64, ss - 64); break;
    case 236: memcpy_avx_128(dd - 236, ss - 236); [[fallthrough]];
    case 108: memcpy_avx_64(dd - 108, ss - 108); memcpy_avx_64(dd - 64, ss - 64); break;
    case 237: memcpy_avx_128(dd - 237, ss - 237); [[fallthrough]];
    case 109: memcpy_avx_64(dd - 109, ss - 109); memcpy_avx_64(dd - 64, ss - 64); break;
    case 238: memcpy_avx_128(dd - 238, ss - 238); [[fallthrough]];
    case 110: memcpy_avx_64(dd - 110, ss - 110); memcpy_avx_64(dd - 64, ss - 64); break;
    case 239: memcpy_avx_128(dd - 239, ss - 239); [[fallthrough]];
    case 111: memcpy_avx_64(dd - 111, ss - 111); memcpy_avx_64(dd - 64, ss - 64); break;
    case 240: memcpy_avx_128(dd - 240, ss - 240); [[fallthrough]];
    case 112: memcpy_avx_64(dd - 112, ss - 112); memcpy_avx_64(dd - 64, ss - 64); break;
    case 241: memcpy_avx_128(dd - 241, ss - 241); [[fallthrough]];
    case 113: memcpy_avx_64(dd - 113, ss - 113); memcpy_avx_64(dd - 64, ss - 64); break;
    case 242: memcpy_avx_128(dd - 242, ss - 242); [[fallthrough]];
    case 114: memcpy_avx_64(dd - 114, ss - 114); memcpy_avx_64(dd - 64, ss - 64); break;
    case 243: memcpy_avx_128(dd - 243, ss - 243); [[fallthrough]];
    case 115: memcpy_avx_64(dd - 115, ss - 115); memcpy_avx_64(dd - 64, ss - 64); break;
    case 244: memcpy_avx_128(dd - 244, ss - 244); [[fallthrough]];
    case 116: memcpy_avx_64(dd - 116, ss - 116); memcpy_avx_64(dd - 64, ss - 64); break;
    case 245: memcpy_avx_128(dd - 245, ss - 245); [[fallthrough]];
    case 117: memcpy_avx_64(dd - 117, ss - 117); memcpy_avx_64(dd - 64, ss - 64); break;
    case 246: memcpy_avx_128(dd - 246, ss - 246); [[fallthrough]];
    case 118: memcpy_avx_64(dd - 118, ss - 118); memcpy_avx_64(dd - 64, ss - 64); break;
    case 247: memcpy_avx_128(dd - 247, ss - 247); [[fallthrough]];
    case 119: memcpy_avx_64(dd - 119, ss - 119); memcpy_avx_64(dd - 64, ss - 64); break;
    case 248: memcpy_avx_128(dd - 248, ss - 248); [[fallthrough]];
    case 120: memcpy_avx_64(dd - 120, ss - 120); memcpy_avx_64(dd - 64, ss - 64); break;
    case 249: memcpy_avx_128(dd - 249, ss - 249); [[fallthrough]];
    case 121: memcpy_avx_64(dd - 121, ss - 121); memcpy_avx_64(dd - 64, ss - 64); break;
    case 250: memcpy_avx_128(dd - 250, ss - 250); [[fallthrough]];
    case 122: memcpy_avx_64(dd - 122, ss - 122); memcpy_avx_64(dd - 64, ss - 64); break;
    case 251: memcpy_avx_128(dd - 251, ss - 251); [[fallthrough]];
    case 123: memcpy_avx_64(dd - 123, ss - 123); memcpy_avx_64(dd - 64, ss - 64); break;
    case 252: memcpy_avx_128(dd - 252, ss - 252); [[fallthrough]];
    case 124: memcpy_avx_64(dd - 124, ss - 124); memcpy_avx_64(dd - 64, ss - 64); break;
    case 253: memcpy_avx_128(dd - 253, ss - 253); [[fallthrough]];
    case 125: memcpy_avx_64(dd - 125, ss - 125); memcpy_avx_64(dd - 64, ss - 64); break;
    case 254: memcpy_avx_128(dd - 254, ss - 254); [[fallthrough]];
    case 126: memcpy_avx_64(dd - 126, ss - 126); memcpy_avx_64(dd - 64, ss - 64); break;
    case 255: memcpy_avx_128(dd - 255, ss - 255); [[fallthrough]];
    case 127: memcpy_avx_64(dd - 127, ss - 127); memcpy_avx_64(dd - 64, ss - 64); break;
    case 256: memcpy_avx_256(dd - 256, ss - 256); break;
    }

    return dst;
}


//---------------------------------------------------------------------
// main routine
//---------------------------------------------------------------------
void* memcpy_fast_avx(void * __restrict destination, const void * __restrict source, size_t size) /// NOLINT(misc-definitions-in-headers)
{
    unsigned char *dst = reinterpret_cast<unsigned char*>(destination);
    const unsigned char *src = reinterpret_cast<const unsigned char*>(source);
    static size_t cachesize = 0x200000; // L3-cache size
    size_t padding;

    // small memory copy
    if (size <= 256)
    {
        memcpy_tiny_avx(dst, src, size);
        _mm256_zeroupper();
        return destination;
    }

    // align destination to 16 bytes boundary
    padding = (32 - (((size_t)dst) & 31)) & 31;

#if 0
    if (padding > 0)
    {
        __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
        _mm256_storeu_si256((__m256i*)dst, head);
        dst += padding;
        src += padding;
        size -= padding;
    }
#else
    __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
    _mm256_storeu_si256((__m256i*)dst, head);
    dst += padding;
    src += padding;
    size -= padding;
#endif

    // medium size copy
    if (size <= cachesize)
    {
        __m256i c0, c1, c2, c3, c4, c5, c6, c7;

        for (; size >= 256; size -= 256)
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
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 0), c0);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 1), c1);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 2), c2);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 3), c3);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 4), c4);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 5), c5);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 6), c6);
            _mm256_storeu_si256(((reinterpret_cast<__m256i*>(dst)) + 7), c7);
            dst += 256;
        }
    }
    else
    {        // big memory copy
        __m256i c0, c1, c2, c3, c4, c5, c6, c7;
        /* __m256i c0, c1, c2, c3, c4, c5, c6, c7; */

        if ((((size_t)src) & 31) == 0)
        {    // source aligned
            for (; size >= 256; size -= 256)
            {
                c0 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 0);
                c1 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 1);
                c2 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 2);
                c3 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 3);
                c4 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 4);
                c5 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 5);
                c6 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 6);
                c7 = _mm256_load_si256((reinterpret_cast<const __m256i*>(src)) + 7);
                src += 256;
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 0), c0);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 1), c1);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 2), c2);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 3), c3);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 4), c4);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 5), c5);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 6), c6);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 7), c7);
                dst += 256;
            }
        }
        else
        {                            // source unaligned
            for (; size >= 256; size -= 256)
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
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 0), c0);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 1), c1);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 2), c2);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 3), c3);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 4), c4);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 5), c5);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 6), c6);
                _mm256_stream_si256(((reinterpret_cast<__m256i*>(dst)) + 7), c7);
                dst += 256;
            }
        }
        _mm_sfence();
    }

    memcpy_tiny_avx(dst, src, size);
    _mm256_zeroupper();

    return destination;
}
