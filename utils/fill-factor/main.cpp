#include <iostream>
#include <iomanip>

#if __SSE2__
#include <emmintrin.h>
#endif

#include <common/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>

/** Counts number of 0 in a file.
  * Outputs "fullness" of file - ration of non-0 bytes to the expected number of non-0 bytes in file with random bytes.
  */

int main(int argc, char ** argv)
{
#if __SSE2__
    try
    {
        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
        size_t zeros = 0;
        UInt64 limit = 0;

        if (argc == 2)
            limit = DB::parse<UInt64>(argv[1]);

        while (!in.eof())
        {
            const __m128i zero = {0};
            for (; in.position() + 15 < in.buffer().end(); in.position() += 16)
            {
                __m128i bytes = *reinterpret_cast<const __m128i *>(in.position());
                __m128i byte_mask = _mm_cmpeq_epi8(bytes, zero);
                UInt16 bit_mask = _mm_movemask_epi8(byte_mask);
                zeros += __builtin_popcount(bit_mask);
            }

            for (; in.position() < in.buffer().end(); ++in.position())
                if (*in.position() == 0)
                    ++zeros;

            if (limit && in.count() >= limit)
                break;
        }

        std::cout << std::fixed
            << 1 - std::max(0.0, static_cast<double>(zeros) / in.count() - 1.0 / 256)
            << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.what() << ", " << e.message() << std::endl
            << std::endl
            << "Stack trace:" << std::endl
            << e.getStackTrace().toString()
            << std::endl;
        throw;
    }
#else
    std::cerr << "Only for x86_64 arch " << std::endl;
#endif

    return 0;
}
