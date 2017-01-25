#include <iostream>
#include <iomanip>

#if defined(__x86_64__)
#include <emmintrin.h>
#endif

#include <common/Common.h>
#include <Poco/NumberParser.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>

/** Считает количество нулей в файле.
  * Выводит "заполненность" файла - отношение количества ненулевых байт к ожидаемому количеству ненулевых байт в файле со случайными байтами.
  */

int main(int argc, char ** argv)
{
#if defined(__x86_64__)
	try
	{
		DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
		size_t zeros = 0;
		size_t limit = 0;

		if (argc == 2)
			limit = Poco::NumberParser::parseUnsigned64(argv[1]);

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
