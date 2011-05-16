#include <iostream>

#include <Poco/SharedPtr.h>

#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/copyData.h>


int main(int argc, char ** argv)
{
	if (argc > 2 || (argc == 2 && strcmp(argv[1], "-d")))
	{
		std::cerr << "Usage: " << argv[0] << " [-d] < in > out" << std::endl;
		return 1;
	}

	Poco::SharedPtr<DB::ReadBuffer> rb = new DB::ReadBufferFromIStream(std::cin);
	Poco::SharedPtr<DB::WriteBuffer> wb = new DB::WriteBufferFromOStream(std::cout);
	Poco::SharedPtr<DB::ReadBuffer> from;
	Poco::SharedPtr<DB::WriteBuffer> to;

	if (argc == 1)
	{
		/// Сжатие
		from = rb;
		to = new DB::CompressedWriteBuffer(*wb);
	}
	else
	{
		/// Разжатие
		from = new DB::CompressedReadBuffer(*rb);
		to = wb;
	}

	DB::copyData(*from, *to);

    return 0;
}
