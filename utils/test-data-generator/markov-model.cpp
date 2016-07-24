#include <iostream>
#include <random>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFile.h>

#include "MarkovModel.h"

using namespace DB;


int main(int argc, char ** argv)
try
{
	size_t n = parse<size_t>(argv[1]);
	size_t results = parse<size_t>(argv[2]);
	ReadBufferFromFileDescriptor in(STDIN_FILENO);

	std::mt19937 random;
	MarkovModel model(n);

	String s;
	while (!in.eof())
	{
		readText(s, in);
		assertChar('\n', in);

		model.consume(s.data(), s.size());
	}

	std::string dst;

	for (size_t i = 0; i < results; ++i)
	{
		dst.resize(10000);
		dst.resize(model.generate(&dst[0], dst.size(), [&]{ return random(); }));

		std::cerr << dst << "\n";
	}

	WriteBufferFromFile dump("dump.bin");
	model.write(dump);

	return 0;
}
catch (...)
{
	std::cerr << getCurrentExceptionMessage(true) << '\n';
	throw;
}
