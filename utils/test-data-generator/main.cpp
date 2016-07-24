#include <iostream>
#include <random>
#include <cmath>

#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Common/HashTable/Hash.h>

#include "MarkovModel.h"


/** Generate infinite stream of pseudo-random data
 *   like 'hits' table in Yandex.Metrica (with structure as it was in 2013)
  *  and output it in TabSeparated format.
  */

using namespace DB;


struct Generator
{
	WriteBufferFromFileDescriptor out;
	std::mt19937_64 random;

	UInt64 WatchID = random();
	String Title;
	String URL;
	String Referer;
	String FlashMinor2;
	String UserAgentMinor;

	Generator() : out(STDOUT_FILENO) {}

	/** Choosing of distributions parameters sometimes resembles real data, but quite arbitary.
	  */

	void generateRow()
	{
		/// Unique identifier of event.
		WatchID += std::uniform_int_distribution<UInt64>(0, 10000000000)(random);
		writeText(WatchID, out);
		writeChar('\t', out);

		bool JavaEnable = std::bernoulli_distribution(0.6)(random);
		writeText(JavaEnable, out);
		writeChar('\t', out);

		/// Title

		bool GoodEvent = 1;
		writeText(GoodEvent, out);
		writeChar('\t', out);

		LocalDateTime EventTime;
		EventTime.year(2013);
		EventTime.month(7);
		EventTime.day(std::discrete_distribution<>({
			0, 0, 13, 30, 0, 14, 42, 5, 6, 31, 17, 0, 0, 0, 0, 23, 10, 0, 0, 0, 19, 24, 8, 7, 0, 0, 8, 2, 15, 12, 7, 29})(random));
		EventTime.hour(std::discrete_distribution<>({
			13, 7, 4, 3, 2, 3, 4, 6, 10, 16, 20, 23, 24, 23, 18, 19, 19, 19, 14, 15, 14, 13, 17, 17})(random));
		EventTime.minute(std::uniform_int_distribution<UInt8>(0, 59)(random));
		EventTime.second(std::uniform_int_distribution<UInt8>(0, 59)(random));
		writeText(EventTime, out);
		writeChar('\t', out);

		LocalDate EventDate = EventTime.toDate();
		writeText(EventDate, out);
		writeChar('\t', out);

		UInt32 CounterID = hash(1, powerLaw(20, 1.1)) % 10000000;
		writeText(CounterID, out);
		writeChar('\t', out);

		UInt32 ClientIP = hash(2, powerLaw(5000, 1.1));
		writeText(ClientIP, out);
		writeChar('\t', out);

		UInt32 RegionID = hash(3, powerLaw(15, 1.1)) % 5000;
		writeText(RegionID, out);
		writeChar('\t', out);

		UInt64 UserID = hash(4, powerLaw(5000, 1.1));
		UserID = UserID / 10000000000ULL * 10000000000ULL + static_cast<time_t>(EventTime) + UserID % 1000000;
		writeText(UserID, out);
		writeChar('\t', out);

		bool CounterClass = (hash(5, CounterID) % 100) < 25;
		writeText(CounterClass, out);
		writeChar('\t', out);

		UInt8 OS = hash(6, powerLaw(10, 4)) % 100;
		writeText(OS, out);
		writeChar('\t', out);

		UInt8 UserAgent = hash(7, powerLaw(10, 4)) % 100;
		writeText(UserAgent, out);
		writeChar('\t', out);

		/// URL

		/// Referer

		bool Refresh = std::bernoulli_distribution(0.1)(random);
		writeText(Refresh, out);
		writeChar('\t', out);

		UInt16 RefererCategoryID = std::bernoulli_distribution(0.1)(random) ? 0 : (hash(8, powerLaw(10, 4)) % 10000);
		writeText(RefererCategoryID, out);
		writeChar('\t', out);

		UInt32 RefererRegionID = std::bernoulli_distribution(0.1)(random) ? 0 : (hash(9, powerLaw(15, 1.1)) % 5000);
		writeText(RefererRegionID, out);
		writeChar('\t', out);

		UInt16 URLCategoryID = std::bernoulli_distribution(0.1)(random) ? 0 : (hash(10, powerLaw(10, 4)) % 10000);
		writeText(URLCategoryID, out);
		writeChar('\t', out);

		UInt32 URLRegionID = std::bernoulli_distribution(0.1)(random) ? 0 : (hash(11, powerLaw(15, 1.1)) % 5000);
		writeText(URLRegionID, out);
		writeChar('\t', out);

		UInt16 ResolutionWidth;
		UInt16 ResolutionHeight;

		std::tie(ResolutionWidth, ResolutionHeight) = powerLawSampleFrom<std::pair<UInt16, UInt16>>(15, 1.1,
		{
			{1366, 768}, {1280, 1024}, {1920, 1080}, {0, 0}, {1024, 768},
			{1280, 800}, {1440, 900}, {1600, 900}, {1600, 900}, {1680, 1050},
			{768, 1024}, {1024, 600}, {1360, 768}, {1280, 720}, {1152, 864},
			{1280, 768}, {320, 480}, {1920, 1200}, {320, 568}, {1093, 614},
		});

		if (std::bernoulli_distribution(0.1)(random))
		{
			ResolutionWidth = std::bernoulli_distribution(0.1)(random)
				? std::uniform_int_distribution<UInt16>(160, 3000)(random)
				: (std::uniform_int_distribution<UInt16>(160, 3000)(random) / 16 * 16);

			ResolutionHeight = std::bernoulli_distribution(0.1)(random)
				? std::uniform_int_distribution<UInt16>(160, 3000)(random)
				: (ResolutionWidth / 16 * 10);
		}

		writeText(ResolutionWidth, out);
		writeChar('\t', out);

		writeText(ResolutionHeight, out);
		writeChar('\t', out);

		UInt8 ResolutionDepth = weightedSelect<UInt8>({32, 24, 0, 16, 8}, {2000000, 700000, 300000, 50000, 100});
		writeText(ResolutionDepth, out);
		writeChar('\t', out);

		UInt8 FlashMajor = weightedSelect<UInt8>({11, 0, 10, 6, 9, 8, 7, 5, 12}, {2000000, 600000, 200000, 100000, 8000, 800, 600, 20, 10});
		UInt8 FlashMinor = weightedSelect<UInt8>({7, 0, 8, 1, 6, 3, 2, 5, 4}, {1500000, 700000, 500000, 150000, 100000, 80000, 60000, 50000, 50000});

		writeText(FlashMajor, out);
		writeChar('\t', out);

		writeText(FlashMinor, out);
		writeChar('\t', out);

		FlashMinor2.clear();
		if (FlashMajor && FlashMinor)
		{
			FlashMinor2 = toString(hash(12, powerLaw(10, 4)) % 1000);
			if (std::bernoulli_distribution(0.2)(random))
			{
				FlashMinor2 += '.';
				FlashMinor2 += toString(hash(13, powerLaw(10, 4)) % 1000);
			}
		}

		writeText(FlashMinor2, out);
		writeChar('\t', out);

		UInt8 NetMajor = weightedSelect<UInt8>({0, 3, 2, 1, 4}, {3000000, 100000, 10000, 5000, 2});
		UInt8 NetMinor = weightedSelect<UInt8>({0, 5, 1}, {3000000, 200000, 5000});

		writeText(NetMajor, out);
		writeChar('\t', out);

		writeText(NetMinor, out);
		writeChar('\t', out);

		UInt16 UserAgentMajor = UserAgent ? hash(14, powerLaw(10, 4)) % 100 : 0;
		writeText(UserAgentMajor, out);
		writeChar('\t', out);

		UserAgentMinor.clear();
		if (UserAgentMajor)
		{
			UserAgentMinor = toString(hash(15, powerLaw(10, 4)) % 100);
			if (UserAgentMinor.size() == 1 && std::bernoulli_distribution(0.1)(random))
				UserAgentMinor += 'a' + std::uniform_int_distribution<UInt8>(0, 25)(random);
		}
		writeText(UserAgentMinor, out);
		writeChar('\t', out);

		bool CookieEnable = std::bernoulli_distribution(0.999)(random);
		writeText(CookieEnable, out);
		writeChar('\t', out);

		bool JavascriptEnable = std::bernoulli_distribution(0.95)(random);
		writeText(JavascriptEnable, out);
		writeChar('\t', out);



		writeChar('\n', out);
	}

	UInt64 powerLawImpl(double scale, double alpha, double unit_random_value)
	{
		return scale * std::pow(unit_random_value, -1.0 / alpha) - scale;
	}

	UInt64 powerLaw(double scale, double alpha)
	{
		return powerLawImpl(scale, alpha, std::uniform_real_distribution<double>(0, 1)(random));
	}

	template <typename T>
	T powerLawSampleFrom(double scale, double alpha, std::initializer_list<T> set)
	{
		return set.begin()[powerLaw(scale, alpha) % set.size()];
	}

	template <typename T>
	T weightedSelect(std::initializer_list<T> items, std::initializer_list<double> weights)
	{
		return items.begin()[std::discrete_distribution<>(weights)(random)];
	}

	static UInt64 hash(unsigned seed, UInt64 x)
	{
		return intHash64(x + seed * 0xDEADBEEF);
	}
};


int main(int argc, char ** argv)
try
{
/*	Generator generator;
	while (true)
		generator.generateRow();*/

	std::mt19937 random;
	MarkovModel model(3);

	model.consume(src.data(), src.size());

	std::string dst;
	dst.resize(src.size());
	dst.resize(model.generate(&dst[0], dst.size(), [&]{ return random(); }));

	std::cerr << dst << "\n";

	return 0;
}
catch (...)
{
	/// Broken pipe, when piped to 'head', by example.
	if (errno != EPIPE)
	{
		std::cerr << getCurrentExceptionMessage(true) << '\n';
		throw;
	}
}
