#include <iostream>
#include <iomanip>

#include <Poco/NumberParser.h>
#include <Poco/String.h>

#include <dbms/src/IO/ReadBufferFromFileDescriptor.h>
#include <dbms/src/IO/ReadHelpers.h>

#include <dbms/src/Common/Stopwatch.h>
#include <metrika/core/libs/boost-threadpool/threadpool.hpp>

#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>


typedef std::vector<std::string> Strings;

void thread(const UATraits & uatraits, Strings::const_iterator begin, Strings::const_iterator end, UInt64 & res)
{
    UATraits::MatchedSubstrings matched_substrings;
    for (Strings::const_iterator it = begin; it != end; ++it)
    {
        UATraits::Result result;
        uatraits.detect(*it, StringRef(), StringRef(), result, matched_substrings);
        res += result.bool_fields[UATraits::Result::isMobile];
    }
}


int main(int argc, char ** argv)
{
    try
    {
        DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);

        UATraits uatraits("browser.xml", "profiles.xml", "extra.xml");

        Strings user_agents;

        while (!in.eof())
        {
            user_agents.push_back(std::string());
            DB::readEscapedString(user_agents.back(), in);
            user_agents.back() = Poco::toLower(user_agents.back());
            DB::assertString("\n", in);
        }

        size_t threads = argc < 2 ? 2 : Poco::NumberParser::parseUnsigned(argv[1]);

        typedef std::vector<UInt64> Results;
        Results results(threads);

        boost::threadpool::pool pool(threads);

        Stopwatch watch;

        for (size_t i = 0; i < threads; ++i)
            pool.schedule(std::bind(thread,
                std::cref(uatraits),
                user_agents.begin() + user_agents.size() * i / threads,
                user_agents.begin() + user_agents.size() * (i + 1) / threads,
                std::ref(results[i])));

        pool.wait();

        UInt64 res = 0;
        for (size_t i = 0; i < threads; ++i)
            res += results[i];

        double seconds = watch.elapsedSeconds();
        std::cerr << std::fixed << std::setprecision(2)
            << "Res = " << res << ". Elapsed: " << seconds << " sec., " << user_agents.size() / seconds << " rows/sec."
            << std::endl;
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.displayText() << "\nStack trace:\n" << e.getStackTrace().toString() << "\n";
        return 1;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
