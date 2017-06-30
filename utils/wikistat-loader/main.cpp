#include <boost/program_options.hpp>

#include <Common/hex.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>


/** Reads uncompressed wikistat data from stdin,
  *  and writes transformed data in tsv format,
  *  ready to be loaded into ClickHouse.
  *
  * Input data has format:
  *
  * aa Wikipedia 1 17224
  * aa.b Main_Page 2 21163
  *
  * project, optional subproject, path, hits, total size in bytes.
  */


template <bool break_at_dot>
static void readString(std::string & s, DB::ReadBuffer & buf)
{
    s.clear();

    while (!buf.eof())
    {
        const char * next_pos;

        if (break_at_dot)
            next_pos = find_first_symbols<' ', '\n', '.'>(buf.position(), buf.buffer().end());
        else
            next_pos = find_first_symbols<' ', '\n'>(buf.position(), buf.buffer().end());

        s.append(buf.position(), next_pos - buf.position());
        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == ' ' || *buf.position() == '\n' || (break_at_dot && *buf.position() == '.'))
            return;
    }
}


/** Reads path before whitespace and decodes %xx sequences (to more compact and handy representation),
  *  except %2F '/', %26 '&', %3D '=', %3F '?', %23 '#' (to not break structure of URL).
  */
static void readPath(std::string & s, DB::ReadBuffer & buf)
{
    s.clear();

    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<' ', '\n', '%'>(buf.position(), buf.buffer().end());

        s.append(buf.position(), next_pos - buf.position());
        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == ' ' || *buf.position() == '\n')
            return;

        if (*buf.position() == '%')
        {
            ++buf.position();

            char c1;
            char c2;

            if (buf.eof() || *buf.position() == ' ')
                break;

            DB::readChar(c1, buf);

            if (buf.eof() || *buf.position() == ' ')
                break;

            DB::readChar(c2, buf);

            if ((c1 == '2' && (c2 == 'f' || c2 == '6' || c2 == '3' || c2 == 'F'))
                || (c1 == '3' && (c2 == 'd' || c2 == 'f' || c2 == 'D' || c2 == 'F')))
            {
                s += '%';
                s += c1;
                s += c2;
            }
            else
                s += static_cast<char>(static_cast<UInt8>(unhex(c1)) * 16 + static_cast<UInt8>(unhex(c2)));
        }
    }
}


static void skipUntilNewline(DB::ReadBuffer & buf)
{
    while (!buf.eof())
    {
        const char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());

        buf.position() += next_pos - buf.position();

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}


namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    }
}


int main(int argc, char ** argv)
try
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("time", boost::program_options::value<std::string>()->required(),
            "time of data in YYYY-MM-DD hh:mm:ss form")
    ;

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << "Reads uncompressed wikistat data from stdin and writes transformed data in tsv format." << std::endl;
        std::cout << "Usage: " << argv[0] << " --time='YYYY-MM-DD hh:00:00' < in > out" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    std::string time_str = options.at("time").as<std::string>();
    LocalDateTime time(time_str);
    LocalDate date(time);

    DB::ReadBufferFromFileDescriptor in(STDIN_FILENO);
    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    std::string project;
    std::string subproject;
    std::string path;
    UInt64 hits = 0;
    UInt64 size = 0;

    size_t row_num = 0;
    while (!in.eof())
    {
        try
        {
            ++row_num;
            readString<true>(project, in);

            if (in.eof())
                break;

            if (*in.position() == '.')
                readString<false>(subproject, in);
            else
                subproject.clear();

            DB::assertChar(' ', in);
            readPath(path, in);
            DB::assertChar(' ', in);
            DB::readIntText(hits, in);
            DB::assertChar(' ', in);
            DB::readIntText(size, in);
            DB::assertChar('\n', in);
        }
        catch (const DB::Exception & e)
        {
            /// Sometimes, input data has errors. For example, look at first lines in pagecounts-20130210-130000.gz
            /// To save rest of data, just skip lines with errors.
            if (e.code() == DB::ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
            {
                std::cerr << "At row " << row_num << ": " << DB::getCurrentExceptionMessage(false) << '\n';
                skipUntilNewline(in);
                continue;
            }
            else
                throw;
        }

        DB::writeText(date, out);
        DB::writeChar('\t', out);
        DB::writeText(time, out);
        DB::writeChar('\t', out);
        DB::writeText(project, out);
        DB::writeChar('\t', out);
        DB::writeText(subproject, out);
        DB::writeChar('\t', out);
        DB::writeText(path, out);
        DB::writeChar('\t', out);
        DB::writeText(hits, out);
        DB::writeChar('\t', out);
        DB::writeText(size, out);
        DB::writeChar('\n', out);
    }

    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
