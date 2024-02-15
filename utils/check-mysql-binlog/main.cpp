#include <IO/WriteBufferFromOStream.h>
#include <Databases/MySQL/MySQLBinlog.h>
#include <boost/program_options.hpp>
#include <iostream>

bool quit = false;
void signal_handler(int)
{
    quit = true;
}

static void processBinlogFromFile(const std::string & bin_path, bool disable_checksum)
{
    DB::MySQLReplication::BinlogFromFile binlog;
    binlog.open(bin_path);
    binlog.setChecksum(disable_checksum ? DB::MySQLReplication::IBinlog::NONE : DB::MySQLReplication::IBinlog::CRC32);

    DB::MySQLReplication::BinlogEventPtr event;
    while (binlog.tryReadEvent(event, /*timeout*/ 0) && !quit)
    {
        DB::WriteBufferFromOStream cout(std::cout);
        event->dump(cout);
        binlog.getPosition().dump(cout);
        cout.finalize();
    }
}

static void processBinlogFromSocket(const std::string & host, int port, const std::string & user, const std::string & password, const std::string & executed_gtid_set, bool disable_checksum)
{
    DB::MySQLReplication::BinlogFromSocket binlog;
    binlog.setChecksum(disable_checksum ? DB::MySQLReplication::IBinlog::NONE : DB::MySQLReplication::IBinlog::CRC32);

    binlog.connect(host, port, user, password);
    binlog.start(/*unique number*/ 42, executed_gtid_set);
    DB::MySQLReplication::BinlogEventPtr event;

    while (!quit)
    {
        if (binlog.tryReadEvent(event, /*timeout*/ 100))
        {
            if (event->header.type != DB::MySQLReplication::HEARTBEAT_EVENT)
            {
                DB::WriteBufferFromOStream cout(std::cout);
                event->dump(cout);
                binlog.getPosition().dump(cout);
                cout.finalize();
            }
        }
    }
}

int main(int argc, char ** argv)
{
    (void)signal(SIGINT, signal_handler);
    boost::program_options::options_description desc("Allowed options");

    std::string host = "127.0.0.1";
    int port = 3306;
    std::string user = "root";
    std::string password;
    std::string gtid;

    desc.add_options()
        ("help", "Produce help message")
        ("disable_checksum", "Disable checksums in binlog files.")
        ("binlog", boost::program_options::value<std::string>(), "Binlog file")
        ("host", boost::program_options::value<std::string>(&host)->default_value(host), "Host to connect")
        ("port", boost::program_options::value<int>(&port)->default_value(port), "Port number to connect")
        ("user", boost::program_options::value<std::string>(&user)->default_value(user), "User")
        ("password", boost::program_options::value<std::string>(&password), "Password")
        ("gtid", boost::program_options::value<std::string>(&gtid), "Executed gtid set");

    try
    {
        boost::program_options::variables_map options;
        boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);
        boost::program_options::notify(options);

        if (options.count("help") || (!options.count("binlog") && !options.count("gtid")))
        {
            std::cout << "Usage: " << argv[0] << std::endl;
            std::cout << desc << std::endl;
            return EXIT_FAILURE;
        }

        if (options.count("binlog"))
            processBinlogFromFile(options["binlog"].as<std::string>(), options.count("disable_checksum"));
        else
            processBinlogFromSocket(host, port, user, password, gtid, options.count("disable_checksum"));
    }
    catch (std::exception & ex)
    {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
