#include <iostream>

#include <boost/program_options.hpp>

#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/LimitReadBuffer.h>
#include <IO/MySQLBinlogEventReadBuffer.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromOStream.h>
#include <Core/MySQL/MySQLReplication.h>

static DB::MySQLReplication::BinlogEventPtr parseSingleEventBody(
    DB::MySQLReplication::EventHeader & header, DB::ReadBuffer & payload,
    std::shared_ptr<DB::MySQLReplication::TableMapEvent> & last_table_map_event, bool exist_checksum)
{
    DB::MySQLReplication::BinlogEventPtr event;
    DB::ReadBufferPtr limit_read_buffer = std::make_shared<DB::LimitReadBuffer>(payload, header.event_size - 19, false);
    DB::ReadBufferPtr event_payload = std::make_shared<DB::MySQLBinlogEventReadBuffer>(*limit_read_buffer, exist_checksum ? 4 : 0);

    switch (header.type)
    {
        case DB::MySQLReplication::FORMAT_DESCRIPTION_EVENT:
        {
            event = std::make_shared<DB::MySQLReplication::FormatDescriptionEvent>(std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        case DB::MySQLReplication::ROTATE_EVENT:
        {
            event = std::make_shared<DB::MySQLReplication::RotateEvent>(std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        case DB::MySQLReplication::QUERY_EVENT:
        {
            event = std::make_shared<DB::MySQLReplication::QueryEvent>(std::move(header));
            event->parseEvent(*event_payload);

            auto query = std::static_pointer_cast<DB::MySQLReplication::QueryEvent>(event);
            switch (query->typ)
            {
                case DB::MySQLReplication::QUERY_EVENT_MULTI_TXN_FLAG:
                case DB::MySQLReplication::QUERY_EVENT_XA:
                {
                    event = std::make_shared<DB::MySQLReplication::DryRunEvent>(std::move(query->header));
                    break;
                }
                default:
                    break;
            }
            break;
        }
        case DB::MySQLReplication::XID_EVENT:
        {
            event = std::make_shared<DB::MySQLReplication::XIDEvent>(std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        case DB::MySQLReplication::TABLE_MAP_EVENT:
        {
            event = std::make_shared<DB::MySQLReplication::TableMapEvent>(std::move(header));
            event->parseEvent(*event_payload);
            last_table_map_event = std::static_pointer_cast<DB::MySQLReplication::TableMapEvent>(event);
            break;
        }
        case DB::MySQLReplication::WRITE_ROWS_EVENT_V1:
        case DB::MySQLReplication::WRITE_ROWS_EVENT_V2:
        {
            event = std::make_shared<DB::MySQLReplication::WriteRowsEvent>(last_table_map_event, std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        case DB::MySQLReplication::DELETE_ROWS_EVENT_V1:
        case DB::MySQLReplication::DELETE_ROWS_EVENT_V2:
        {
            event = std::make_shared<DB::MySQLReplication::DeleteRowsEvent>(last_table_map_event, std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        case DB::MySQLReplication::UPDATE_ROWS_EVENT_V1:
        case DB::MySQLReplication::UPDATE_ROWS_EVENT_V2:
        {
            event = std::make_shared<DB::MySQLReplication::UpdateRowsEvent>(last_table_map_event, std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        case DB::MySQLReplication::GTID_EVENT:
        {
            event = std::make_shared<DB::MySQLReplication::GTIDEvent>(std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
        default:
        {
            event = std::make_shared<DB::MySQLReplication::DryRunEvent>(std::move(header));
            event->parseEvent(*event_payload);
            break;
        }
    }

    return event;
}

static int checkBinLogFile(const std::string & bin_path, bool exist_checksum)
{
    DB::ReadBufferFromFile in(bin_path);
    DB::assertString("\xfe\x62\x69\x6e", in);   /// magic number

    DB::MySQLReplication::BinlogEventPtr last_event;
    std::shared_ptr<DB::MySQLReplication::EventHeader> last_header;
    std::shared_ptr<DB::MySQLReplication::TableMapEvent> table_map;

    try
    {
        while (!in.eof())
        {
            last_header = std::make_shared<DB::MySQLReplication::EventHeader>();
            last_header->parse(in);
            last_event = parseSingleEventBody(*last_header, in, table_map, exist_checksum);
        }
    }
    catch (...)
    {
        DB::WriteBufferFromOStream cerr(std::cerr);
        cerr << "Unable to parse MySQL binlog event. Code: " << DB::getCurrentExceptionCode() << ", Exception message: "
            << DB::getCurrentExceptionMessage(false) << '\n' << ", Previous event: " << '\n';
        last_event->dump(cerr);
        cerr << '\n' << ", Event header: " << '\n';
        last_header->dump(cerr);
        cerr << '\n';
        return DB::getCurrentExceptionCode();
    }

    DB::WriteBufferFromOStream cout(std::cout);
    cout << "Check passed. " << '\n' << "No exception was thrown." << '\n' << "The last binlog event: " << '\n';
    last_event->dump(cout);
    cout << '\n';
    return 0;
}


int main(int argc, char ** argv)
{
    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help,h", "Produce help message");
    desc.add_options()("disable_checksum", "Disable checksums in binlog files.");

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") || argc < 2)
    {
        std::cout << "Usage: " << argv[0] << " mysql_binlog_file" << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }

    return checkBinLogFile(argv[argc - 1], !options.count("disable_checksum"));
}
