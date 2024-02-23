#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Coordination/KeeperStateMachine.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/Exception.h>
#include <libnuraft/nuraft.hxx>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/Changelog.h>
#include <Common/logger_useful.h>
#include <Disks/DiskLocal.h>

using namespace Coordination;
using namespace DB;

void dumpMachine(std::shared_ptr<KeeperStateMachine> machine)
{
    auto & storage = machine->getStorageUnsafe();
    std::queue<std::string> keys;
    keys.push("/");

    while (!keys.empty())
    {
        auto key = keys.front();
        keys.pop();
        std::cout << key << "\n";
        auto value = storage.container.getValue(key);
        std::cout << "\tStat: {version: " << value.stat.version <<
            ", mtime: " << value.stat.mtime <<
            ", emphemeralOwner: " << value.stat.ephemeralOwner <<
            ", czxid: " << value.stat.czxid <<
            ", mzxid: " << value.stat.mzxid <<
            ", numChildren: " << value.stat.numChildren <<
            ", dataLength: " << value.getData().size() <<
            "}" << std::endl;
        std::cout << "\tData: " << storage.container.getValue(key).getData() << std::endl;

        for (const auto & child : value.getChildren())
        {
            if (key == "/")
                keys.push(key + child.toString());
            else
                keys.push(key + "/" + child.toString());
        }
    }
    std::cout << std::flush;
}

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        std::cerr << "usage: " << argv[0] << " snapshotpath logpath" << std::endl;
        return 3;
    }
    else
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel("trace");
    }
    auto * logger = &Poco::Logger::get("keeper-dumper");
    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};
    CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
    KeeperContextPtr keeper_context = std::make_shared<DB::KeeperContext>(true);
    keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", argv[2]));
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", argv[1]));

    auto state_machine = std::make_shared<KeeperStateMachine>(queue, snapshots_queue, settings, keeper_context, nullptr);
    state_machine->init();
    size_t last_commited_index = state_machine->last_commit_index();

    LOG_INFO(logger, "Last committed index: {}", last_commited_index);

    DB::KeeperLogStore changelog(
        LogFileSettings{.force_sync = true, .compress_logs = settings->compress_logs, .rotate_interval = 10000000},
        FlushSettings(),
        keeper_context);
    changelog.init(last_commited_index, 10000000000UL); /// collect all logs
    if (changelog.size() == 0)
        LOG_INFO(logger, "Changelog empty");
    else
        LOG_INFO(logger, "Last changelog entry {}", changelog.next_slot() - 1);

    for (size_t i = last_commited_index + 1; i < changelog.next_slot(); ++i)
    {
        if (changelog.entry_at(i)->get_val_type() == nuraft::log_val_type::app_log)
        {
            state_machine->pre_commit(i, changelog.entry_at(i)->get_buf());
            state_machine->commit(i, changelog.entry_at(i)->get_buf());
        }
    }

    dumpMachine(state_machine);

    return 0;
}
