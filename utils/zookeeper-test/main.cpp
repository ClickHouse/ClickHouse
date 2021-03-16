#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/ConsoleChannel.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/LineReader.h>
#include <common/logger_useful.h>
#include <fmt/format.h>
#include <random>
#include <iterator>
#include <algorithm>
#include <chrono>

#include <iostream>
#include <sstream>
#include <exception>
#include <future>

using namespace std;

/// TODO: Remove ME

void checkEq(zkutil::ZooKeeper & zk, const std::string & path, const std::string & expected)
{
    auto result = zk.get(path);
    if (result != expected)
        throw std::runtime_error(fmt::format("Data on path '{}' = '{}' doesn't match expected '{}'",
                                             path, result, expected));
}

void checkExists(zkutil::ZooKeeper & zk, const std::string & path)
{
    if (!zk.exists(path))
        throw std::runtime_error(fmt::format("Path '{}' doesn't exists", path));
}

void testCreateGetExistsNode(zkutil::ZooKeeper & zk)
{
    zk.create("/data", "test_string", zkutil::CreateMode::Persistent);
    zk.create("/data/seq-", "another_string", zkutil::CreateMode::PersistentSequential);
    checkEq(zk, "/data", "test_string");
    checkExists(zk, "/data/seq-0000000000");
    checkEq(zk, "/data/seq-0000000000", "another_string");
}

void testCreateSetNode(zkutil::ZooKeeper & zk)
{
    zk.create("/data/set", "sssss", zkutil::CreateMode::Persistent);
    checkEq(zk, "/data/set", "sssss");
    zk.set("/data/set", "qqqqq");
    checkEq(zk, "/data/set", "qqqqq");
}

void testCreateList(zkutil::ZooKeeper & zk)
{
    zk.create("/data/lst", "", zkutil::CreateMode::Persistent);
    zk.create("/data/lst/d1", "", zkutil::CreateMode::Persistent);
    zk.create("/data/lst/d2", "", zkutil::CreateMode::Persistent);
    zk.create("/data/lst/d3", "", zkutil::CreateMode::Persistent);
    auto children = zk.getChildren("/data/lst");
    if (children.size() != 3)
        throw std::runtime_error("Children of /data/lst doesn't equal to three");
    for (size_t i = 0; i < children.size(); ++i)
    {
        if (children[i] != "d" + std::to_string(i + 1))
            throw std::runtime_error(fmt::format("Incorrect children #{} got {}, expected {}", i, children[i], "d" + std::to_string(i + 1)));
    }
}

void testCreateSetVersionRequest(zkutil::ZooKeeper & zk)
{
    zk.create("/data/check_data", "d", zkutil::CreateMode::Persistent);
    Coordination::Stat stat;
    std::string result = zk.get("/data/check_data", &stat);
    try
    {
        zk.set("/data/check_data", "e", stat.version + 2);
        std::terminate();
    }
    catch (...)
    {
        std::cerr << "Got exception on incorrect version (it's ok)\n";
    }

    checkEq(zk, "/data/check_data", "d");
    zk.set("/data/check_data", "e", stat.version);

    checkEq(zk, "/data/check_data", "e");
}

void testCreateSetWatchEvent(zkutil::ZooKeeper & zk)
{

    std::shared_ptr<Poco::Event> event = std::make_shared<Poco::Event>();
    zk.create("/data/nodeforwatch", "", zkutil::CreateMode::Persistent);
    Coordination::Stat stat;
    zk.get("/data/nodeforwatch", &stat, event);

    if (event->tryWait(300))
        throw std::runtime_error(fmt::format("Event for path {} was set without any actions", "/data/nodeforwatch"));

    zk.set("/data/nodeforwatch", "x");
    if (!event->tryWait(300))
        throw std::runtime_error(fmt::format("Event for path {} was not set after set", "/data/nodeforwatch"));
    else
        std::cerr << "Event was set well\n";
}

void testCreateListWatchEvent(zkutil::ZooKeeper & zk)
{
    std::shared_ptr<Poco::Event> event = std::make_shared<Poco::Event>();
    std::string path = "/data/pathforwatch";
    zk.create(path, "", zkutil::CreateMode::Persistent);
    zk.create(path + "/n1", "", zkutil::CreateMode::Persistent);
    zk.create(path + "/n2", "", zkutil::CreateMode::Persistent);
    zk.getChildren(path, nullptr, event);

    if (event->tryWait(300))
        throw std::runtime_error(fmt::format("ListEvent for path {} was set without any actions", path));

    zk.create(path + "/n3", "", zkutil::CreateMode::Persistent);
    if (!event->tryWait(300))
        throw std::runtime_error(fmt::format("ListEvent for path {} was not set after create", path));
    else
        std::cerr << "ListEvent was set well\n";
}

void testMultiRequest(zkutil::ZooKeeper & zk)
{
    Coordination::Requests requests;
    requests.push_back(zkutil::makeCreateRequest("/data/multirequest", "aaa", zkutil::CreateMode::Persistent));
    requests.push_back(zkutil::makeSetRequest("/data/multirequest", "bbb", -1));
    zk.multi(requests);

    try
    {
        requests.clear();
        requests.push_back(zkutil::makeCreateRequest("/data/multirequest", "qweqwe", zkutil::CreateMode::Persistent));
        requests.push_back(zkutil::makeSetRequest("/data/multirequest", "bbb", -1));
        requests.push_back(zkutil::makeSetRequest("/data/multirequest", "ccc", -1));
        zk.multi(requests);
        std::terminate();
    }
    catch (...)
    {
        std::cerr << "Got exception on multy request (it's ok)\n";
    }

    checkEq(zk, "/data/multirequest", "bbb");
}

std::mutex elements_mutex;
std::vector<int> current_elements;
std::atomic<int> watches_triggered = 0;

void triggerWatch(const Coordination::WatchResponse &)
{
    watches_triggered++;
}

template<typename Iter, typename RandomGenerator>
Iter select_randomly(Iter start, Iter end, RandomGenerator& g)
{
    std::uniform_int_distribution<> dis(0, std::distance(start, end) - 1);
    std::advance(start, dis(g));
    return start;
}

template<typename Iter>
Iter select_randomly(Iter start, Iter end)
{
    static std::random_device rd;
    static std::mt19937 gen(rd());
    return select_randomly(start, end, gen);
}

std::atomic<int> element_counter = 0;
std::atomic<int> failed_setup_counter = 0;

void createPathAndSetWatch(zkutil::ZooKeeper & zk, const String & path_prefix, size_t total)
{
    for (size_t i = 0; i < total; ++i)
    {
        int element = element_counter++;
        zk.createIfNotExists(path_prefix + "/" + std::to_string(element), "");

        std::string result;
        if (!zk.tryGetWatch(path_prefix + "/" + std::to_string(element), result, nullptr, triggerWatch))
            failed_setup_counter++;

        {
            std::lock_guard lock(elements_mutex);
            current_elements.push_back(element);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        {
            std::lock_guard lock(elements_mutex);
            if (current_elements.empty())
                continue;
            element = *select_randomly(current_elements.begin(), current_elements.end());
            current_elements.erase(std::remove(current_elements.begin(), current_elements.end(), element), current_elements.end());
        }
        zk.tryRemove(path_prefix + "/" + std::to_string(element));
    }

}

void tryConcurrentWatches(zkutil::ZooKeeper & zk)
{
    std::string path_prefix = "/concurrent_watches";
    std::vector<std::future<void>> asyncs;
    zk.createIfNotExists(path_prefix, "");
    for (size_t i = 0; i < 100; ++i)
    {
        auto callback = [&zk, path_prefix] ()
        {
            createPathAndSetWatch(zk, path_prefix, 100);
        };
        asyncs.push_back(std::async(std::launch::async, callback));
    }

    for (auto & async : asyncs)
    {
        async.wait();
    }

    size_t counter = 0;
    while (watches_triggered != 100 * 100)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (counter++ > 20)
            break;
    }

    std::cerr << "Failed setup counter:" << failed_setup_counter << std::endl;
    std::cerr << "Current elements size:" << current_elements.size() << std::endl;
    std::cerr << "WatchesTriggered:" << watches_triggered << std::endl;
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        std::cerr << "usage: " << argv[0] << " hosts" << std::endl;
        return 2;
    }
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    zkutil::ZooKeeper zk(argv[1]);

    try
    {
        zk.tryRemoveRecursive("/data");
        testCreateGetExistsNode(zk);
        testCreateSetNode(zk);
        testCreateList(zk);
        testCreateSetVersionRequest(zk);
        testMultiRequest(zk);
        testCreateSetWatchEvent(zk);
        testCreateListWatchEvent(zk);
        tryConcurrentWatches(zk);
    }
    catch (...)
    {
        zk.tryRemoveRecursive("/data");
        throw;
    }
    return 0;
}
