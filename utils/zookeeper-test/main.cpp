#include <iostream>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/ConsoleChannel.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <common/LineReader.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <iostream>
#include <sstream>
#include <exception>

using namespace std;

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
    catch(...)
    {
        std::cerr << "Got exception on multy request (it's ok)\n";
    }

    checkEq(zk, "/data/multirequest", "bbb");
}

int main(int argc, char *argv[]) {

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
    }
    catch(...)
    {
        zk.tryRemoveRecursive("/data");
        throw;
    }
    return 0;
}
