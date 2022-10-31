#include "Generator.h"
#include <random>
#include <filesystem>

using namespace Coordination;
using namespace zkutil;

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{
std::string generateRandomString(size_t length)
{
    if (length == 0)
        return "";

    static const auto & chars = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    static pcg64 rng(randomSeed());
    static std::uniform_int_distribution<size_t> pick(0, sizeof(chars) - 2);

    std::string s;

    s.reserve(length);

    while (length--)
        s += chars[pick(rng)];

    return s;
}
}

std::string generateRandomPath(const std::string & prefix, size_t length)
{
    return std::filesystem::path(prefix) / generateRandomString(length);
}

std::string generateRandomData(size_t size)
{
    return generateRandomString(size);
}

void removeRecursive(Coordination::ZooKeeper & zookeeper, const std::string & path)
{
    namespace fs = std::filesystem;

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    Strings children;
    auto list_callback = [promise, &children] (const ListResponse & response)
    {
        children = response.names;

        promise->set_value();
    };
    zookeeper.list(path, ListRequestType::ALL, list_callback, nullptr);
    future.get();

    while (!children.empty())
    {
        Coordination::Requests ops;
        for (size_t i = 0; i < MULTI_BATCH_SIZE && !children.empty(); ++i)
        {
            removeRecursive(zookeeper, fs::path(path) / children.back());
            ops.emplace_back(makeRemoveRequest(fs::path(path) / children.back(), -1));
            children.pop_back();
        }
        auto multi_promise = std::make_shared<std::promise<void>>();
        auto multi_future = multi_promise->get_future();

        auto multi_callback = [multi_promise] (const MultiResponse &)
        {
            multi_promise->set_value();
        };
        zookeeper.multi(ops, multi_callback);
        multi_future.get();
    }
    auto remove_promise = std::make_shared<std::promise<void>>();
    auto remove_future = remove_promise->get_future();

    auto remove_callback = [remove_promise] (const RemoveResponse &)
    {
        remove_promise->set_value();
    };

    zookeeper.remove(path, -1, remove_callback);
    remove_future.get();
}


void CreateRequestGenerator::startup(Coordination::ZooKeeper & zookeeper)
{
    removeRecursive(zookeeper, path_prefix);

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto create_callback = [promise] (const CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        else
            promise->set_value();
    };
    zookeeper.create(path_prefix, "", false, false, default_acls, create_callback);
    future.get();
}

ZooKeeperRequestPtr CreateRequestGenerator::generate()
{
    auto request = std::make_shared<ZooKeeperCreateRequest>();
    request->acls = default_acls;
    size_t plength = 5;
    if (path_length)
        plength = *path_length;
    auto path_candidate = generateRandomPath(path_prefix, plength);

    while (paths_created.contains(path_candidate))
        path_candidate = generateRandomPath(path_prefix, plength);

    paths_created.insert(path_candidate);

    request->path = path_candidate;
    if (data_size)
        request->data = generateRandomData(*data_size);

    return request;
}


void SetRequestGenerator::startup(Coordination::ZooKeeper & zookeeper)
{
    removeRecursive(zookeeper, path_prefix);

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto create_callback = [promise] (const CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        else
            promise->set_value();
    };
    zookeeper.create(path_prefix, "", false, false, default_acls, create_callback);
    future.get();
}

ZooKeeperRequestPtr SetRequestGenerator::generate()
{
    auto request = std::make_shared<ZooKeeperSetRequest>();
    request->path = path_prefix;
    request->data = generateRandomData(data_size);

    return request;
}


void GetRequestGenerator::startup(Coordination::ZooKeeper & zookeeper)
{
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto create_callback = [promise] (const CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        else
            promise->set_value();
    };
    zookeeper.create(path_prefix, "", false, false, default_acls, create_callback);
    future.get();
    size_t total_nodes = 1;
    if (num_nodes)
        total_nodes = *num_nodes;

    for (size_t i = 0; i < total_nodes; ++i)
    {
        auto path = generateRandomPath(path_prefix, 5);
        while (std::find(paths_to_get.begin(), paths_to_get.end(), path) != paths_to_get.end())
            path = generateRandomPath(path_prefix, 5);

        auto create_promise = std::make_shared<std::promise<void>>();
        auto create_future = create_promise->get_future();
        auto callback = [create_promise] (const CreateResponse & response)
        {
            if (response.error != Coordination::Error::ZOK)
                create_promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
            else
                create_promise->set_value();
        };
        std::string data;
        if (nodes_data_size)
            data = generateRandomString(*nodes_data_size);

        zookeeper.create(path, data, false, false, default_acls, callback);
        create_future.get();
        paths_to_get.push_back(path);
    }
}

Coordination::ZooKeeperRequestPtr GetRequestGenerator::generate()
{
    auto request = std::make_shared<ZooKeeperGetRequest>();

    size_t path_index = distribution(rng);
    request->path = paths_to_get[path_index];
    return request;
}

void ListRequestGenerator::startup(Coordination::ZooKeeper & zookeeper)
{
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto create_callback = [promise] (const CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        else
            promise->set_value();
    };
    zookeeper.create(path_prefix, "", false, false, default_acls, create_callback);
    future.get();

    size_t total_nodes = 1;
    if (num_nodes)
        total_nodes = *num_nodes;

    size_t path_length = 5;
    if (paths_length)
        path_length = *paths_length;

    for (size_t i = 0; i < total_nodes; ++i)
    {
        auto path = generateRandomPath(path_prefix, path_length);

        auto create_promise = std::make_shared<std::promise<void>>();
        auto create_future = create_promise->get_future();
        auto callback = [create_promise] (const CreateResponse & response)
        {
            if (response.error != Coordination::Error::ZOK)
                create_promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
            else
                create_promise->set_value();
        };
        zookeeper.create(path, "", false, false, default_acls, callback);
        create_future.get();
    }
}

Coordination::ZooKeeperRequestPtr ListRequestGenerator::generate()
{
    auto request = std::make_shared<ZooKeeperListRequest>();
    request->path = path_prefix;
    return request;
}

std::unique_ptr<IGenerator> getGenerator(const std::string & name)
{
    if (name == "create_no_data")
    {
        return std::make_unique<CreateRequestGenerator>();
    }
    else if (name == "create_small_data")
    {
        return std::make_unique<CreateRequestGenerator>("/create_generator", 5, 32);
    }
    else if (name == "create_medium_data")
    {
        return std::make_unique<CreateRequestGenerator>("/create_generator", 5, 1024);
    }
    else if (name == "create_big_data")
    {
        return std::make_unique<CreateRequestGenerator>("/create_generator", 5, 512 * 1024);
    }
    else if (name == "get_no_data")
    {
        return std::make_unique<GetRequestGenerator>("/get_generator", 10, 0);
    }
    else if (name == "get_small_data")
    {
        return std::make_unique<GetRequestGenerator>("/get_generator", 10, 32);
    }
    else if (name == "get_medium_data")
    {
        return std::make_unique<GetRequestGenerator>("/get_generator", 10, 1024);
    }
    else if (name == "get_big_data")
    {
        return std::make_unique<GetRequestGenerator>("/get_generator", 10, 512 * 1024);
    }
    else if (name == "list_no_nodes")
    {
        return std::make_unique<ListRequestGenerator>("/list_generator", 0, 1);
    }
    else if (name == "list_few_nodes")
    {
        return std::make_unique<ListRequestGenerator>("/list_generator", 10, 5);
    }
    else if (name == "list_medium_nodes")
    {
        return std::make_unique<ListRequestGenerator>("/list_generator", 1000, 5);
    }
    else if (name == "list_a_lot_nodes")
    {
        return std::make_unique<ListRequestGenerator>("/list_generator", 100000, 5);
    }
    else if (name == "set_small_data")
    {
        return std::make_unique<SetRequestGenerator>("/set_generator", 5);
    }


    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unknown generator {}", name);
}
