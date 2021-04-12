#include "Generator.h"
#include <random>
#include <filesystem>

using namespace Coordination;
using namespace zkutil;

namespace
{
std::string generateRandomString(size_t length)
{
    static const auto & chrs = "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    thread_local static std::mt19937 rg{std::random_device{}()};
    thread_local static std::uniform_int_distribution<std::string::size_type> pick(0, sizeof(chrs) - 2);

    std::string s;

    s.reserve(length);

    while(length--)
    {
        s += chrs[pick(rg)];
    }

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

ZooKeeperRequestPtr CreateRequestGenerator::generate()
{
    auto request = std::make_shared<ZooKeeperCreateRequest>();
    size_t plength = 5;
    if (path_length)
        plength = *path_length;
    auto path_candidate = generateRandomPath(path_prefix, plength);

    while(paths_created.count(path_candidate))
        path_candidate = generateRandomPath(path_prefix, plength);

    paths_created.insert(path_candidate);

    request->path = path_candidate;
    if (data_size)
        request->data = generateRandomData(*data_size);

    return request;
}
