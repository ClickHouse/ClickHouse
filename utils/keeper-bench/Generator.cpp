#include "Generator.h"
#include "Common/Exception.h"
#include "Common/ZooKeeper/ZooKeeperCommon.h"
#include <Common/Config/ConfigProcessor.h>
#include <random>
#include <filesystem>
#include <Poco/Util/AbstractConfiguration.h>

using namespace Coordination;
using namespace zkutil;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
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

NumberGetter
NumberGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, std::optional<uint64_t> default_value)
{
    NumberGetter number_getter;

    if (!config.has(key) && default_value.has_value())
    {
        number_getter.value = *default_value;
    }
    else if (config.has(key + ".min_value") && config.has(key + ".max_value"))
    {
        NumberRange range{.min_value = config.getUInt64(key + ".min_value"), .max_value = config.getUInt64(key + ".max_value")};
        if (range.max_value <= range.min_value)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Range is invalid for key {}: [{}, {}]", key, range.min_value, range.max_value);
        number_getter.value = range;
    }
    else
    {
        number_getter.value = config.getUInt64(key);
    }

    return number_getter;
}

std::string NumberGetter::description() const
{
    if (const auto * number = std::get_if<uint64_t>(&value))
        return std::to_string(*number);

    const auto & range = std::get<NumberRange>(value);
    return fmt::format("random value from range [{}, {}]", range.min_value, range.max_value);
}

uint64_t NumberGetter::getNumber() const
{
    if (const auto * number = std::get_if<uint64_t>(&value))
        return *number;

    const auto & range = std::get<NumberRange>(value);
    static pcg64 rng(randomSeed());
    return std::uniform_int_distribution<uint64_t>(range.min_value, range.max_value)(rng);
}

StringGetter StringGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    StringGetter string_getter;
    if (config.has(key + ".random_string"))
        string_getter.value
            = NumberGetter::fromConfig(key + ".random_string.size", config);
    else
        string_getter.value = config.getString(key);

    return string_getter;
}

void StringGetter::setString(std::string name)
{
    value = std::move(name);
}

std::string StringGetter::getString() const
{
    if (const auto * string = std::get_if<std::string>(&value))
        return *string;

    const auto number_getter = std::get<NumberGetter>(value);
    return generateRandomString(number_getter.getNumber());
}

std::string StringGetter::description() const
{
    if (const auto * string = std::get_if<std::string>(&value))
        return *string;

    const auto number_getter = std::get<NumberGetter>(value);
    return fmt::format("random string with size of {}", number_getter.description());
}

bool StringGetter::isRandom() const
{
    return std::holds_alternative<NumberGetter>(value);
}

PathGetter PathGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    static constexpr std::string_view path_key_string = "path";

    PathGetter path_getter;
    Poco::Util::AbstractConfiguration::Keys path_keys;
    config.keys(key, path_keys);

    for (const auto & path_key : path_keys)
    {
        if (!path_key.starts_with(path_key_string))
            continue;

        const auto current_path_key_string = key + "." + path_key;
        const auto children_of_key = current_path_key_string + ".children_of";
        if (config.has(children_of_key))
        {
            auto parent_node = config.getString(children_of_key);
            if (parent_node.empty() || parent_node[0] != '/')
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid path for request generator: '{}'", parent_node);
            path_getter.parent_paths.push_back(std::move(parent_node));
        }
        else
        {
            auto path = config.getString(key + "." + path_key);

            if (path.empty() || path[0] != '/')
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid path for request generator: '{}'", path);

            path_getter.paths.push_back(std::move(path));
        }
    }

    path_getter.path_picker = std::uniform_int_distribution<size_t>(0, path_getter.paths.size() - 1);
    return path_getter;
}

void PathGetter::initialize(Coordination::ZooKeeper & zookeeper)
{
    for (const auto & parent_path : parent_paths)
    {
        auto list_promise = std::make_shared<std::promise<ListResponse>>();
        auto list_future = list_promise->get_future();
        auto callback = [list_promise] (const ListResponse & response)
        {
            if (response.error != Coordination::Error::ZOK)
                list_promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
            else
                list_promise->set_value(response);
        };
        zookeeper.list(parent_path, ListRequestType::ALL, std::move(callback), {});
        auto list_response = list_future.get();

        for (const auto & child : list_response.names)
            paths.push_back(std::filesystem::path(parent_path) / child);
    }

    path_picker = std::uniform_int_distribution<size_t>(0, paths.size() - 1);
    initialized = true;
}

std::string PathGetter::getPath() const
{
    if (!initialized)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "PathGetter is not initialized");

    if (paths.size() == 1)
        return paths[0];

    static pcg64 rng(randomSeed());
    return paths[path_picker(rng)];
}

std::string PathGetter::description() const
{
    std::string description;
    for (const auto & path : parent_paths)
    {
        if (!description.empty())
            description += ", ";
        description += fmt::format("children of {}", path);
    }

    for (const auto & path : paths)
    {
        if (!description.empty())
            description += ", ";
        description += path;
    }

    return description;
}

RequestGetter::RequestGetter(std::vector<RequestGeneratorPtr> request_generators_)
    : request_generators(std::move(request_generators_))
{}

RequestGetter RequestGetter::fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, bool for_multi)
{
    RequestGetter request_getter;

    Poco::Util::AbstractConfiguration::Keys generator_keys;
    config.keys(key, generator_keys);

    bool use_weights = false;
    size_t weight_sum = 0;
    auto & generators = request_getter.request_generators;
    for (const auto & generator_key : generator_keys)
    {
        RequestGeneratorPtr request_generator;

        if (generator_key.starts_with("create"))
            request_generator = std::make_unique<CreateRequestGenerator>();
        else if (generator_key.starts_with("set"))
            request_generator = std::make_unique<SetRequestGenerator>();
        else if (generator_key.starts_with("get"))
            request_generator = std::make_unique<GetRequestGenerator>();
        else if (generator_key.starts_with("list"))
            request_generator = std::make_unique<ListRequestGenerator>();
        else if (generator_key.starts_with("multi"))
        {
            if (for_multi)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Nested multi requests are not allowed");
            request_generator = std::make_unique<MultiRequestGenerator>();
        }
        else
        {
            if (for_multi)
                continue;

            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown generator {}", key + "." + generator_key);
        }

        request_generator->getFromConfig(key + "." + generator_key, config);

        auto weight = request_generator->getWeight();
        use_weights |= weight != 1;
        weight_sum += weight;

        generators.push_back(std::move(request_generator));
    }

    if (generators.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "No request generators found in config for key '{}'", key);


    size_t max_value = use_weights ? weight_sum - 1 : generators.size() - 1;
    request_getter.request_generator_picker = std::uniform_int_distribution<size_t>(0, max_value);

    /// construct weight vector
    if (use_weights)
    {
        auto & weights = request_getter.weights;
        weights.reserve(generators.size());
        weights.push_back(generators[0]->getWeight() - 1);

        for (size_t i = 1; i < generators.size(); ++i)
            weights.push_back(weights.back() + generators[i]->getWeight());
    }

    return request_getter;
}

RequestGeneratorPtr RequestGetter::getRequestGenerator() const
{
    static pcg64 rng(randomSeed());

    auto random_number = request_generator_picker(rng);

    if (weights.empty())
        return request_generators[random_number];

    for (size_t i = 0; i < request_generators.size(); ++i)
    {
        if (random_number <= weights[i])
            return request_generators[i];
    }

    throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Invalid number generated: {}", random_number);
}

std::string RequestGetter::description() const
{
    std::string guard(30, '-');
    std::string description = guard;

    for (const auto & request_generator : request_generators)
        description += fmt::format("\n{}\n", request_generator->description());
    return description + guard;
}

void RequestGetter::startup(Coordination::ZooKeeper & zookeeper)
{
    for (const auto & request_generator : request_generators)
        request_generator->startup(zookeeper);
}

const std::vector<RequestGeneratorPtr> & RequestGetter::requestGenerators() const
{
    return request_generators;
}

void RequestGenerator::getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    if (config.has(key + ".weight"))
        weight = config.getUInt64(key + ".weight");
    getFromConfigImpl(key, config);
}

std::string RequestGenerator::description()
{
    std::string weight_string = weight == 1 ? "" : fmt::format("\n- weight: {}", weight);
    return fmt::format("{}{}", descriptionImpl(), weight_string);
}

Coordination::ZooKeeperRequestPtr RequestGenerator::generate(const Coordination::ACLs & acls)
{
    return generateImpl(acls);
}

void RequestGenerator::startup(Coordination::ZooKeeper & zookeeper)
{
    startupImpl(zookeeper);
}

size_t RequestGenerator::getWeight() const
{
    return weight;
}

CreateRequestGenerator::CreateRequestGenerator()
    : rng(randomSeed())
    , remove_picker(0, 1.0)
{}

void CreateRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    parent_path = PathGetter::fromConfig(key, config);

    name = StringGetter(NumberGetter::fromConfig(key + ".name_length", config, 5));

    if (config.has(key + ".data"))
        data = StringGetter::fromConfig(key + ".data", config);

    if (config.has(key + ".remove_factor"))
        remove_factor = config.getDouble(key + ".remove_factor");
}

std::string CreateRequestGenerator::descriptionImpl()
{
    std::string data_string
        = data.has_value() ? fmt::format("data for created nodes: {}", data->description()) : "no data for created nodes";
    std::string remove_factor_string
        = remove_factor.has_value() ? fmt::format("- remove factor: {}", *remove_factor) : "- without removes";
    return fmt::format(
        "Create Request Generator\n"
        "- parent path(s) for created nodes: {}\n"
        "- name for created nodes: {}\n"
        "- {}\n"
        "{}",
        parent_path.description(),
        name.description(),
        data_string,
        remove_factor_string);
}

void CreateRequestGenerator::startupImpl(Coordination::ZooKeeper & zookeeper)
{
    parent_path.initialize(zookeeper);
}

Coordination::ZooKeeperRequestPtr CreateRequestGenerator::generateImpl(const Coordination::ACLs & acls)
{
    if (remove_factor.has_value() && !paths_created.empty() && remove_picker(rng) < *remove_factor)
    {
        auto request = std::make_shared<ZooKeeperRemoveRequest>();
        auto it = paths_created.begin();
        request->path = *it;
        paths_created.erase(it);
        return request;
    }

    auto request = std::make_shared<ZooKeeperCreateRequest>();
    request->acls = acls;

    std::string path_candidate = std::filesystem::path(parent_path.getPath()) / name.getString();

    while (paths_created.contains(path_candidate))
        path_candidate = std::filesystem::path(parent_path.getPath()) / name.getString();

    paths_created.insert(path_candidate);

    request->path = std::move(path_candidate);

    if (data)
        request->data = data->getString();

    return request;
}

void SetRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    path = PathGetter::fromConfig(key, config);

    data = StringGetter::fromConfig(key + ".data", config);
}

std::string SetRequestGenerator::descriptionImpl()
{
    return fmt::format(
        "Set Request Generator\n"
        "- path(s) to set: {}\n"
        "- data to set: {}",
        path.description(),
        data.description());
}

Coordination::ZooKeeperRequestPtr SetRequestGenerator::generateImpl(const Coordination::ACLs & /*acls*/)
{
    auto request = std::make_shared<ZooKeeperSetRequest>();
    request->path = path.getPath();
    request->data = data.getString();
    return request;
}

void SetRequestGenerator::startupImpl(Coordination::ZooKeeper & zookeeper)
{
    path.initialize(zookeeper);
}

void GetRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    path = PathGetter::fromConfig(key, config);
}

std::string GetRequestGenerator::descriptionImpl()
{
    return fmt::format(
        "Get Request Generator\n"
        "- path(s) to get: {}",
        path.description());
}

Coordination::ZooKeeperRequestPtr GetRequestGenerator::generateImpl(const Coordination::ACLs & /*acls*/)
{
    auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = path.getPath();
    return request;
}

void GetRequestGenerator::startupImpl(Coordination::ZooKeeper & zookeeper)
{
    path.initialize(zookeeper);
}

void ListRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    path = PathGetter::fromConfig(key, config);
}

std::string ListRequestGenerator::descriptionImpl()
{
    return fmt::format(
        "List Request Generator\n"
        "- path(s) to get: {}",
        path.description());
}

Coordination::ZooKeeperRequestPtr ListRequestGenerator::generateImpl(const Coordination::ACLs & /*acls*/)
{
    auto request = std::make_shared<ZooKeeperFilteredListRequest>();
    request->path = path.getPath();
    return request;
}

void ListRequestGenerator::startupImpl(Coordination::ZooKeeper & zookeeper)
{
    path.initialize(zookeeper);
}

void MultiRequestGenerator::getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    if (config.has(key + ".size"))
        size = NumberGetter::fromConfig(key + ".size", config);

    request_getter = RequestGetter::fromConfig(key, config, /*for_multi*/ true);
};

std::string MultiRequestGenerator::descriptionImpl()
{
    std::string size_string = size.has_value() ? fmt::format("- number of requests: {}\n", size->description()) : "";
    return fmt::format(
        "Multi Request Generator\n"
        "{}"
        "- requests:\n{}",
        size_string,
        request_getter.description());
}

Coordination::ZooKeeperRequestPtr MultiRequestGenerator::generateImpl(const Coordination::ACLs & acls)
{
    Coordination::Requests ops;

    if (size)
    {
        auto request_count = size->getNumber();

        for (size_t i = 0; i < request_count; ++i)
            ops.push_back(request_getter.getRequestGenerator()->generate(acls));
    }
    else
    {
        for (const auto & request_generator : request_getter.requestGenerators())
            ops.push_back(request_generator->generate(acls));
    }

    return std::make_shared<ZooKeeperMultiRequest>(ops, acls);
}

void MultiRequestGenerator::startupImpl(Coordination::ZooKeeper & zookeeper)
{
    request_getter.startup(zookeeper);
}

Generator::Generator(const Poco::Util::AbstractConfiguration & config)
{
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    static const std::string generator_key = "generator";

    std::cerr << "---- Parsing setup ---- " << std::endl;
    static const std::string setup_key = generator_key + ".setup";
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(setup_key, keys);
    for (const auto & key : keys)
    {
        if (key.starts_with("node"))
        {
            auto node_key = setup_key + "." + key;
            auto parsed_root_node = parseNode(node_key, config);
            const auto node = root_nodes.emplace_back(parsed_root_node);

            if (config.has(node_key + ".repeat"))
            {
                if (!node->name.isRandom())
                    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", node_key);

                auto repeat_count = config.getUInt64(node_key + ".repeat");
                node->repeat_count = repeat_count;
                for (size_t i = 1; i < repeat_count; ++i)
                    root_nodes.emplace_back(node->clone());
            }

            std::cerr << "Tree to create:" << std::endl;

            node->dumpTree();
            std::cerr << std::endl;
        }
    }
    std::cerr << "---- Done parsing data setup ----\n" << std::endl;

    std::cerr << "---- Collecting request generators ----" << std::endl;
    static const std::string requests_key = generator_key + ".requests";
    request_getter = RequestGetter::fromConfig(requests_key, config);
    std::cerr << request_getter.description() << std::endl;
    std::cerr << "---- Done collecting request generators ----\n" << std::endl;
}

std::shared_ptr<Generator::Node> Generator::parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config)
{
    auto node = std::make_shared<Generator::Node>();
    node->name = StringGetter::fromConfig(key + ".name", config);

    if (config.has(key + ".data"))
        node->data = StringGetter::fromConfig(key + ".data", config);

    Poco::Util::AbstractConfiguration::Keys node_keys;
    config.keys(key, node_keys);

    for (const auto & node_key : node_keys)
    {
        if (!node_key.starts_with("node"))
            continue;

        const auto node_key_string = key + "." + node_key;
        auto child_node = parseNode(node_key_string, config);
        node->children.push_back(child_node);

        if (config.has(node_key_string + ".repeat"))
        {
            if (!child_node->name.isRandom())
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Repeating node creation for key {}, but name is not randomly generated", node_key_string);

            auto repeat_count = config.getUInt64(node_key_string + ".repeat");
            child_node->repeat_count = repeat_count;
            for (size_t i = 1; i < repeat_count; ++i)
                node->children.push_back(child_node);
        }
    }

    return node;
}

void Generator::Node::dumpTree(int level) const
{
    std::string data_string
        = data.has_value() ? fmt::format("{}", data->description()) : "no data";

    std::string repeat_count_string = repeat_count != 0 ? fmt::format(", repeated {} times", repeat_count) : "";

    std::cerr << fmt::format("{}name: {}, data: {}{}", std::string(level, '\t'), name.description(), data_string, repeat_count_string) << std::endl;

    for (auto it = children.begin(); it != children.end();)
    {
        const auto & child = *it;
        child->dumpTree(level + 1);
        std::advance(it, child->repeat_count != 0 ? child->repeat_count : 1);
    }
}

std::shared_ptr<Generator::Node> Generator::Node::clone() const
{
    auto new_node = std::make_shared<Node>();
    new_node->name = name;
    new_node->data = data;
    new_node->repeat_count = repeat_count;

    // don't do deep copy of children because we will do clone only for root nodes
    new_node->children = children;

    return new_node;
}

void Generator::Node::createNode(Coordination::ZooKeeper & zookeeper, const std::string & parent_path, const Coordination::ACLs & acls) const
{
    auto path = std::filesystem::path(parent_path) / name.getString();
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    auto create_callback = [promise] (const CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            promise->set_exception(std::make_exception_ptr(zkutil::KeeperException(response.error)));
        else
            promise->set_value();
    };
    zookeeper.create(path, data ? data->getString() : "", false, false, acls, create_callback);
    future.get();

    for (const auto & child : children)
        child->createNode(zookeeper, path, acls);
}

void Generator::startup(Coordination::ZooKeeper & zookeeper)
{
    std::cerr << "---- Creating test data ----" << std::endl;
    for (const auto & node : root_nodes)
    {
        auto node_name = node->name.getString();
        node->name.setString(node_name);

        std::string root_path = std::filesystem::path("/") / node_name;
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path);

        node->createNode(zookeeper, "/", default_acls);
    }
    std::cerr << "---- Created test data ----\n" << std::endl;

    std::cerr << "---- Initializing generators ----" << std::endl;

    request_getter.startup(zookeeper);
}

Coordination::ZooKeeperRequestPtr Generator::generate()
{
    return request_getter.getRequestGenerator()->generate(default_acls);
}

void Generator::cleanup(Coordination::ZooKeeper & zookeeper)
{
    std::cerr << "---- Cleaning up test data ----" << std::endl;
    for (const auto & node : root_nodes)
    {
        auto node_name = node->name.getString();
        std::string root_path = std::filesystem::path("/") / node_name;
        std::cerr << "Cleaning up " << root_path << std::endl;
        removeRecursive(zookeeper, root_path);
    }
}
