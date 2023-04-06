#pragma once
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <functional>
#include <optional>
#include <pcg-random/pcg_random.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/randomSeed.h>


std::string generateRandomPath(const std::string & prefix, size_t length = 5);

std::string generateRandomData(size_t size);

//
//class CreateRequestGenerator final : public IGenerator
//{
//public:
//    explicit CreateRequestGenerator(
//        std::string path_prefix_ = "/create_generator",
//        std::optional<uint64_t> path_length_ = std::nullopt,
//        std::optional<uint64_t> data_size_ = std::nullopt)
//        : path_prefix(path_prefix_)
//        , path_length(path_length_)
//        , data_size(data_size_)
//    {}
//
//    void startup(Coordination::ZooKeeper & zookeeper) override;
//    Coordination::ZooKeeperRequestPtr generate() override;
//
//private:
//    std::string path_prefix;
//    std::optional<uint64_t> path_length;
//    std::optional<uint64_t> data_size;
//    std::unordered_set<std::string> paths_created;
//};
//
//
//class GetRequestGenerator final : public IGenerator
//{
//public:
//    explicit GetRequestGenerator(
//        std::string path_prefix_ = "/get_generator",
//        std::optional<uint64_t> num_nodes_ = std::nullopt,
//        std::optional<uint64_t> nodes_data_size_ = std::nullopt)
//        : path_prefix(path_prefix_)
//        , num_nodes(num_nodes_)
//        , nodes_data_size(nodes_data_size_)
//        , rng(randomSeed())
//        , distribution(0, num_nodes ? *num_nodes - 1 : 0)
//    {}
//
//    void startup(Coordination::ZooKeeper & zookeeper) override;
//    Coordination::ZooKeeperRequestPtr generate() override;
//
//private:
//    std::string path_prefix;
//    std::optional<uint64_t> num_nodes;
//    std::optional<uint64_t> nodes_data_size;
//    std::vector<std::string> paths_to_get;
//
//    pcg64 rng;
//    std::uniform_int_distribution<size_t> distribution;
//};
//
//class ListRequestGenerator final : public IGenerator
//{
//public:
//    explicit ListRequestGenerator(
//        std::string path_prefix_ = "/list_generator",
//        std::optional<uint64_t> num_nodes_ = std::nullopt,
//        std::optional<uint64_t> paths_length_ = std::nullopt)
//        : path_prefix(path_prefix_)
//        , num_nodes(num_nodes_)
//        , paths_length(paths_length_)
//    {}
//
//    void startup(Coordination::ZooKeeper & zookeeper) override;
//    Coordination::ZooKeeperRequestPtr generate() override;
//
//private:
//    std::string path_prefix;
//    std::optional<uint64_t> num_nodes;
//    std::optional<uint64_t> paths_length;
//};
//
//class SetRequestGenerator final : public IGenerator
//{
//public:
//    explicit SetRequestGenerator(
//        std::string path_prefix_ = "/set_generator",
//        uint64_t data_size_ = 5)
//        : path_prefix(path_prefix_)
//        , data_size(data_size_)
//    {}
//
//    void startup(Coordination::ZooKeeper & zookeeper) override;
//    Coordination::ZooKeeperRequestPtr generate() override;
//
//private:
//    std::string path_prefix;
//    uint64_t data_size;
//};
//
//class MixedRequestGenerator final : public IGenerator
//{
//public:
//    explicit MixedRequestGenerator(std::vector<std::unique_ptr<IGenerator>> generators_)
//        : generators(std::move(generators_))
//    {}
//
//    void startup(Coordination::ZooKeeper & zookeeper) override;
//    Coordination::ZooKeeperRequestPtr generate() override;
//
//private:
//    std::vector<std::unique_ptr<IGenerator>> generators;
//};

struct NumberGetter
{
    static NumberGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config, std::optional<uint64_t> default_value = std::nullopt);
    uint64_t getNumber() const;
    std::string description() const;
private:
    struct NumberRange
    {
        uint64_t min_value;
        uint64_t max_value;
    };

    std::variant<uint64_t, NumberRange> value;
};

struct StringGetter
{
    explicit StringGetter(NumberGetter number_getter)
        : value(std::move(number_getter))
    {}

    StringGetter() = default;

    static StringGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);
    void setString(std::string name);
    std::string getString() const;
    std::string description() const;
    bool isRandom() const;
private:
    std::variant<std::string, NumberGetter> value;
};

struct PathGetter
{
    static PathGetter fromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::string getPath() const;
    std::string description() const;

    void initialize(Coordination::ZooKeeper & zookeeper);
private:
    std::vector<std::string> parent_paths;

    bool initialized = false;

    std::vector<std::string> paths;
    mutable std::uniform_int_distribution<size_t> path_picker;
};

struct RequestGenerator
{
    virtual ~RequestGenerator() = default;

    void getFromConfig(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    Coordination::ZooKeeperRequestPtr generate(const Coordination::ACLs & acls);

    std::string description();

    void startup(Coordination::ZooKeeper & zookeeper);
private:
    virtual void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) = 0;
    virtual std::string descriptionImpl() = 0;
    virtual Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) = 0;
    virtual void startupImpl(Coordination::ZooKeeper &) {}
};

using RequestGeneratorPtr = std::unique_ptr<RequestGenerator>;

struct CreateRequestGenerator final : public RequestGenerator
{
    CreateRequestGenerator();
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter parent_path;
    StringGetter name;
    std::optional<StringGetter> data;

    double remove_factor;
    pcg64 rng;
    std::uniform_real_distribution<double> remove_picker;

    std::unordered_set<std::string> paths_created;
};

struct SetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter path;
    StringGetter data;
};

struct GetRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter path;
};

struct ListRequestGenerator final : public RequestGenerator
{
private:
    void getFromConfigImpl(const std::string & key, const Poco::Util::AbstractConfiguration & config) override;
    std::string descriptionImpl() override;
    Coordination::ZooKeeperRequestPtr generateImpl(const Coordination::ACLs & acls) override;
    void startupImpl(Coordination::ZooKeeper & zookeeper) override;

    PathGetter path;
};

class Generator
{
public:
    explicit Generator(const Poco::Util::AbstractConfiguration & config);

    void startup(Coordination::ZooKeeper & zookeeper);
    Coordination::ZooKeeperRequestPtr generate();
private:
    struct Node
    {
        StringGetter name;
        std::optional<StringGetter> data;
        std::vector<std::shared_ptr<Node>> children;

        void createNode(Coordination::ZooKeeper & zookeeper, const std::string & parent_path, const Coordination::ACLs & acls) const;
        void dumpTree(int level = 0) const;
    };

    static std::shared_ptr<Node> parseNode(const std::string & key, const Poco::Util::AbstractConfiguration & config);

    std::uniform_int_distribution<size_t> request_picker;
    std::vector<std::shared_ptr<Node>> root_nodes;
    std::vector<RequestGeneratorPtr> request_generators;
    Coordination::ACLs default_acls;
};

std::unique_ptr<Generator> getGenerator(const std::string & name);
