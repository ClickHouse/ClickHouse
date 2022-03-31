#pragma once
#include <base/types.h>
#include <Common/randomSeed.h>
#include <Core/Field.h>
#include <DataTypes/Serializations/SubcolumnsTree.h>
#include <pcg-random/pcg_random.hpp>

class JSONFuzzer
{
public:
    struct Config
    {
        size_t max_added_children = 10;
        size_t max_array_size = 10;
        size_t max_string_size = 10;
        size_t random_seed = randomSeed();
        bool verbose = false;
    };

    explicit JSONFuzzer(const Config & config_)
        : config(config_), rnd(config_.random_seed) {}

    String fuzz(const String & json);
    static bool isValidJSON(const String & json);

private:
    struct EmptyData{};
    using DocumentTree = DB::SubcolumnsTree<EmptyData>;
    using Node = DocumentTree::Node;

    struct Context
    {
        DB::NameSet existing_keys;
        size_t num_added_children = 0;
        size_t num_renamed_keys = 0;
        size_t num_removed_keys = 0;
        size_t key_id = 0;
    };

    static void makeScalar(Node * node);

    void traverse(Node * node, Context & ctx);
    void fuzzScalar(Node * node, Context & ctx);
    void fuzzChildren(Node * node, Context & ctx);
    void addChildren(Node * node, Context & ctx);
    String takeName(Context & ctx);

    String generateJSONStringFromTree(const DocumentTree & tree);
    void generateJSONStringFromNode(const Node * node, DB::WriteBuffer & out);
    DB::Field generateRandomField(size_t type);
    String generateRandomString();

    Config config;
    pcg64 rnd;
};
