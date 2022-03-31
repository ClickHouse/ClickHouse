#include "JSONFuzzer.h"
#include <DataTypes/Serializations/JSONDataParser.h>
#include <Common/JSONParsers/SimdJSONParser.h>
#include <Common/JSONParsers/RapidJSONParser.h>
#include <Common/FieldVisitorToString.h>
#include <Client/QueryFuzzer.h>
#include <IO/Operators.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NOT_IMPLEMENTED;
        extern const int INCORRECT_DATA;
    }
}

using namespace DB;

static auto getParser()
{
#if USE_SIMDJSON
        return JSONDataParser<SimdJSONParser>();
#elif USE_RAPIDJSON
        return DB::JSONDataParser<DB::RapidJSONParser>>>();
#else
        throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "To use json-fuzzer, it should be build with Simdjson or Rapidjson");
#endif
}

static String getRandomString(pcg64 & rnd, size_t max_size)
{
    if (rnd() % 5 == 0)
        return "";

    size_t size = rnd() % max_size;
    String res;
    res.reserve(size);
    for (size_t i = 0; i < size; ++i)
        res.push_back(rnd() % 26 + 'a');
    return res;
}

static void writeFieldJSON(const Field & field, WriteBuffer & out)
{
    FormatSettings settings;
    settings.json.quote_denormals = true;
    settings.json.quote_64bit_integers = false;
    switch (field.getType())
    {
        case Field::Types::Null:
            writeString("null", out);
            break;
        case Field::Types::String:
            writeJSONString(field.get<const String &>(), out, settings);
            break;
        case Field::Types::UInt64:
            writeJSONNumber(field.get<UInt64>(), out, settings);
            break;
        case Field::Types::Int64:
            writeJSONNumber(field.get<Int64>(), out, settings);
            break;
        case Field::Types::Float64:
            writeJSONNumber(field.get<Float64>(), out, settings);
            break;
        case Field::Types::Decimal64:
            writeJSONNumber(field.get<Decimal64>(), out, settings);
            break;
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported type of field {}", field.getTypeName());
    }
}

bool JSONFuzzer::isValidJSON(const String & json)
{
    auto parser = getParser();
    return parser.parse(json.data(), json.size()) != std::nullopt;
}

String JSONFuzzer::fuzz(const String & json)
{
    auto parser = getParser();
    auto result = parser.parse(json.data(), json.size());
    if (!result)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse json: {}", json);

    Context context;
    DocumentTree tree;

    for (const auto & path : result->paths)
    {
        tree.add(path, EmptyData{});
        const auto & parts = path.getParts();
        for (const auto & part : parts)
            context.existing_keys.insert(String(part.key));
    }

    traverse(tree.getRoot(), context);

    if (config.verbose)
    {
        std::cerr << "stats:\n";
        std::cerr << "\tadded children: " << context.num_added_children << "\n";
        std::cerr << "\trenamed keys: " << context.num_renamed_keys << "\n";
        std::cerr << "\tremoved keys: " << context.num_removed_keys << "\n";
    }

    return generateJSONStringFromTree(tree);
}

void JSONFuzzer::traverse(DocumentTree::Node * node, Context & ctx)
{
    if (node->isScalar())
    {
        fuzzScalar(node, ctx);
        return;
    }

    /// Do not convert root node to scalar.
    if (node->parent && rnd() % 10 == 0)
    {
        makeScalar(node);
        return;
    }

    fuzzChildren(node, ctx);
}

void JSONFuzzer::fuzzScalar(Node * node, Context & ctx)
{
    if (rnd() % 5 == 0)
    {
        node->kind = Node::TUPLE;
        addChildren(node, ctx);
    }
}

void JSONFuzzer::makeScalar(Node * node)
{
    node->kind = Node::SCALAR;
    node->children.clear();
    node->strings_pool.realloc(nullptr, 0, 0);
}

void JSONFuzzer::fuzzChildren(Node * node, Context & ctx)
{
    enum KeyAction
    {
        None,
        Remove,
        Rename,
    };

    std::vector<std::pair<StringRef, KeyAction>> children_actions;
    children_actions.reserve(node->children.size());

    for (const auto & elem : node->children)
    {
        switch (rnd() % 8)
        {
            case 0:
                children_actions.emplace_back(elem.getKey(), KeyAction::Remove);
                break;
            case 1:
                children_actions.emplace_back(elem.getKey(), KeyAction::Rename);
                break;
            default:
                break;
        }
    }

    for (const auto & [key, action] : children_actions)
    {
        switch (action)
        {
            case KeyAction::Remove:
            {
                node->removeChild(key.toView());
                ++ctx.num_removed_keys;
                break;
            }
            case KeyAction::Rename:
            {
                auto child = node->children[key];
                node->removeChild(key.toView());
                node->addChild(takeName(ctx), child);
                ++ctx.num_renamed_keys;
                traverse(child.get(), ctx);
                break;
            }
            case KeyAction::None:
            {
                traverse(node->children[key].get(), ctx);
                break;
            }
        }
    }

    if (rnd() % 4 == 0)
        addChildren(node, ctx);
}

void JSONFuzzer::addChildren(DocumentTree::Node * node, Context & ctx)
{
    if (node->isScalar() || ctx.num_added_children >= config.max_added_children)
    {
        if (node->children.empty())
            makeScalar(node);
        return;
    }

    size_t num_children = rnd() % 5 + 1;
    ctx.num_added_children += num_children;
    for (size_t i = 0; i < num_children; ++i)
    {
        auto kind = magic_enum::enum_cast<Node::Kind>(rnd() % 3);
        assert(kind);

        auto child = std::make_shared<Node>(*kind);
        auto name = takeName(ctx);
        node->addChild(name, child);
        addChildren(child.get(), ctx);
    }
}

String JSONFuzzer::takeName(Context & ctx)
{
    if (!ctx.existing_keys.empty() && rnd() % 20 == 0)
    {
        auto it = ctx.existing_keys.begin();
        std::advance(it, rnd() % ctx.existing_keys.size());
        return *it;
    }

    String name = "key_" + std::to_string(ctx.key_id++);
    ctx.existing_keys.insert(name);
    return name;
}

String JSONFuzzer::generateJSONStringFromTree(const DocumentTree & tree)
{
    WriteBufferFromOwnString out;
    generateJSONStringFromNode(tree.getRoot(), out);
    return out.str();
}

void JSONFuzzer::generateJSONStringFromNode(const Node * node, WriteBuffer & out)
{
    if (node->kind == Node::SCALAR)
    {
        auto field = generateRandomField(rnd() % 3);
        writeFieldJSON(field, out);
    }
    else if (node->kind == Node::TUPLE)
    {
        out << "{";
        bool first = true;
        for (const auto & elem : node->children)
        {
            if (first)
                first = false;
            else
                out << ",";

            writeDoubleQuoted(elem.getKey(), out);
            out << ":";
            generateJSONStringFromNode(elem.getMapped().get(), out);
        }
        out << "}";
    }
    else if (node->kind == Node::NESTED)
    {
        out << "[";
        size_t size = rnd() % config.max_array_size;
        std::unordered_map<std::string_view, size_t> types_by_key;
        for (const auto & elem : node->children)
            types_by_key[elem.getKey().toView()] = rnd() % 3;

        for (size_t i = 0; i < size; ++i)
        {
            if (i > 0)
                out << ",";
            out << "{";

            size_t skip_key_prob = node->children.size() + 1;
            bool first = true;
            for (const auto & elem : node->children)
            {
                const auto & key = elem.getKey();
                const auto * child = elem.getMapped().get();

                size_t tmp = rnd() % skip_key_prob;
                if (tmp == 0)
                    continue;

                if (tmp == 1)
                    types_by_key[key.toView()] = rnd() % 3;

                if (first)
                    first = false;
                else
                    out << ",";

                writeDoubleQuoted(key, out);
                out << ":";

                if (child->isScalar())
                    writeFieldJSON(generateRandomField(types_by_key[key.toView()]), out);
                else
                    generateJSONStringFromNode(child, out);
            }
            out << "}";
        }
        out << "]";
    }
}

Field JSONFuzzer::generateRandomField(size_t type)
{
    if (rnd() % 20 == 0)
        return Field();

    switch (type)
    {
        case 0:
            return getRandomString(rnd, config.max_string_size);
        case 1:
            return getRandomInteger(rnd);
        case 2:
            return getRandomDecimal(rnd);
    }

    return Field();
}
