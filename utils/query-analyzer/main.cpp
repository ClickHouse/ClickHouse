#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

#include <unistd.h>

#include <boost/program_options.hpp>

#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>

#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Analyzer/Passes/QueryAnalysisPass.h>
#include <Analyzer/QueryTreeBuilder.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/registerDatabases.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/registerInterpreters.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/QueryScope.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils.h>
#include <Common/ThreadStatus.h>

namespace po = boost::program_options;
namespace fs = std::filesystem;

using namespace DB;

namespace
{

ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

/// Mirrors the minimal context bootstrap from src/Interpreters/fuzzers/execute_query_fuzzer.cpp.
ContextMutablePtr createGlobalContext()
{
    static SharedContextHolder shared_context = Context::createShared(); /// static: the shared context must outlive the returned context pointer.
    auto context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();
    context->setConfig(getConfigurationFromXMLString("<clickhouse></clickhouse>"));

    fs::path work_dir = fs::temp_directory_path() / ("query-analyzer-" + std::to_string(getpid()));
    fs::create_directories(work_dir);
    /// The directory is intentionally left behind on exit, so data of setup-created tables can be inspected.
    context->setPath(work_dir.string() + "/");
    context->setTemporaryStoragePath((work_dir / "tmp" / "").string(), 0);

    /// Caches are needed if setup statements create and read MergeTree tables.
    context->setMarkCache("SLRU", 128 * 1024 * 1024, 0.5);
    context->setUncompressedCache("SLRU", 128 * 1024 * 1024, 0.5);

    MainThreadStatus::getInstance();
    getActivePartsLoadingThreadPool().initialize(4, 0, 100);

    registerInterpreters();
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();

    /// A SELECT without FROM is resolved against system.one, so the system database must exist
    /// (unlike in the fuzzer, which goes through executeQuery and may not hit the analyzer).
    DatabasePtr system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, context);
    if (UUID uuid = system_database->getUUID(); uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().addUUIDMapping(uuid);
    DatabaseCatalog::instance().attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
    attachSystemTablesServer(context, *system_database, /* has_zookeeper= */ false, /* has_keeper_server= */ false);

    const std::string default_database = "default";
    DatabasePtr database = std::make_shared<DatabaseMemory>(default_database, context);
    if (UUID uuid = database->getUUID(); uuid != UUIDHelpers::Nil)
        DatabaseCatalog::instance().addUUIDMapping(uuid);
    DatabaseCatalog::instance().attachDatabase(default_database, database);
    context->setCurrentDatabase(default_database);

    return context;
}

/// Splits the input into ';'-separated statements, keeping both the AST and the original text slice.
std::vector<std::pair<ASTPtr, std::string>> splitStatements(const std::string & queries_text)
{
    std::vector<std::pair<ASTPtr, std::string>> statements;

    const char * pos = queries_text.data();
    const char * end = pos + queries_text.size();
    ParserQuery parser(end);

    while (pos < end)
    {
        while (pos < end && (isWhitespaceASCII(*pos) || *pos == ';'))
            ++pos;
        if (pos >= end)
            break;

        const char * begin = pos;
        ASTPtr ast = parseQueryAndMovePosition(
            parser, pos, end, "query", /* allow_multi_statements= */ true,
            DBMS_DEFAULT_MAX_QUERY_SIZE, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

        /// An INSERT with inline data stops parsing at the beginning of the data. The data extends
        /// to the end of the line, same convention as in splitMultipartQuery.
        if (auto * insert = ast->as<ASTInsertQuery>(); insert && insert->data)
        {
            pos = insert->data;
            while (pos < end && *pos != '\n')
                ++pos;
            insert->end = pos;
        }

        statements.emplace_back(ast, std::string(begin, pos));
    }

    return statements;
}

void executeSetupStatement(const std::string & statement, ContextMutablePtr global_context)
{
    auto query_context = Context::createCopy(global_context);
    query_context->makeQueryContext();
    query_context->setCurrentQueryId({});

    QueryScope query_scope;
    if (!CurrentThread::getGroup())
        query_scope = QueryScope::create(query_context);

    auto io = executeQuery(statement, query_context, QueryFlags{.internal = true}, QueryProcessingStage::Complete).second;
    io.executeWithCallbacks([&]
    {
        if (io.pipeline.pulling())
        {
            PullingPipelineExecutor executor(io.pipeline);
            Block block;
            while (executor.pull(block))
                ;
        }
        else if (io.pipeline.completed())
        {
            CompletedPipelineExecutor executor(io.pipeline);
            executor.execute();
        }
        /// DDL statements have no pipeline, nothing to execute.
    });
}

void printTimings(std::vector<UInt64> times_ns, UInt64 build_ns)
{
    auto to_ms = [](UInt64 ns) { return static_cast<double>(ns) / 1e6; };

    UInt64 total = std::accumulate(times_ns.begin(), times_ns.end(), UInt64{0});
    std::sort(times_ns.begin(), times_ns.end());
    auto quantile = [&](double q)
    {
        return times_ns[static_cast<size_t>(q * static_cast<double>(times_ns.size() - 1))];
    };

    std::cout << std::fixed << std::setprecision(3)
              << "Query tree build time: " << to_ms(build_ns) << " ms\n"
              << "Analysis iterations: " << times_ns.size() << '\n'
              << "  total:  " << to_ms(total) << " ms\n"
              << "  avg:    " << to_ms(total) / static_cast<double>(times_ns.size()) << " ms\n"
              << "  min:    " << to_ms(times_ns.front()) << " ms\n"
              << "  median: " << to_ms(quantile(0.5)) << " ms\n"
              << "  p90:    " << to_ms(quantile(0.9)) << " ms\n"
              << "  max:    " << to_ms(times_ns.back()) << " ms\n";
}

}

int main(int argc, char ** argv)
try
{
    po::options_description description("query-analyzer options");
    description.add_options()
        ("help,h", "print usage and exit")
        ("query", po::value<std::string>(), "SQL text; if omitted, read from stdin. "
            "May contain multiple ';'-separated statements: all but the last are executed as setup "
            "(CREATE TABLE, INSERT, SET, ...), the last one must be a SELECT and is analyzed")
        ("iterations,n", po::value<size_t>()->default_value(1), "number of analysis iterations")
        ("only-analyze", "do not execute scalar subqueries during analysis")
        ("dump-tree", "dump the resolved query tree after the last iteration")
        ("dump-ast", "dump the resolved query tree converted back to AST")
        ("setting", po::value<std::vector<std::string>>()->composing(), "key=value pair applied to the context settings (repeatable)");

    po::positional_options_description positional;
    positional.add("query", 1);

    po::variables_map options;
    po::store(po::command_line_parser(argc, argv).options(description).positional(positional).run(), options);
    po::notify(options);

    if (options.contains("help"))
    {
        std::cout << "Runs QueryAnalysisPass on a SELECT query in a loop and reports timings.\n"
                  << "Usage: query-analyzer [options] [query]\n" << description << '\n';
        return 0;
    }

    const size_t iterations = options["iterations"].as<size_t>();
    if (iterations == 0)
    {
        std::cerr << "--iterations must be at least 1\n";
        return 1;
    }

    std::string queries_text;
    if (options.contains("query"))
    {
        queries_text = options["query"].as<std::string>();
    }
    else
    {
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        readStringUntilEOF(queries_text, in);
    }

    auto global_context = createGlobalContext();

    if (options.contains("setting"))
    {
        Settings settings;
        for (const auto & key_value : options["setting"].as<std::vector<std::string>>())
        {
            size_t eq_pos = key_value.find('=');
            if (eq_pos == std::string::npos)
            {
                std::cerr << "Invalid --setting value '" << key_value << "', expected key=value\n";
                return 1;
            }
            settings.set(key_value.substr(0, eq_pos), key_value.substr(eq_pos + 1));
        }
        global_context->setSettings(settings);
    }

    auto statements = splitStatements(queries_text);
    if (statements.empty())
    {
        std::cerr << "No queries to analyze\n";
        return 1;
    }

    /// All statements except the last one are setup. SET statements must be applied to the
    /// global context directly, because executeQuery applies them to a discarded context copy.
    for (size_t i = 0; i + 1 < statements.size(); ++i)
    {
        if (const auto * set_query = statements[i].first->as<ASTSetQuery>())
            global_context->applySettingsChanges(set_query->changes);
        else
            executeSetupStatement(statements[i].second, global_context);
    }

    /// buildQueryTree throws if the last statement is not a SELECT / UNION / INTERSECT / EXCEPT query.
    Stopwatch build_watch;
    auto unresolved_tree = buildQueryTree(statements.back().first, global_context);
    UInt64 build_ns = build_watch.elapsedNanoseconds();

    const bool only_analyze = options.contains("only-analyze");
    std::vector<UInt64> times_ns(iterations);
    QueryTreeNodePtr last_resolved_tree;

    for (size_t i = 0; i < iterations; ++i)
    {
        /// A fresh query context and a fresh clone of the tree per iteration: resolution mutates
        /// the tree, and scalar subquery results are cached in the query context, so reusing
        /// either would make iterations after the first one measure something else.
        auto query_context = Context::createCopy(global_context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId({});

        QueryScope query_scope;
        if (!CurrentThread::getGroup())
            query_scope = QueryScope::create(query_context);

        auto tree = unresolved_tree->clone();
        QueryAnalysisPass pass(only_analyze);

        Stopwatch watch;
        pass.run(tree, query_context);
        times_ns[i] = watch.elapsedNanoseconds();

        last_resolved_tree = std::move(tree);
    }

    printTimings(std::move(times_ns), build_ns);

    if (options.contains("dump-tree"))
        std::cout << "\nResolved query tree:\n" << last_resolved_tree->dumpTree() << '\n';

    if (options.contains("dump-ast"))
        std::cout << "\nResolved query AST:\n" << last_resolved_tree->toAST()->formatWithSecretsMultiLine() << '\n';

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(/* with_stacktrace= */ true) << '\n';
    return 1;
}
