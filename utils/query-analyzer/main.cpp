#include <iostream>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

namespace po = boost::program_options;

int main(int argc, char ** argv)
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

    if (options.count("help"))
    {
        std::cout << "Runs QueryAnalysisPass on a SELECT query in a loop and reports timings.\n"
                  << "Usage: query-analyzer [options] [query]\n" << description << '\n';
        return 0;
    }

    std::cerr << "Not implemented yet\n";
    return 1;
}
