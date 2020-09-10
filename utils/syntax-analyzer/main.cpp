#include <Parsers/IAST.h>
#include <Parsers/New/parseQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>

#include <support/Any.h>

#include <iostream>
#include <iterator>
#include <string>

using namespace DB;

int main(int argc, const char **)
{
    if (argc > 1)
    {
        std::cerr << "No arguments needed. Reads query from input until EOF" << std::endl;
        return 1;
    }

    std::istreambuf_iterator<char> begin(std::cin), end;
    std::string query(begin, end);

    std::cout << "New AST:" << std::endl;
    ASTPtr old_ast = parseQuery(query);

    std::cout << std::endl << "Original AST:" << std::endl;
    {
        std::vector<String> queries;
        splitMultipartQuery(query, queries, 100000, 10000);
        for (const auto & q : queries)
        {
            ParserQuery parser(q.data() + q.size(), true);
            ASTPtr orig_ast = parseQuery(parser, q, 100000, 10000);
            if (orig_ast) orig_ast->dumpTree(std::cout);
        }
    }

    std::cout << std::endl << "Converted AST:" << std::endl;
    if (old_ast) old_ast->dumpTree(std::cout);
}
