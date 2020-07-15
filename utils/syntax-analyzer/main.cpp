#include <Parsers/IAST.h>
#include <Parsers/New/parseQuery.h>

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

    ASTPtr old_ast = parseQuery(query);
    if (old_ast) old_ast->dumpTree(std::cout);
}
