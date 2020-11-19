#include <boost/algorithm/string.hpp>
#include <cstdlib>
#include <iostream>

#include <pcg_random.hpp>
#include <Core/Field.h>
#include <Core/Types.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.cpp>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

using ColumnType = uint32_t;
using TableAndColumn = std::pair<std::string, std::string>;
pcg64 rng;

std::string randomString(size_t length)
{
    auto randchar = []() -> char
    {
        const char charset[] = "0123456789" "ABCDEFGHIJKLMNOPQRSTUVWXYZ" "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rng() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}
std::string randomInteger(unsigned int min = 0, unsigned int max = 4294967295)
{
    int r = rng() % (max - min) + min;
    return std::to_string(r);
}

std::string randomFloat(unsigned int min = 0, unsigned int max = 4294967295)
{
    float r = static_cast<float>(rng() % max) / (static_cast<float>(rng() % 100)) + min;
    return std::to_string(r);
}

std::string randomDate()
{
    int32_t year = rng() % 136 + 1970;
    int32_t month = rng() % 12 + 1;
    int32_t day = rng() % 12 + 1;
    char ans[13];
    sprintf(ans, "'%04u-%02u-%02u'", year, month, day);
    return std::string(ans);
}

std::string randomDatetime()
{
    int32_t year = rng() % 136 + 1970;
    int32_t month = rng() % 12 + 1;
    int32_t day = rng() % 12 + 1;
    int32_t hours = rng() % 24;
    int32_t minutes = rng() % 60;
    int32_t seconds = rng() % 60;
    char ans[22];
    sprintf(
            ans,
            "'%04u-%02u-%02u %02u:%02u:%02u'",
            year,
            month,
            day,
            hours,
            minutes,
            seconds);
    return std::string(ans);
}
TableAndColumn get_table_a_column(const std::string & c)
{
    auto point_place = c.rfind('.');
    std::string db{};
    std::string column{};
    if (point_place != std::string::npos)
    {
        db = c.substr(0, point_place);
        column = c.substr(point_place + 1);
    }
    else
    {
        column = c;
    }
    return { db, column };
}


enum type : ColumnType
{
    i = 1,
    // int
    f = 2,
    // float
    s = 4,
    // string
    d = 8,
    // date
    dt = 16,
    // datetime
    b = 32,
    // bool
    all = 63,
    a = 64,
    // array
    t = 128,
    // tuple
};


std::map<ColumnType, std::string> type_definition = {
        {type::i, "Int64"}, {type::f, "Float64"}, {type::s, "String"}, {type::d, "Date"}, {type::dt, "DateTime"}, {type::b, "UInt8"}
};
ColumnType time_type(std::string value)
{
    if (value.length() == 12)
    {
        for (size_t i : {5, 8})
        {
            if (value[i] != '-')
                return type::s;
        }
        for (size_t i : {1, 2, 3, 4, 6, 7, 9, 10})
        {
            if (!isdigit(value[i]))
                return type::s;
        }
        return type::d;
    }

    if (value.length() == 21)
    {
        for (size_t i : {5, 8})
        {
            if (value[i] != '-')
                return type::s;
        }
        for (size_t i : {14, 17})
        {
            if (value[i] != '-')
                return type::s;
        }
        if (value[11] != '-')
            return type::s;
        return type::dt;
    }
    return type::s;
}
// Casting inner clickhouse parser type to our type
ColumnType type_cast(int t)
{
    switch (t)
    {
        case 1:
        case 2:
        case 4:
        case 5:
        case 19:
        case 20:
        case 21:
            return type::i;

        case 3:
            return type::f;

        case 16:
            return type::s;

        case 17:
            return type::a | type::all;

        case 18:
            return type::t | type::all;
    }
    return type::all;
}


class FuncRet
{
public:
    FuncRet() = default;

    FuncRet(ColumnType t, std::string v)
            : value(v)
            , type(t) {}

    FuncRet(ColumnType t, std::string v, bool is_a)
            : value(v)
            , type(t)
            , is_array(is_a) {}

    std::string value{};
    ColumnType type = type::all;
    bool is_array = false;
};


std::map<std::string, FuncRet> func_to_return_type = {
        {"divide", FuncRet(type::f, "")}, {"e", FuncRet(type::f, "e()")}, {"pi", FuncRet(type::f, "pi()")}, {"exp", FuncRet(type::f, "")},
        {"log", FuncRet(type::f,"")}, {"exp2", FuncRet(type::f, "")}, {"log2", FuncRet(type::f, "")}, {"exp10", FuncRet(type::f, "")},
        {"log10", FuncRet(type::f, "")}, {"sqrt", FuncRet(type::f, "")}, {"cbrt", FuncRet(type::f, "")}, {"erf", FuncRet(type::f, "")},
        {"erfc", FuncRet(type::f, "")}, {"lgamma", FuncRet(type::f, "")}, {"tgamma", FuncRet(type::f, "")}, {"sin", FuncRet(type::f, "")},
        {"cos", FuncRet(type::f, "")}, {"tan", FuncRet(type::f, "")}, {"asin", FuncRet(type::f, "")}, {"acos", FuncRet(type::f, "")},
        {"atan", FuncRet(type::f, "")}, {"pow", FuncRet(type::f, "")}, {"splitbystring", FuncRet(type::s | type::a,"")},
        {"splitbychar", FuncRet(type::s | type::a, "")}, {"alphatokens", FuncRet(type::s | type::a, "")}, {"toyear", FuncRet(type::i, "")},
        {"tomonth", FuncRet(type::i, "")}, {"todayofmonth", FuncRet(type::i, "")}, {"tohour", FuncRet(type::dt, "")}, {"tominute", FuncRet(type::dt, "")},
        {"toseconds", FuncRet(type::dt, "")}, {"tounixtimestamp", FuncRet(type::i, "")}, {"tostartofyear", FuncRet(type::dt | type::d, "")},
        {"tostartofquater",FuncRet(type::dt | type::d, "")}, {"tostartofmonth", FuncRet(type::dt | type::d, "")}, {"tomonday", FuncRet(type::dt | type::d, "")},
        {"tostartoffiveminutes", FuncRet(type::dt, "")}, {"tostartoftenminutes", FuncRet(type::dt, "")}, {"tostartoffifteenminutes", FuncRet(type::dt, "")},
        {"tostartofinterval", FuncRet(type::dt, "")}, {"totime", FuncRet(type::dt, "")}, {"torelativemonthnum", FuncRet(type::i, "")},
        {"torelativeweeknum", FuncRet(type::i, "")}, {"torelativedaynum", FuncRet(type::i, "")}, {"torelativehournum", FuncRet(type::i, "")},
        {"torelativeminutenum", FuncRet(type::i, "")}, {"torelativesecondsnum", FuncRet(type::i, "")}, {"datediff", FuncRet(type::d | type::dt, "")},
        {"formatdatetime", FuncRet(type::s, "")}, {"now", FuncRet(type::dt | type::d, "now()")}, {"today", FuncRet(type::d | type::dt, "today()")},
        {"yesterday", FuncRet(type::d | type::dt, "yesterday()")}
};

std::set<std::string> func_args_same_types = {
        "equals", "notequals", "less", "greater", "lessorequals", "greaterorequals", "multiply"
};

std::map<std::string, ColumnType> func_to_param_type = {
        {"tostartofminute", type::dt}, {"plus", type::i | type::f | type::d | type::dt}, {"multiply", type::i | type::f},
        {"minus", type::i | type::f | type::d | type::dt}, {"negate", type::i | type::f}, {"divide", type::i | type::f},
        {"abs", type::i | type::f}, {"gcd", type::i | type::f}, {"lcm", type::i | type::f}, {"bitnot", type::i}, {"bitshiftleft", type::i},
        {"bitshiftright", type::i}, {"bittest", type::i}, {"exp", type::i | type::f}, {"log", type::i | type::f},
        {"exp2", type::i | type::f}, {"log2", type::i | type::f}, {"exp10", type::i | type::f}, {"log10", type::i | type::f},
        {"sqrt", type::i | type::f}, {"cbrt", type::i | type::f}, {"erf", type::i | type::f}, {"erfc", type::i | type::f},
        {"lgamma", type::i | type::f}, {"tgamma", type::i | type::f}, {"sin", type::i | type::f}, {"cos", type::i | type::f},
        {"tan", type::i | type::f}, {"asin", type::i | type::f}, {"acos", type::i | type::f}, {"atan", type::i | type::f},
        {"pow", type::i | type::f}, {"arrayjoin", type::all | type::a}, {"substring", type::s}, {"splitbystring", type::s}, {"splitbychar", type::s},
        {"alphatokens", type::s}, {"toyear", type::d | type::dt}, {"tomonth", type::d | type::dt}, {"todayofmonth", type::d | type::dt}, {"tohour", type::dt},
        {"tominute", type::dt}, {"tosecond", type::dt}, {"touixtimestamp", type::dt}, {"tostartofyear", type::d | type::dt},
        {"tostartofquarter", type::d | type::dt}, {"tostartofmonth", type::d | type::dt}, {"tomonday", type::d | type::dt},
        {"tostartoffiveminute", type::dt}, {"tostartoftenminutes", type::dt}, {"tostartoffifteenminutes", type::d | type::dt},
        {"tostartofinterval", type::d | type::dt}, {"totime", type::d | type::dt}, {"torelativehonthnum", type::d | type::dt},
        {"torelativeweeknum", type::d | type::dt}, {"torelativedaynum", type::d | type::dt}, {"torelativehournum", type::d | type::dt},
        {"torelativeminutenum", type::d | type::dt}, {"torelativesecondnum", type::d | type::dt}, {"datediff", type::d | type::dt},
        {"formatdatetime", type::dt}
};


class Column
{
public:
    TableAndColumn name;
    std::set<TableAndColumn> equals;
    std::set<std::string> values;
    ColumnType type = type::all;
    bool is_array = false;

    Column() = default;

    explicit Column(const std::string & column_name)
    {
        name = std::make_pair("", column_name);
        type = type::all;
    }

    void merge(Column other)
    {
        if (name.second.empty())
            name = other.name;
        equals.insert(other.equals.begin(), other.equals.end());
        values.insert(other.values.begin(), other.values.end());
        type &= other.type;
        is_array |= other.is_array;
    }

    void printType() const
    {
        if (type & type::i)
            std::cout << "I";
        if (type & type::f)
            std::cout << "F";
        if (type & type::s)
            std::cout << "S";
        if (type & type::d)
            std::cout << "D";
        if (type & type::dt)
            std::cout << "DT";
        if (is_array)
            std::cout << "ARR";
        std::cout << "\n";
    }

    void print()
    {
        std::cout << name.first << "." << name.second << "\n";
        std::cout << "type: ";
        printType();
        std::cout << "values:";
        for (const auto & val : values)
            std::cout << " " << val;
        std::cout << "\n";
        std::cout << "equal:";
        for (const auto & col : equals)
            std::cout << " " << col.first << "." << col.second;
        std::cout << "\n";
    }

    std::string generateOneValue() const
    {
        if (type & type::i)
            return randomInteger();

        if (type & type::f)
            return randomFloat();

        if (type & type::d)
            return randomDate();

        if (type & type::dt)
            return randomDatetime();

        if (type & type::s)
            return "'" + randomString(rng() % 40) + "'";

        if (type & type::b)
            return "0";

        return "";
    }

    bool generateValues(int amount = 0)
    {
        if (values.size() > 2 && amount == 0)
            return false;
        while (values.size() < 1 or amount > 0)
        {
            amount -= 1;
            if (is_array)
            {
                std::string v = "[";
                for (unsigned int i = 0; i < static_cast<unsigned int>(rng()) % 10 + 1; ++i)
                {
                    if (i != 0)
                        v += ", ";
                    v += generateOneValue();
                }
                v += "]";
                values.insert(v);
            }
            else
            {
                values.insert(generateOneValue());
            }
        }
        return true;
    }

    void unifyType()
    {
        if (type & type::i)
            type = type::i;
        else if (type & type::f)
            type = type::f;
        else if (type & type::d)
            type = type::d;
        else if (type & type::dt)
            type = type::dt;
        else if (type & type::s)
            type = type::s;
        else if (type & type::b)
            type = type::b;
        else
            throw std::runtime_error("Error in determination column type " + name.first + '.' + name.second);
    }
};


std::set<std::vector<std::string>>
decartMul(
        std::set<std::vector<std::string>> & prev,
        std::set<std::string> &              mul)
{
    std::set<std::vector<std::string>> result;
    for (auto v : prev)
        for (auto m : mul)
        {
            std::vector<std::string> tmp = v;
            tmp.push_back(m);
            result.insert(tmp);
        }
    return result;
}


class Table
{
public:
    Table() = default;

    explicit Table(std::string table_name)
            : name(table_name) {}

    std::string name;
    std::set<std::string> columns;
    std::map<std::string, Column> column_description;

    bool columnExists(const std::string & column_name) const
    {
        return columns.count(column_name); // || columns_maybe.count(column_name);
    }

    void addColumn(const std::string & column_name)
    {
        columns.insert(column_name);
    }

    void setDescription(Column other)
    {
        column_description[other.name.second].merge(other);
    }

    void print()
    {
        std::cout << "Table\n";
        std::cout << name << "\n";
        std::cout << "Columns:\n\n";
        for (const auto & column : columns)
        {
            std::cout << column << "\n";
            if (column_description.count(column))
                column_description[column].print();
            std::cout << "\n";
        }
        std::cout << "\n";
    }

    void merge(Table other)
    {
        name = other.name;
        columns.insert(other.columns.begin(), other.columns.end());
        for (auto desc : other.column_description)
            column_description[desc.first].merge(desc.second);
    }

    std::string createQuery()
    {
        std::string create;
        std::string db, _;
        std::tie(db, _) = get_table_a_column(name);
        create = "CREATE DATABASE IF NOT EXISTS " + db + ";\n\n";
        create += "CREATE TABLE IF NOT EXISTS " + name + " (\n";
        for (auto column = columns.begin(); column != columns.end(); ++column)
        {
            if (column != columns.begin())
                create += ", \n";
            create += *column + " ";
            create += column_description[*column].is_array ? "Array(" : "";
            create += type_definition[column_description[*column].type];
            create += column_description[*column].is_array ? ")" : "";
        }
        create += "\n) ENGINE = Log;\n\n";
        return create;
    }

    std::string insertQuery()
    {
        std::string insert = "INSERT INTO " + name + "\n";
        insert += "(";
        std::set<std::vector<std::string>> values = {std::vector<std::string>(0)};
        for (auto column = columns.begin(); column != columns.end(); ++column)
        {
            if (column != columns.begin())
                insert += ", ";
            insert += *column;
            values = decartMul(values, column_description[*column].values);
        }
        insert += ") VALUES \n";
        for (auto val_set_iter = values.begin(); val_set_iter != values.end();
             ++val_set_iter)
        {
            if (val_set_iter != values.begin())
                insert += ",\n";
            auto val_set = *val_set_iter;
            insert += "(";
            for (auto val = val_set.begin(); val != val_set.end(); ++val)
            {
                if (val != val_set.begin())
                    insert += ", ";
                insert += *val;
            }
            insert += ")";
        }
        insert += ";\n\n";
        return insert;
    }
};


class TableList
{
public:
    std::string main_table;
    std::map<std::string, std::string> aliases;
    std::unordered_map<std::string, Table> tables;
    std::set<std::string> nested;

    bool tableExists(const std::string & table_name) const
    {
        return tables.count(table_name);
    }

    void addColumn(std::string full_column)
    {
        std::string table, column;
        std::tie(table, column) = get_table_a_column(full_column);
        if (!table.empty())
        {
            if (tables.count(table))
            {
                tables[table].addColumn(column);
                return;
            }
            if (aliases.count(table))
            {
                tables[aliases[table]].addColumn(column);
                return;
            }
            nested.insert(table);
        }
        tables[main_table].addColumn(full_column);
    }

    void addTable(std::string table_name)
    {
        if (tables.count(table_name))
            return;

        tables[table_name] = Table(table_name);
        if (main_table.empty())
            main_table = table_name;
    }

    void addDescription(const Column & description)
    {
        std::string table = description.name.first;
        if (tables.count(table))
            tables[table].setDescription(description);
    }

    TableAndColumn getTable(std::string full_column) const
    {
        std::string table, column;
        std::tie(table, column) = get_table_a_column(full_column);
        if (!table.empty())
        {
            if (tables.count(table))
                return std::make_pair(table, column);

            if (aliases.count(table))
            {
                table = aliases.find(table)->second;
                return std::make_pair(table, column);
            }
        }
        return std::make_pair(main_table, full_column);
    }

    void print()
    {
        for (auto & table : tables)
        {
            table.second.print();
            std::cout << "\n";
        }
    }

    void merge(TableList other)
    {
        for (auto table : other.tables)
            tables[table.first].merge(table.second);
        nested.insert(other.nested.begin(), other.nested.end());
        if (main_table.empty())
            main_table = other.main_table;
    }
};

std::string getAlias(DB::ASTPtr ch)
{
    auto x = std::dynamic_pointer_cast<DB::ASTWithAlias>(ch);
    if (x)
        return x->alias;

    for (const auto & child : (*ch).children)
    {
        auto alias = getAlias(child);
        if (!alias.empty())
            return alias;
    }
    return "";
}

using FuncHandler = std::function<FuncRet(DB::ASTPtr, std::map<std::string, Column> &)>;
std::map<std::string, FuncHandler> handlers = {};

FuncRet arrayJoinFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto x = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (x)
    {
        std::set<std::string> indents = {};
        for (auto & arg : x->arguments->children)
        {
            auto ident = std::dynamic_pointer_cast<DB::ASTIdentifier>(arg);
            if (ident)
                indents.insert(ident->name);
        }
        for (const auto & indent : indents)
        {
            auto c = Column(indent);
            c.type = type::all;
            c.is_array = true;
            if (columns.count(indent))
                columns[indent].merge(c);
            else
                columns[indent] = c;
        }
        FuncRet r(type::all, "");
        return r;
    }
    return FuncRet();
}

FuncRet inFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto x = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (x)
    {
        std::set<std::string> indents{};
        std::set<std::string> values{};
        ColumnType type_value = type::all;

        for (auto & arg : x->arguments->children)
        {
            auto ident = std::dynamic_pointer_cast<DB::ASTIdentifier>(arg);
            if (ident)
            {
                indents.insert(ident->name);
            }
            auto literal = std::dynamic_pointer_cast<DB::ASTLiteral>(arg);
            if (literal)
            {
                ColumnType type = type_cast(literal->value.getType());

                /// C++20
                auto routine = [&] <typename T>(const T & arr_values)
                {
                    for (auto val : arr_values)
                    {
                        type = type_cast(val.getType());
                        if (type == type::s || type == type::d || type == type::dt)
                            type = time_type(applyVisitor(DB::FieldVisitorToString(), val));
                        type_value &= type;
                        values.insert(applyVisitor(DB::FieldVisitorToString(), val));
                    }
                };

                if (type & type::a)
                {
                    auto arr_values = literal->value.get<DB::Array>();
                    routine(arr_values);
                }

                if (type & type::a)
                {
                    auto arr_values = literal->value.get<DB::Tuple>();
                    routine(arr_values);
                }
            }
            auto subfunc = std::dynamic_pointer_cast<DB::ASTFunction>(arg);
            if (subfunc)
            {
                FuncHandler f;
                auto arg_func_name = std::dynamic_pointer_cast<DB::ASTFunction>(arg)->name;
                if (handlers.count(arg_func_name))
                    f = handlers[arg_func_name];
                else
                    f = handlers[""];
                FuncRet ret = f(arg, columns);
                if (ret.value != "")
                {
                    values.insert(ret.value);
                }
                type_value &=  ret.type;
            }
        }
        for (const auto & indent : indents)
        {
            auto c = Column(indent);
            c.type = type_value;
            c.values.insert(values.begin(), values.end());
            c.generateValues(1);
            if (columns.count(indent))
                columns[indent].merge(c);
            else
                columns[indent] = c;
        }
        FuncRet r(type::b | type::i, "");
        return r;
    }
    return FuncRet();
}

FuncRet arrayFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto x = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (x)
    {
        std::set<std::string> indents = {};
        std::string value = "[";
        ColumnType type_value = type::i | type::f | type::d | type::dt | type::s;
        bool no_indent = true;
        for (const auto & arg : x->arguments->children)
        {
            auto ident = std::dynamic_pointer_cast<DB::ASTIdentifier>(arg);
            if (ident)
            {
                no_indent = false;
                indents.insert(ident->name);
            }
            auto literal = std::dynamic_pointer_cast<DB::ASTLiteral>(arg);
            if (literal)
            {
                ColumnType type = type_cast(literal->value.getType());
                if (type == type::s || type == type::d || type == type::dt)
                    type = time_type(value);
                type_value &= type;

                if (value != "[")
                    value += ", ";
                value += applyVisitor(DB::FieldVisitorToString(), literal->value);
            }
        }
        for (const auto & indent : indents)
        {
            auto c = Column(indent);
            c.type = type_value;
            if (columns.count(indent))
                columns[indent].merge(c);
            else
                columns[indent] = c;
        }
        value += ']';
        FuncRet r(type_value, "");
        r.is_array = true;
        if (no_indent)
            r.value = value;
        return r;
    }
    return FuncRet();
}
FuncRet arithmeticFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto x = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (x)
    {
        std::set<std::string> indents = {};
        std::set<std::string> values = {};
        ColumnType type_value = type::i | type::f | type::d | type::dt;
        ColumnType args_types = 0;
        bool no_indent = true;
        for (auto & arg : x->arguments->children)
        {
            ColumnType type = 0;
            auto ident = std::dynamic_pointer_cast<DB::ASTIdentifier>(arg);
            if (ident)
            {
                no_indent = false;
                indents.insert(ident->name);
            }
            auto literal = std::dynamic_pointer_cast<DB::ASTLiteral>(arg);
            if (literal)
                type = type_cast(literal->value.getType());
            auto subfunc = std::dynamic_pointer_cast<DB::ASTFunction>(arg);
            if (subfunc)
            {
                FuncHandler f;
                auto arg_func_name = std::dynamic_pointer_cast<DB::ASTFunction>(arg)->name;
                if (handlers.count(arg_func_name))
                    f = handlers[arg_func_name];
                else
                    f = handlers[""];
                FuncRet ret = f(arg, columns);
                type = ret.type;
            }
            args_types |= type;
        }
        if (args_types & (type::d | type::dt))
            type_value -= type::f;
        if (args_types & type::f)
            type_value -= type::d | type::dt;
        for (auto indent : indents)
        {
            auto c = Column(indent);
            c.type = type_value;
            if (columns.count(indent))
                columns[indent].merge(c);
            else
                columns[indent] = c;
        }
        ColumnType ret_type = 0;
        if (args_types & type::dt)
            ret_type = type::dt;
        else if (args_types & type::d)
            ret_type = type::d | type::dt;
        else if (args_types & type::f)
            ret_type = type::f;
        else
            ret_type = type::d | type::f | type::dt | type::i;
        FuncRet r(ret_type, "");
        if (no_indent)
        {
            std::ostringstream ss;
            formatAST(*ch, ss);
            r.value = ss.str();
        }
        return r;
    }
    return FuncRet();
}
FuncRet likeFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto x = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (x)
    {
        std::set<std::string> indents = {};
        std::set<std::string> values = {};
        ColumnType type_value = type::s;
        for (auto & arg : x->arguments->children)
        {
            auto ident = std::dynamic_pointer_cast<DB::ASTIdentifier>(arg);
            if (ident)
                indents.insert(ident->name);
            auto literal = std::dynamic_pointer_cast<DB::ASTLiteral>(arg);
            if (literal)
            {
                std::string value = applyVisitor(DB::FieldVisitorToString(), literal->value);
                std::string example{};
                for (size_t i = 0; i != value.size(); ++i)
                {
                    if (value[i] == '%')
                        example += randomString(rng() % 10);
                    else if (value[i] == '_')
                        example += randomString(1);
                    else
                        example += value[i];
                }
                values.insert(example);
            }
        }
        for (const auto & indent : indents)
        {
            auto c = Column(indent);
            c.type = type_value;
            c.values.insert(values.begin(), values.end());
            if (columns.count(indent))
                columns[indent].merge(c);
            else
                columns[indent] = c;
        }
        FuncRet r(type::b, "");
        return r;
    }
    return FuncRet();
}

FuncRet simpleFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto X = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (X)
    {
        std::set<std::string> indents = {};
        std::set<std::string> values = {};
        ColumnType type_value = type::all;
        bool is_array = false;
        bool no_indent = true;
        if (func_to_param_type.count(boost::algorithm::to_lower_copy(X->name)))
        {
            type_value &= func_to_param_type[boost::algorithm::to_lower_copy(X->name)];
            is_array = func_to_param_type[boost::algorithm::to_lower_copy(X->name)] & type::a;
        }
        for (auto arg : X->arguments->children)
        {
            ColumnType type = type::all;
            std::string value;
            auto ident = std::dynamic_pointer_cast<DB::ASTIdentifier>(arg);
            if (ident)
            {
                no_indent = false;
                indents.insert(ident->name);
            }
            auto literal = std::dynamic_pointer_cast<DB::ASTLiteral>(arg);
            if (literal)
            {
                value = applyVisitor(DB::FieldVisitorToString(), literal->value);
                type = type_cast(literal->value.getType());
                is_array |= type & type::a;
            }
            auto subfunc = std::dynamic_pointer_cast<DB::ASTFunction>(arg);
            if (subfunc)
            {
                FuncHandler f;
                auto arg_func_name = std::dynamic_pointer_cast<DB::ASTFunction>(arg)->name;
                if (handlers.count(arg_func_name))
                    f = handlers[arg_func_name];
                else
                    f = handlers[""];
                FuncRet ret = f(arg, columns);
                is_array |= ret.is_array;
                type = ret.type;
                value = ret.value;
                if (value.empty())
                    no_indent = false;
            }
            if (!value.empty())
            {
                if (type == type::i)
                {
                    values.insert(value);
                    values.insert(value + " + " + randomInteger(1, 10));
                    values.insert(value + " - " + randomInteger(1, 10));
                }
                if (type == type::f)
                {
                    values.insert(value);
                    values.insert(value + " + " + randomFloat(1, 10));
                    values.insert(value + " - " + randomFloat(1, 10));
                }
                if (type & type::s || type & type::d || type & type::dt)
                {
                    if (type == type::s)
                        type = time_type(value);
                    if (type == type::s)
                        values.insert(value);
                    if (type & type::d)
                    {
                        values.insert(value);
                        values.insert("toDate(" + value + ") + " + randomInteger(1, 10));
                        values.insert("toDate(" + value + ") - " + randomInteger(1, 10));
                    }
                    else if (type & type::dt)
                    {
                        values.insert(value);
                        values.insert(
                                "toDateTime(" + value + ") + " + randomInteger(1, 10000));
                        values.insert(
                                "toDateTime(" + value + ") - " + randomInteger(1, 10000));
                    }
                }
            }
            if (func_args_same_types.count(boost::algorithm::to_lower_copy(X->name)))
                type_value &= type;
        }
        for (const auto & indent : indents)
        {
            auto c = Column(indent);
            c.type = type_value;
            c.is_array = is_array;
            if (func_args_same_types.count(
                    boost::algorithm::to_lower_copy(X->name)))
                c.values = values;
            for (const auto & ind : indents)
                if (ind != indent)
                    c.equals.insert(std::make_pair("", ind));

            if (columns.count(indent))
                columns[indent].merge(c);
            else
                columns[indent] = c;
        }
        if (func_to_return_type.count(boost::algorithm::to_lower_copy(X->name)))
        {
            if (no_indent)
            {
                std::ostringstream ss;
                formatAST(*ch, ss);
                auto r = func_to_return_type[boost::algorithm::to_lower_copy(X->name)];
                r.value = ss.str();
                return r;
            }
            return func_to_return_type[boost::algorithm::to_lower_copy(X->name)];
        }
        else if (func_to_param_type.count(
                boost::algorithm::to_lower_copy(X->name)))
        {
            if (no_indent)
            {
                std::ostringstream ss;
                formatAST(*ch, ss);
                return FuncRet(
                        func_to_param_type[boost::algorithm::to_lower_copy(X->name)],
                        ss.str());
            }
            return FuncRet(
                    func_to_param_type[boost::algorithm::to_lower_copy(X->name)],
                    "");
        }
    }
    return FuncRet();
}

void processFunc(DB::ASTPtr ch, std::map<std::string, Column> & columns)
{
    auto x = std::dynamic_pointer_cast<DB::ASTFunction>(ch);
    if (x)
    {
        FuncHandler f;
        auto arg_func_name = x->name;
        if (handlers.count(arg_func_name))
            f = handlers[arg_func_name];
        else
            f = handlers[""];
        f(ch, columns);
    }
    else
    {
        for (const auto & child : (*ch).children)
            processFunc(child, columns);
    }
}


std::set<std::string> getIndent(DB::ASTPtr ch)
{
    if (!ch)
        return {};

    std::set<std::string> ret = {};
    auto x = std::dynamic_pointer_cast<DB::ASTIdentifier>(ch);
    if (x)
        ret.insert(x->name);
    for (const auto & child : (*ch).children)
    {
        auto child_ind = getIndent(child);
        ret.insert(child_ind.begin(), child_ind.end());
    }
    return ret;
}


std::set<std::string> getSelectIndent(
        DB::ASTPtr              asp,
        std::set<std::string> & column_alias)
{
    std::set<std::string> ret = {};
    for (auto & ch : asp->children)
    {
        auto alias = getAlias(ch);
        auto columns = getIndent(ch);
        if (alias.empty())
            column_alias.insert(alias);
        ret.insert(columns.begin(), columns.end());
    }
    return ret;
}


std::set<TableAndColumn>
connectedEqualityFind(
        const Column & now,
        std::map<std::string, Column> & columns_descriptions,
        std::set<TableAndColumn> & visited)
{
    std::set<TableAndColumn> result;
    for (auto & column : now.equals)
        if (!visited.count(column))
        {
            visited.insert(column);
            auto sub_r = connectedEqualityFind(
                columns_descriptions[column.first + "." + column.second],
                columns_descriptions,
                visited);
            result.insert(sub_r.begin(), sub_r.end());
        }
    result.insert(now.name);
    return result;
}


std::map<std::string, Column>
unificateColumns(
        std::map<std::string, Column> columns_descriptions,
        const TableList & all_tables)
{
    for (auto & column : columns_descriptions)
    {
        std::set<TableAndColumn> changed_equals;
        for (const auto & eq : column.second.equals)
        {
            std::string t, c;
            std::tie(t, c) = all_tables.getTable(eq.second);
            changed_equals.insert(std::make_pair(t, c));
        }
        column.second.equals = changed_equals;
    }
    std::map<std::string, Column> result;
    for (auto & column : columns_descriptions)
    {
        std::string t, c;
        std::tie(t, c) = all_tables.getTable(column.first);
        column.second.name = std::make_pair(t, c);
        result[t + "." + c].merge(column.second);
    }
    std::set<TableAndColumn> visited;
    for (auto & column : result)
        if (!visited.count(column.second.name))
        {
            auto equal = connectedEqualityFind(
                result[column.second.name.first + "." + column.second.name.second],
                result,
                visited);
            for (auto c : equal)
                result[c.first + "." + c.second].equals = equal;
        }
    for (auto & column : result)
        for (auto e : column.second.equals)
            column.second.merge(result[e.first + "." + e.second]);

    for (auto & column : result)
    {
        column.second.unifyType();
        if (column.second.generateValues())
            for (auto e : column.second.equals)
                result[e.first + "." + e.second].merge(column.second);

    }
    return result;
}

std::vector<DB::ASTPtr> getSelect(DB::ASTPtr vertex)
{
    auto z = std::dynamic_pointer_cast<DB::ASTSelectQuery>(vertex);
    std::vector<DB::ASTPtr> result;
    if (z)
    {
        result.push_back(vertex);
        return result;
    }

    for (const auto & child : (*vertex).children)
    {
        auto v = getSelect(child);
        result.insert(result.end(), v.begin(), v.end());
    }
    return result;
}


void parseSelectQuery(DB::ASTPtr ast, TableList & all_tables)
{
    if (!ast)
        throw std::runtime_error("Bad ASTPtr in parseSelectQuery" + StackTrace().toString());

    auto select_ast = std::dynamic_pointer_cast<DB::ASTSelectQuery>(ast);
    if (!select_ast)
    {
        std::cerr << "not select query";
        return;
    }
    std::set<std::string> columns = {};

    auto x = select_ast->tables();
    if (!x)
        throw std::runtime_error("There is no tables in query. Nothing to generate.");

    for (auto & child : x->children)
    {
        auto ch = std::dynamic_pointer_cast<DB::ASTTablesInSelectQueryElement>(child);
        auto TEast = std::dynamic_pointer_cast<DB::ASTTableExpression>(ch->table_expression);
        if (TEast && TEast->database_and_table_name)
        {
            auto table_name = *(getIndent(TEast->database_and_table_name).begin());
            all_tables.addTable(table_name);
            auto alias = getAlias(ch);
            if (!alias.empty())
                all_tables.aliases[alias] = table_name;
        }
        if (TEast && TEast->subquery)
        {
            for (auto select : getSelect(TEast->subquery))
            {
                TableList local;
                parseSelectQuery(select, local);
                all_tables.merge(local);
            }
        }

        if (ch->table_join)
        {
            auto jch = std::dynamic_pointer_cast<DB::ASTTableJoin>(ch->table_join);
            if (jch->using_expression_list)
            {
                auto join_columns = getIndent(jch->using_expression_list);
                columns.insert(join_columns.begin(), join_columns.end());
            }
            else if (jch->on_expression)
            {
                auto join_columns = getIndent(jch->on_expression);
                columns.insert(join_columns.begin(), join_columns.end());
            }
        }
    }

    std::set<std::string> column_aliases;
    auto select_columns = getSelectIndent(select_ast->select(), column_aliases);
    columns.insert(select_columns.begin(), select_columns.end());

    auto where_columns = getIndent(select_ast->where());
    columns.insert(where_columns.begin(), where_columns.end());

    auto groupby_columns = getIndent(select_ast->groupBy());
    columns.insert(groupby_columns.begin(), groupby_columns.end());

    auto orderby_columns = getIndent(select_ast->orderBy());
    columns.insert(orderby_columns.begin(), orderby_columns.end());

    auto having_columns = getIndent(select_ast->having());
    columns.insert(having_columns.begin(), having_columns.end());

    std::map<std::string, Column> columns_descriptions;
    processFunc(ast, columns_descriptions);

    for (const auto & column : columns)
        if (!column_aliases.count(column))
        {
            if (!columns_descriptions.count(column))
                columns_descriptions[column] = Column(column);
            all_tables.addColumn(column);
        }

    columns_descriptions = unificateColumns(columns_descriptions, all_tables);
    for (auto & column : columns_descriptions)
        all_tables.addDescription(column.second);
}


TableList getTablesFromSelect(std::vector<std::string> queries)
{
    DB::ParserQueryWithOutput parser;
    TableList result;
    for (std::string & query : queries)
    {
        DB::ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, 0);
        for (auto & select : getSelect(ast))
        {
            TableList local;
            parseSelectQuery(select, local);
            result.merge(local);
        }
    }
    return result;
}

int main(int, char **)
{
    handlers["plus"] = arithmeticFunc;
    handlers["minus"] = arithmeticFunc;
    handlers["like"] = likeFunc;
    handlers["array"] = arrayFunc;
    handlers["in"] = inFunc;
    handlers[""] = simpleFunc;

    std::vector<std::string> queries;
    std::string in;
    std::string query{};
    while (getline(std::cin, in))
    {
        /// Skip comments
        if (in.find("--") != std::string::npos)
            continue;

        query += in + " ";

        if (in.find(';') != std::string::npos)
        {
            queries.push_back(query);
            query = "";
        }
    }

    try
    {
        auto result = getTablesFromSelect(queries);

        for (auto & table : result.tables)
        {
            std::cout << table.second.createQuery();
            std::cout << table.second.insertQuery();
        }

        for (auto & q: queries)
            std::cout << q << std::endl;
    }
    catch (std::string & e)
    {
        std::cerr << "Exception: " << e << std::endl;
    }
}
