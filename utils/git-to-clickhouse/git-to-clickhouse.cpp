#include <cstdint>
#include <string>
#include <vector>

#include <boost/program_options.hpp>

#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ShellCommand.h>
#include <common/find_symbols.h>

#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDescriptor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

enum class LineType
{
    Empty,
    Comment,
    Punct,
    Code,
};

void writeText(LineType type, WriteBuffer & out)
{
    switch (type)
    {
        case LineType::Empty: writeString("Empty", out); break;
        case LineType::Comment: writeString("Comment", out); break;
        case LineType::Punct: writeString("Punct", out); break;
        case LineType::Code: writeString("Code", out); break;
    }
}

struct LineChange
{
    int8_t sign{}; /// 1 if added, -1 if deleted
    uint16_t line_number_old{};
    uint16_t line_number_new{};
    uint16_t hunk_num{}; /// ordinal number of hunk in diff, starting with 0
    uint16_t hunk_start_line_number_old{};
    uint16_t hunk_start_line_number_new{};
    std::string hunk_context; /// The context (like a line with function name) as it is calculated by git
    std::string line; /// Line content without leading whitespaces
    uint8_t indent{}; /// The number of leading whitespaces or tabs * 4
    LineType line_type{};

    void setLineInfo(std::string full_line)
    {
        indent = 0;

        const char * pos = full_line.data();
        const char * end = pos + full_line.size();

        while (pos < end)
        {
            if (*pos == ' ')
                ++indent;
            else if (*pos == '\t')
                indent += 4;
            else
                break;
            ++pos;
        }

        line.assign(pos, end);

        if (pos == end)
        {
            line_type = LineType::Empty;
        }
        else if (pos + 1 < end
            && ((pos[0] == '/' && pos[1] == '/')
                || (pos[0] == '*' && pos[1] == ' '))) /// This is not precise.
        {
            line_type = LineType::Comment;
        }
        else
        {
            while (pos < end)
            {
                if (isAlphaNumericASCII(*pos))
                {
                    line_type = LineType::Code;
                    break;
                }
                ++pos;
            }
            if (pos == end)
                line_type = LineType::Punct;
        }
    }

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeText(sign, out);
        writeChar('\t', out);
        writeText(line_number_old, out);
        writeChar('\t', out);
        writeText(line_number_new, out);
        writeChar('\t', out);
        writeText(hunk_num, out);
        writeChar('\t', out);
        writeText(hunk_start_line_number_old, out);
        writeChar('\t', out);
        writeText(hunk_start_line_number_new, out);
        writeChar('\t', out);
        writeText(hunk_context, out);
        writeChar('\t', out);
        writeText(line, out);
        writeChar('\t', out);
        writeText(indent, out);
        writeChar('\t', out);
        writeText(line_type, out);
    }
};

using LineChanges = std::vector<LineChange>;

enum class FileChangeType
{
    Add,
    Delete,
    Modify,
    Rename,
    Copy,
    Type,
};

void writeText(FileChangeType type, WriteBuffer & out)
{
    switch (type)
    {
        case FileChangeType::Add: writeString("Add", out); break;
        case FileChangeType::Delete: writeString("Delete", out); break;
        case FileChangeType::Modify: writeString("Modify", out); break;
        case FileChangeType::Rename: writeString("Rename", out); break;
        case FileChangeType::Copy: writeString("Copy", out); break;
        case FileChangeType::Type: writeString("Type", out); break;
    }
}

struct FileChange
{
    FileChangeType change_type{};
    std::string new_file_path;
    std::string old_file_path;
    uint16_t lines_added{};
    uint16_t lines_deleted{};
    uint16_t hunks_added{};
    uint16_t hunks_removed{};
    uint16_t hunks_changed{};

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeText(change_type, out);
        writeChar('\t', out);
        writeText(new_file_path, out);
        writeChar('\t', out);
        writeText(old_file_path, out);
        writeChar('\t', out);
        writeText(lines_added, out);
        writeChar('\t', out);
        writeText(lines_deleted, out);
        writeChar('\t', out);
        writeText(hunks_added, out);
        writeChar('\t', out);
        writeText(hunks_removed, out);
        writeChar('\t', out);
        writeText(hunks_changed, out);
    }
};

struct FileChangeAndLineChanges
{
    FileChange file_change;
    LineChanges line_changes;
};

struct Commit
{
    std::string hash;
    std::string author_name;
    std::string author_email;
    time_t time{};
    std::string message;
    uint32_t files_added{};
    uint32_t files_deleted{};
    uint32_t files_renamed{};
    uint32_t files_modified{};
    uint32_t lines_added{};
    uint32_t lines_deleted{};
    uint32_t hunks_added{};
    uint32_t hunks_removed{};
    uint32_t hunks_changed{};

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeText(hash, out);
        writeChar('\t', out);
        writeText(author_name, out);
        writeChar('\t', out);
        writeText(author_email, out);
        writeChar('\t', out);
        writeText(time, out);
        writeChar('\t', out);
        writeText(message, out);
        writeChar('\t', out);
        writeText(files_added, out);
        writeChar('\t', out);
        writeText(files_deleted, out);
        writeChar('\t', out);
        writeText(files_renamed, out);
        writeChar('\t', out);
        writeText(files_modified, out);
        writeChar('\t', out);
        writeText(lines_added, out);
        writeChar('\t', out);
        writeText(lines_deleted, out);
        writeChar('\t', out);
        writeText(hunks_added, out);
        writeChar('\t', out);
        writeText(hunks_removed, out);
        writeChar('\t', out);
        writeText(hunks_changed, out);
    }
};


void skipUntilWhitespace(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\t', '\n', ' '>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\t' || *buf.position() == '\n' || *buf.position() == ' ')
            return;
    }
}

void skipUntilNextLine(ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}

void readStringUntilNextLine(std::string & s, ReadBuffer & buf)
{
    s.clear();
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\n'>(buf.position(), buf.buffer().end());
        s.append(buf.position(), next_pos - buf.position());
        buf.position() = next_pos;

        if (!buf.hasPendingData())
            continue;

        if (*buf.position() == '\n')
        {
            ++buf.position();
            return;
        }
    }
}


struct Result
{
    WriteBufferFromFile commits{"commits.tsv"};
    WriteBufferFromFile file_changes{"file_changes.tsv"};
    WriteBufferFromFile line_changes{"line_changes.tsv"};
};


void processCommit(std::string hash, Result & result)
{
    std::string command = fmt::format(
        "git show --raw --pretty='format:%at%x09%aN%x09%aE%x0A%s%x00' --patch --unified=0 {}",
        hash);

    std::cerr << command << "\n";

    auto commit_info = ShellCommand::execute(command);
    auto & in = commit_info->out;

    Commit commit;
    commit.hash = hash;

    readText(commit.time, in);
    assertChar('\t', in);
    readText(commit.author_name, in);
    assertChar('\t', in);
    readText(commit.author_email, in);
    assertChar('\n', in);
    readNullTerminated(commit.message, in);

    std::cerr << fmt::format("{}\t{}\n", toString(LocalDateTime(commit.time)), commit.message);

    if (!in.eof())
        assertChar('\n', in);

    /// File changes in form
    /// :100644 100644 b90fe6bb94 3ffe4c380f M  src/Storages/MergeTree/MergeTreeDataMergerMutator.cpp
    /// :100644 100644 828dedf6b5 828dedf6b5 R100       dbms/src/Functions/GeoUtils.h   dbms/src/Functions/PolygonUtils.h

    std::map<std::string, FileChangeAndLineChanges> file_changes;

    while (checkChar(':', in))
    {
        FileChange file_change;

        for (size_t i = 0; i < 4; ++i)
        {
            skipUntilWhitespace(in);
            skipWhitespaceIfAny(in);
        }

        char change_type;
        readChar(change_type, in);

        int confidence;
        switch (change_type)
        {
            case 'A':
                file_change.change_type = FileChangeType::Add;
                ++commit.files_added;
                break;
            case 'D':
                file_change.change_type = FileChangeType::Delete;
                ++commit.files_deleted;
                break;
            case 'M':
                file_change.change_type = FileChangeType::Modify;
                ++commit.files_modified;
                break;
            case 'R':
                file_change.change_type = FileChangeType::Rename;
                ++commit.files_renamed;
                readText(confidence, in);
                break;
            case 'C':
                file_change.change_type = FileChangeType::Copy;
                readText(confidence, in);
                break;
            case 'T':
                file_change.change_type = FileChangeType::Type;
                break;
            default:
                throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected file change type: {}", change_type);
        }

        skipWhitespaceIfAny(in);

        if (change_type == 'R' || change_type == 'C')
        {
            readText(file_change.old_file_path, in);
            skipWhitespaceIfAny(in);
            readText(file_change.new_file_path, in);
        }
        else
        {
            readText(file_change.new_file_path, in);
        }

        assertChar('\n', in);

        file_changes.emplace(
            file_change.new_file_path,
            FileChangeAndLineChanges{ file_change, {} });
    }

    if (!in.eof())
    {
        assertChar('\n', in);

        /// Diffs for every file in form of
        /// --- a/src/Storages/StorageReplicatedMergeTree.cpp
        /// +++ b/src/Storages/StorageReplicatedMergeTree.cpp
        /// @@ -1387,2 +1387 @@ bool StorageReplicatedMergeTree::tryExecuteMerge(const LogEntry & entry)
        /// -            table_lock, entry.create_time, reserved_space, entry.deduplicate,
        /// -            entry.force_ttl);
        /// +            table_lock, entry.create_time, reserved_space, entry.deduplicate);

        std::string old_file_path;
        std::string new_file_path;
        FileChangeAndLineChanges * file_change_and_line_changes = nullptr;
        LineChange line_change;

        while (!in.eof())
        {
            if (checkString("@@ ", in))
            {
                if (!file_change_and_line_changes)
                {
                    auto file_name = new_file_path.empty() ? old_file_path : new_file_path;
                    auto it = file_changes.find(file_name);
                    if (file_changes.end() == it)
                        std::cerr << fmt::format("Warning: skipping bad file name {}\n", file_name);
                    else
                        file_change_and_line_changes = &it->second;
                }

                if (file_change_and_line_changes)
                {
                    uint16_t old_lines = 1;
                    uint16_t new_lines = 1;

                    assertChar('-', in);
                    readText(line_change.hunk_start_line_number_old, in);
                    if (checkChar(',', in))
                        readText(old_lines, in);

                    assertString(" +", in);
                    readText(line_change.hunk_start_line_number_new, in);
                    if (checkChar(',', in))
                        readText(new_lines, in);

                    assertString(" @@", in);
                    if (checkChar(' ', in))
                        readStringUntilNextLine(line_change.hunk_context, in);
                    else
                        assertChar('\n', in);

                    ++line_change.hunk_num;
                    line_change.line_number_old = line_change.hunk_start_line_number_old;
                    line_change.line_number_new = line_change.hunk_start_line_number_new;

                    if (old_lines && new_lines)
                    {
                        ++commit.hunks_changed;
                        ++file_change_and_line_changes->file_change.hunks_changed;
                    }
                    else if (old_lines)
                    {
                        ++commit.hunks_removed;
                        ++file_change_and_line_changes->file_change.hunks_removed;
                    }
                    else if (new_lines)
                    {
                        ++commit.hunks_added;
                        ++file_change_and_line_changes->file_change.hunks_added;
                    }
                }
            }
            else if (checkChar('-', in))
            {
                if (checkString("-- ", in))
                {
                    if (checkString("a/", in))
                    {
                        readStringUntilNextLine(old_file_path, in);
                        line_change = LineChange{};
                        file_change_and_line_changes = nullptr;
                    }
                    else if (checkString("/dev/null", in))
                    {
                        old_file_path.clear();
                        assertChar('\n', in);
                        line_change = LineChange{};
                        file_change_and_line_changes = nullptr;
                    }
                    else
                        skipUntilNextLine(in); /// Actually it can be the line in diff. Skip it for simplicity.
                }
                else
                {
                    if (file_change_and_line_changes)
                    {
                        ++commit.lines_deleted;

                        line_change.sign = -1;
                        readStringUntilNextLine(line_change.line, in);
                        line_change.setLineInfo(line_change.line);

                        file_change_and_line_changes->line_changes.push_back(line_change);
                        ++line_change.line_number_old;
                    }
                }
            }
            else if (checkChar('+', in))
            {
                if (checkString("++ ", in))
                {
                    if (checkString("b/", in))
                    {
                        readStringUntilNextLine(new_file_path, in);
                        line_change = LineChange{};
                        file_change_and_line_changes = nullptr;
                    }
                    else if (checkString("/dev/null", in))
                    {
                        new_file_path.clear();
                        assertChar('\n', in);
                        line_change = LineChange{};
                        file_change_and_line_changes = nullptr;
                    }
                    else
                        skipUntilNextLine(in); /// Actually it can be the line in diff. Skip it for simplicity.
                }
                else
                {
                    if (file_change_and_line_changes)
                    {
                        ++commit.lines_added;

                        line_change.sign = 1;
                        readStringUntilNextLine(line_change.line, in);
                        line_change.setLineInfo(line_change.line);

                        file_change_and_line_changes->line_changes.push_back(line_change);
                        ++line_change.line_number_new;
                    }
                }
            }
            else
            {
                skipUntilNextLine(in);
            }
        }
    }

    /// Write the result

    /// commits table
    {
        auto & out = result.commits;

        commit.writeTextWithoutNewline(out);
        writeChar('\n', out);
    }

    for (const auto & elem : file_changes)
    {
        const FileChange & file_change = elem.second.file_change;

        /// file_changes table
        {
            auto & out = result.file_changes;

            file_change.writeTextWithoutNewline(out);
            writeChar('\t', out);
            commit.writeTextWithoutNewline(out);
            writeChar('\n', out);
        }

        /// line_changes table
        for (const auto & line_change : elem.second.line_changes)
        {
            auto & out = result.line_changes;

            line_change.writeTextWithoutNewline(out);
            writeChar('\t', out);
            file_change.writeTextWithoutNewline(out);
            writeChar('\t', out);
            commit.writeTextWithoutNewline(out);
            writeChar('\n', out);
        }
    }
}


void processLog()
{
    Result result;

    std::string command = "git log --no-merges --pretty=%H";
    std::cerr << command << "\n";
    auto git_log = ShellCommand::execute(command);

    auto & in = git_log->out;
    while (!in.eof())
    {
        std::string hash;
        readString(hash, in);
        assertChar('\n', in);

        std::cerr << fmt::format("Processing commit {}\n", hash);
        processCommit(std::move(hash), result);
    }
}


}

int main(int /*argc*/, char ** /*argv*/)
try
{
    using namespace DB;

/*    boost::program_options::options_description desc("Allowed options");
    desc.add_options()("help,h", "produce help message");

    boost::program_options::variables_map options;
    boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help") || argc != 2)
    {
        std::cout << "Usage: " << argv[0] << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }*/

    processLog();
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
