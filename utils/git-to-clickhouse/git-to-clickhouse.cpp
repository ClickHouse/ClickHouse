#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <list>
#include <thread>
#include <filesystem>

#include <re2_st/re2.h>

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


static constexpr auto documentation = R"(
Prepare the database by executing the following queries:

DROP DATABASE IF EXISTS git;
CREATE DATABASE git;

CREATE TABLE git.commits
(
    hash String,
    author LowCardinality(String),
    time DateTime,
    message String,
    files_added UInt32,
    files_deleted UInt32,
    files_renamed UInt32,
    files_modified UInt32,
    lines_added UInt32,
    lines_deleted UInt32,
    hunks_added UInt32,
    hunks_removed UInt32,
    hunks_changed UInt32
) ENGINE = MergeTree ORDER BY time;

CREATE TABLE git.file_changes
(
    change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    path LowCardinality(String),
    old_path LowCardinality(String),
    file_extension LowCardinality(String),
    lines_added UInt32,
    lines_deleted UInt32,
    hunks_added UInt32,
    hunks_removed UInt32,
    hunks_changed UInt32,

    commit_hash String,
    author LowCardinality(String),
    time DateTime,
    commit_message String,
    commit_files_added UInt32,
    commit_files_deleted UInt32,
    commit_files_renamed UInt32,
    commit_files_modified UInt32,
    commit_lines_added UInt32,
    commit_lines_deleted UInt32,
    commit_hunks_added UInt32,
    commit_hunks_removed UInt32,
    commit_hunks_changed UInt32
) ENGINE = MergeTree ORDER BY time;

CREATE TABLE git.line_changes
(
    sign Int8,
    line_number_old UInt32,
    line_number_new UInt32,
    hunk_num UInt32,
    hunk_start_line_number_old UInt32,
    hunk_start_line_number_new UInt32,
    hunk_lines_added UInt32,
    hunk_lines_deleted UInt32,
    hunk_context LowCardinality(String),
    line LowCardinality(String),
    indent UInt8,
    line_type Enum('Empty' = 0, 'Comment' = 1, 'Punct' = 2, 'Code' = 3),

    prev_commit_hash String,
    prev_author LowCardinality(String),
    prev_time DateTime,

    file_change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    path LowCardinality(String),
    old_path LowCardinality(String),
    file_extension LowCardinality(String),
    file_lines_added UInt32,
    file_lines_deleted UInt32,
    file_hunks_added UInt32,
    file_hunks_removed UInt32,
    file_hunks_changed UInt32,

    commit_hash String,
    author LowCardinality(String),
    time DateTime,
    commit_message String,
    commit_files_added UInt32,
    commit_files_deleted UInt32,
    commit_files_renamed UInt32,
    commit_files_modified UInt32,
    commit_lines_added UInt32,
    commit_lines_deleted UInt32,
    commit_hunks_added UInt32,
    commit_hunks_removed UInt32,
    commit_hunks_changed UInt32
) ENGINE = MergeTree ORDER BY time;

Insert the data with the following commands:

clickhouse-client --query "INSERT INTO git.commits FORMAT TSV" < commits.tsv
clickhouse-client --query "INSERT INTO git.file_changes FORMAT TSV" < file_changes.tsv
clickhouse-client --query "INSERT INTO git.line_changes FORMAT TSV" < line_changes.tsv

)";

namespace po = boost::program_options;

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}


struct Commit
{
    std::string hash;
    std::string author;
    LocalDateTime time{};
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
        writeText(author, out);
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
    std::string path;
    std::string old_path;
    std::string file_extension;
    uint32_t lines_added{};
    uint32_t lines_deleted{};
    uint32_t hunks_added{};
    uint32_t hunks_removed{};
    uint32_t hunks_changed{};

    void writeTextWithoutNewline(WriteBuffer & out) const
    {
        writeText(change_type, out);
        writeChar('\t', out);
        writeText(path, out);
        writeChar('\t', out);
        writeText(old_path, out);
        writeChar('\t', out);
        writeText(file_extension, out);
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
    uint32_t line_number_old{};
    uint32_t line_number_new{};
    uint32_t hunk_num{}; /// ordinal number of hunk in diff, starting with 0
    uint32_t hunk_start_line_number_old{};
    uint32_t hunk_start_line_number_new{};
    uint32_t hunk_lines_added{};
    uint32_t hunk_lines_deleted{};
    std::string hunk_context; /// The context (like a line with function name) as it is calculated by git
    std::string line; /// Line content without leading whitespaces
    uint8_t indent{}; /// The number of leading whitespaces or tabs * 4
    LineType line_type{};
    std::string prev_commit_hash;
    std::string prev_author;
    LocalDateTime prev_time{};

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
        writeText(hunk_lines_added, out);
        writeChar('\t', out);
        writeText(hunk_lines_deleted, out);
        writeChar('\t', out);
        writeText(hunk_context, out);
        writeChar('\t', out);
        writeText(line, out);
        writeChar('\t', out);
        writeText(indent, out);
        writeChar('\t', out);
        writeText(line_type, out);
        writeChar('\t', out);
        writeText(prev_commit_hash, out);
        writeChar('\t', out);
        writeText(prev_author, out);
        writeChar('\t', out);
        writeText(prev_time, out);
    }
};

using LineChanges = std::vector<LineChange>;


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


struct Options
{
    bool skip_commits_without_parents = true;
    size_t threads = 1;
    std::optional<re2_st::RE2> skip_paths;
    std::unordered_set<std::string> skip_commits;
    std::optional<size_t> diff_size_limit;

    Options(const po::variables_map & options)
    {
        skip_commits_without_parents = options["skip-commits-without-parents"].as<bool>();
        threads = options["threads"].as<size_t>();
        if (options.count("skip-paths"))
        {
            skip_paths.emplace(options["skip-paths"].as<std::string>());
        }
        if (options.count("skip-commit"))
        {
            auto vec = options["skip-commit"].as<std::vector<std::string>>();
            skip_commits.insert(vec.begin(), vec.end());
        }
        if (options.count("diff-size-limit"))
        {
            diff_size_limit = options["diff-size-limit"].as<size_t>();
        }
    }
};


/// Rough snapshot of repository calculated by application of diffs. It's used to calculate blame info.
struct FileBlame
{
    using Lines = std::list<Commit>;
    Lines lines;
    Lines::iterator it;
    size_t current_idx = 1;

    FileBlame()
    {
        it = lines.begin();
    }

    FileBlame & operator=(const FileBlame & rhs)
    {
        lines = rhs.lines;
        it = lines.begin();
        current_idx = 1;
        return *this;
    }

    FileBlame(const FileBlame & rhs)
    {
        *this = rhs;
    }

    void walk(uint32_t num)
    {
        if (current_idx < num)
        {
            while (current_idx < num && it != lines.end())
            {
                ++current_idx;
                ++it;
            }
        }
        else if (current_idx > num)
        {
            --current_idx;
            --it;
        }
    }

    const Commit * find(uint32_t num)
    {
        walk(num);

        if (current_idx == num && it != lines.end())
            return &*it;
        return {};
    }

    void addLine(uint32_t num, Commit commit)
    {
        walk(num);

        while (it == lines.end() && current_idx < num)
        {
            lines.emplace_back();
            ++current_idx;
        }
        if (it == lines.end())
        {
            lines.emplace_back();
            --it;
        }

        lines.insert(it, commit);
    }

    void removeLine(uint32_t num)
    {
        walk(num);

        if (current_idx == num)
            it = lines.erase(it);
    }
};

using Snapshot = std::map<std::string /* path */, FileBlame>;

struct FileChangeAndLineChanges
{
    FileChangeAndLineChanges(FileChange file_change_) : file_change(file_change_) {}

    FileChange file_change;
    LineChanges line_changes;

    std::map<uint32_t, Commit> deleted_lines;
};


void processCommit(
    std::unique_ptr<ShellCommand> & commit_info,
    const Options & options,
    size_t commit_num,
    size_t total_commits,
    std::string hash,
    Snapshot & snapshot,
    Result & result)
{
    auto & in = commit_info->out;

    Commit commit;
    commit.hash = hash;

    time_t commit_time;
    readText(commit_time, in);
    commit.time = commit_time;
    assertChar('\t', in);
    readText(commit.author, in);
    assertChar('\t', in);
    std::string parent_hash;
    readString(parent_hash, in);
    assertChar('\n', in);
    readNullTerminated(commit.message, in);

    std::string message_to_print = commit.message;
    std::replace_if(message_to_print.begin(), message_to_print.end(), [](char c){ return std::iscntrl(c); }, ' ');

    fmt::print("{}%  {}  {}  {}\n",
        commit_num * 100 / total_commits, toString(commit.time), hash, message_to_print);

    if (options.skip_commits_without_parents && commit_num != 0 && parent_hash.empty())
    {
        std::cerr << "Warning: skipping commit without parents\n";
        return;
    }

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
            readText(file_change.old_path, in);
            skipWhitespaceIfAny(in);
            readText(file_change.path, in);

            snapshot[file_change.path] = snapshot[file_change.old_path];
        }
        else
        {
            readText(file_change.path, in);
        }

        file_change.file_extension = std::filesystem::path(file_change.path).extension();

        assertChar('\n', in);

        if (!(options.skip_paths && re2_st::RE2::PartialMatch(file_change.path, *options.skip_paths)))
        {
            file_changes.emplace(
                file_change.path,
                FileChangeAndLineChanges(file_change));
        }
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
                    if (file_changes.end() != it)
                        file_change_and_line_changes = &it->second;
                }

                if (file_change_and_line_changes)
                {
                    uint32_t old_lines = 1;
                    uint32_t new_lines = 1;

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

                    line_change.hunk_lines_added = new_lines;
                    line_change.hunk_lines_deleted = old_lines;

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
                        ++file_change_and_line_changes->file_change.lines_deleted;

                        line_change.sign = -1;
                        readStringUntilNextLine(line_change.line, in);
                        line_change.setLineInfo(line_change.line);

                        FileBlame & file_snapshot = snapshot[old_file_path];
                        if (const Commit * prev_commit = file_snapshot.find(line_change.line_number_old))
                        {
                            line_change.prev_commit_hash = prev_commit->hash;
                            line_change.prev_author = prev_commit->author;
                            line_change.prev_time = prev_commit->time;
                            file_change_and_line_changes->deleted_lines[line_change.line_number_old] = *prev_commit;
                            file_snapshot.removeLine(line_change.line_number_old);
                        }

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
                        ++file_change_and_line_changes->file_change.lines_added;

                        line_change.sign = 1;
                        readStringUntilNextLine(line_change.line, in);
                        line_change.setLineInfo(line_change.line);

                        FileBlame & file_snapshot = snapshot[new_file_path];
                        if (file_change_and_line_changes->deleted_lines.count(line_change.line_number_new))
                        {
                            const auto & prev_commit = file_change_and_line_changes->deleted_lines[line_change.line_number_new];
                            line_change.prev_commit_hash = prev_commit.hash;
                            line_change.prev_author = prev_commit.author;
                            line_change.prev_time = prev_commit.time;
                        }
                        file_snapshot.addLine(line_change.line_number_new, commit);

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

    if (options.diff_size_limit && commit.lines_added + commit.lines_deleted > *options.diff_size_limit)
        return;

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


auto gitShow(const std::string & hash)
{
    std::string command = fmt::format(
        "git show --raw --pretty='format:%at%x09%aN%x09%P%x0A%s%x00' --patch --unified=0 {}",
        hash);

    return ShellCommand::execute(command);
}


void processLog(const Options & options)
{
    Result result;

    std::string command = "git log --reverse --no-merges --pretty=%H";
    fmt::print("{}\n", command);
    auto git_log = ShellCommand::execute(command);

    /// Collect hashes in memory. This is inefficient but allows to display beautiful progress.
    /// The number of commits is in order of single millions for the largest repositories,
    /// so don't care about potential waste of ~100 MB of memory.

    std::vector<std::string> hashes;

    auto & in = git_log->out;
    while (!in.eof())
    {
        std::string hash;
        readString(hash, in);
        assertChar('\n', in);

        if (!options.skip_commits.count(hash))
            hashes.emplace_back(std::move(hash));
    }

    size_t num_commits = hashes.size();
    fmt::print("Total {} commits to process.\n", num_commits);

    /// Will run multiple processes in parallel
    size_t num_threads = options.threads;

    std::vector<std::unique_ptr<ShellCommand>> show_commands(num_threads);
    for (size_t i = 0; i < num_commits && i < num_threads; ++i)
        show_commands[i] = gitShow(hashes[i]);

    Snapshot snapshot;
    for (size_t i = 0; i < num_commits; ++i)
    {
        processCommit(show_commands[i % num_threads], options, i, num_commits, hashes[i], snapshot, result);
        if (i + num_threads < num_commits)
            show_commands[i % num_threads] = gitShow(hashes[i + num_threads]);
    }
}


}

int main(int argc, char ** argv)
try
{
    using namespace DB;

    po::options_description desc("Allowed options");
    desc.add_options()
        ("help,h", "produce help message")
        ("skip-commits-without-parents", po::value<bool>()->default_value(true),
            "Skip commits without parents (except the initial commit)."
            " These commits are usually erroneous but they can make sense in very rare cases.")
        ("skip-paths", po::value<std::string>(),
            "Skip paths that matches regular expression (re2 syntax).")
        ("skip-commit", po::value<std::vector<std::string>>(),
            "Skip commit with specified hash. The option can be specified multiple times.")
        ("diff-size-limit", po::value<size_t>(),
            "Skip commits whose diff size (number of added + removed lines) is larger than specified threshold")
        ("threads", po::value<size_t>()->default_value(std::thread::hardware_concurrency()),
            "Number of threads to interact with git")
    ;

    po::variables_map options;
    po::store(boost::program_options::parse_command_line(argc, argv, desc), options);

    if (options.count("help"))
    {
        std::cout << documentation << '\n'
            << "Usage: " << argv[0] << '\n'
            << desc << '\n'
            << "\nExample:\n"
            << "\n./git-to-clickhouse --diff-size-limit 100000 --skip-paths 'generated\\.cpp|^(contrib|docs?|website|libs/(libcityhash|liblz4|libdivide|libvectorclass|libdouble-conversion|libcpuid|libzstd|libfarmhash|libmetrohash|libpoco|libwidechar_width))/'\n";
        return 1;
    }

    processLog(options);
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
    throw;
}
