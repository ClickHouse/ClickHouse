#!/bin/bash -e

# Load every line of every source file at every commit with annotation to ClickHouse table for further analysys.
# Run it from the directory that contains cloned clean git repository in ClickHouse subdirectory.
# This script is quite ugly and slow.

clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS commits 
    (
        sha String, 
        time DateTime, 
        author_name LowCardinality(String), 
        author_email LowCardinality(String), 
        title String
    ) ENGINE = MergeTree ORDER BY time;
"

clickhouse-client --query "
    CREATE TABLE IF NOT EXISTS lines
    (
        snapshot_sha LowCardinality(String),
        snapshot_time DateTime,
        snapshot_author_name LowCardinality(String),
        snapshot_author_email LowCardinality(String),
        snapshot_title LowCardinality(String),

        change_sha LowCardinality(String),
        change_time DateTime,
        change_author_name LowCardinality(String),

        path LowCardinality(String),
        name LowCardinality(String),
        extension LowCardinality(String),
        line_number UInt32,
        content LowCardinality(String)
    ) ENGINE = MergeTree ORDER BY (snapshot_time, path, name);
"

pushd ClickHouse

git checkout master
git log --grep 'Merge pull request' --pretty='format:%H,%at,%aN,%aE,%f' | clickhouse-client --query "INSERT INTO commits FORMAT CSV"

clickhouse-client --query "SELECT * FROM commits ORDER BY time DESC" | while IFS=$'\t' read -r -a FIELDS;
do
    SNAPSHOT_SHA=${FIELDS[0]}
    SNAPSHOT_TIME=${FIELDS[1]}
    SNAPSHOT_AUTHOR_NAME=${FIELDS[2]}
    SNAPSHOT_AUTHOR_EMAIL=${FIELDS[3]}
    SNAPSHOT_TITLE=${FIELDS[4]}

    echo -e "\n\033[1mProcessing commit $SNAPSHOT_SHA at $SNAPSHOT_TIME\033[0m" 1>&2
    git reset --hard $SNAPSHOT_SHA

    find . -name '*.h' -or -name '*.cpp' -or -name '*.sql' -or -name '*.sh' -or -name '*.py' -or -name '*.rst' -or -name '*.md' -or -name '*.html' -or -name 'CMakeLists.txt' -or -name '*.cmake' | grep -v -P 'contrib/|doc/|docs/|website/|\.generated\.' |
        xargs -I{} -P32 bash -c "git blame -c -l -t {} > {}.blame; echo -n '.'"
    
    echo
        
    find . -name '*.h' -or -name '*.cpp' -or -name '*.sql' -or -name '*.sh' -or -name '*.py' -or -name '*.rst' -or -name '*.md' -or -name '*.html' -or -name 'CMakeLists.txt' -or -name '*.cmake' | grep -v -P 'contrib|docs|website|\.generated\.' |
    while read file;
    do
        echo -n -e "\rProcessing file $file " 1>&2

        FILE_NAME=$(basename $file)
        FILE_PATH=$(dirname $file | sed -r 's/^\.\///')
        FILE_EXTENSION=$(echo $file | grep -o -P '\w+$')
        
        DELIM="@${RANDOM}@"
        
        cat ${file}.blame | sed -r -e 's!^(\w+)\t\(\s*(\S.*)?\t([0-9]{10}) [+\-][0-9]{4}\t([0-9]+)\)(.*)$!'"${FILE_PATH}${DELIM}${FILE_NAME}${DELIM}${FILE_EXTENSION}${DELIM}"'\1'$DELIM'\2'$DELIM'\3'$DELIM'\4'$DELIM'\5!; s/\\/\\\\/g; s/\t/\\t/g; s/'$DELIM'/\t/g'
    done |
        clickhouse-client --query "INSERT INTO lines 
        SELECT '$SNAPSHOT_SHA', '$SNAPSHOT_TIME', '$SNAPSHOT_AUTHOR_NAME', '$SNAPSHOT_AUTHOR_EMAIL', '$SNAPSHOT_TITLE', sha, time, author_name, path, name, extension, line, content
        FROM input('path String, name String, extension String, sha String, author_name String, time DateTime, line UInt32, content String') FORMAT TSV"
done
