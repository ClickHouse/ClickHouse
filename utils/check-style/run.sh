#!/bin/bash

DIR=$(readlink -f $(dirname $0))
. "$DIR/functions.sh"
CHANGED_FILES=$(get_files_to_check)

# CHANGED_FILES=$(git ls-files --exclude-standard)
# while IFS= read -r file; do
#     if [ -e "$root/$file" ]; then
#         filtered_files+="$file"$'\n'
#     # else
#     #     echo "Skipping non-existing file: $file"
#     fi
# done <<< "$CHANGED_FILES"
# CHANGED_FILES="$filtered_files"

echo "Check $(echo "$CHANGED_FILES" |  wc -l) changed files"

echo "Check duplicates" | ts
./check-duplicate-includes.sh "$CHANGED_FILES" |& tee /test_output/duplicate_includes_output.txt

echo "Check style" | ts
./check-style "$CHANGED_FILES"              |& tee /test_output/style_output.txt

echo "Check python formatting with black" | ts
./check-black "$CHANGED_FILES"              |& tee /test_output/black_output.txt

echo "Check python type hinting with mypy" | ts
./check-mypy "$CHANGED_FILES"               |& tee /test_output/mypy_output.txt

echo "Check typos" | ts
./check-typos "$CHANGED_FILES"              |& tee /test_output/typos_output.txt

echo "Check docs spelling" | ts
./check-doc-aspell "$CHANGED_FILES"         |& tee /test_output/docs_spelling_output.txt

echo "Check workflows" | ts
./check-workflows                           |& tee /test_output/workflows_output.txt

echo "Check whitespaces" | ts
./check-whitespaces "$CHANGED_FILES"        |& tee /test_output/whitespaces_output.txt

echo "Check submodules" | ts
./check-submodules                          |& tee /test_output/submodules_output.txt

echo "Check shell scripts with shellcheck" | ts
./shellcheck-run.sh "$CHANGED_FILES"        |& tee /test_output/shellcheck_output.txt

echo "Process results" | ts
./process_style_check_result.py || echo -e "failure\tCannot parse results" > /test_output/check_status.tsv
echo "Check help for changelog generator works" | ts
cd ../changelog || exit 1
./changelog.py -h 2>/dev/null 1>&2
