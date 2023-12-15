
function get_files_to_check {
    if git merge-base --is-ancestor HEAD origin/master; then
        CHANGED_FILES=$(git ls-files --full-name --exclude-standard)
        # echo "Check all $(echo "$CHANGED_FILES" |  wc -l) repo files"
    else
        base_branch=origin/master
        CHANGED_FILES="$(git diff --name-only $(git merge-base HEAD $base_branch)..HEAD && git diff --name-only --cached --ignore-submodules && git diff --name-only --ignore-submodules | sort -u || exit 1)"
        # remove deleted files (not existing)
        root=$(git rev-parse --show-toplevel)
        while IFS= read -r file; do
            if [ -e "$root/$file" ]; then
                filtered_files+="$file"$'\n'
            # else
            #     echo "Skipping non-existing file: $file"
            fi
        done <<< "$CHANGED_FILES"
        CHANGED_FILES="$filtered_files"
        unset filtered_files
        # echo "Check $(echo "$CHANGED_FILES" |  wc -l) changed files"
    fi
    echo "$CHANGED_FILES"
}