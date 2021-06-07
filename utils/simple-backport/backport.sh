#!/bin/bash
set -e

branch="$1"
merge_base=$(git merge-base origin/master "origin/$branch")
master_git_cmd=(git log "$merge_base..origin/master" --first-parent)
# The history in back branches shouldn't be too crazy, and sometimes we have a PR
# that merges several backport commits there (3f2cba6824fddf31c30bde8c6f4f860572f4f580),
# so don't use --first-parent
branch_git_cmd=(git log "$merge_base..origin/$branch")

# Make lists of PRs that were merged into each branch. Use first parent here, or else
# we'll get weird things like seeing older master that was merged into a PR branch
# that was then merged into master.
"${master_git_cmd[@]}" > master-log.txt
"${branch_git_cmd[@]}" > "$branch-log.txt"

# Check for diamond merges.
diamonds_in_master=$("${master_git_cmd[@]}" --oneline --grep "Merge branch '")
diamonds_in_branch=$("${branch_git_cmd[@]}" --oneline --grep "Merge branch '")

if [ "$diamonds_in_master" != "" ] || [ "$diamonds_in_branch" != "" ]
then
    echo "$diamonds_in_master"
    echo "$diamonds_in_branch"
    # DO NOT ADD automated handling of diamond merges to this script.
    # It is an unsustainable way to work with git, and it MUST be visible.
    echo Warning: suspected diamond merges above.
    echo Some commits will be missed, review these manually.
fi

# NOTE keep in sync with ./backport.sh.
# Search for PR numbers in commit messages. First variant is normal merge, and second
# variant is squashed. Next are some backport message variants.
find_prs=(sed -n "s/^.*merg[eding]*.*#\([[:digit:]]\+\).*$/\1/Ip;
                  s/^.*#\([[:digit:]]\+\))$/\1/p;
                  s/^.*back[- ]*port[ed of]*.*#\([[:digit:]]\+\).*$/\1/Ip;
                  s/^.*cherry[- ]*pick[ed of]*.*#\([[:digit:]]\+\).*$/\1/Ip")

# awk is to filter out small task numbers from different task tracker, which are
# referenced by documentation commits like '* DOCSUP-824: query log (#115)'.
"${find_prs[@]}" master-log.txt | sort -rn | uniq | awk '$0 > 1000 { print $0 }' > master-prs.txt
"${find_prs[@]}" "$branch-log.txt" | sort -rn | uniq | awk '$0 > 1000 { print $0 }' > "$branch-prs.txt"

# Find all master PRs that are not in branch by calculating differences of two PR lists.
grep -f "$branch-prs.txt" -F -x -v master-prs.txt > "$branch-diff-prs.txt"

echo "$(wc -l < "$branch-diff-prs".txt) PRs differ between $branch and master."

function github_download()
{
    local url=${1}
    local file=${2}
    if ! [ -f "$file" ]
    then
        if ! curl -H "Authorization: token $GITHUB_TOKEN" \
                -sSf "$url" \
                > "$file"
        then
            >&2 echo "Failed to download '$url' to '$file'. Contents: '$(cat "$file")'."
            rm "$file"
            return 1
        fi
        sleep 0.1
    fi
}

rm "$branch-report.tsv" &> /dev/null ||:
for pr in $(cat "$branch-diff-prs.txt")
do
    # Download PR info from github.
    file="pr$pr.json"
    github_download "https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$pr" "$file" || continue

    if ! [ "$pr" == "$(jq -r .number "$file")" ]
    then
        >&2 echo "Got wrong data for PR #$pr (please check and remove '$file')."
        continue
    fi

    action="skip"

    # First, check the changelog category. We port all bugfixes.
    if jq -r .body "$file" | grep -i "^- bug[ -]*fix" > /dev/null
    then
        action="backport"
    fi

    # Next, check the tag. They might override the decision. Checks are ordered by priority.
    labels="$(jq -r .labels[].name "$file")"
    if echo "$labels" | grep -x "pr-must-backport\|v$branch-must-backport" > /dev/null; then action="backport"; fi
    if echo "$labels" | grep -x "v$branch-conflicts" > /dev/null;                       then action="conflict"; fi
    if echo "$labels" | grep -x "pr-no-backport\|v$branch-no-backport" > /dev/null;     then action="no-backport"; fi
    # FIXME Ignore "backported" labels for now. If we can't find the backport commit,
    # this means that the changelog script also won't be able to. An alternative
    # way to mark PR as backported is to add an empty commit with text like
    # "backported #12345", so that it can be found between tags and put in proper
    # place in changelog.
    #if echo "$labels" | grep -x "v$branch\|v$branch-backported" > /dev/null;            then action="done"; fi

    # Find merge commit SHA for convenience
    merge_sha="$(jq -r .merge_commit_sha "$file")"

    url="https://github.com/ClickHouse/ClickHouse/pull/$pr"
    printf "%s\t%s\t%s\t%s\t%s\n" "$action" "$pr" "$url" "$file" "$merge_sha" >> "$branch-report.tsv"
    if [ "$action" == "backport" ]
    then
        printf "%s\t%s\t%s\n" "$action" "$url" "$merge_sha"
    fi
done

echo "Done."
