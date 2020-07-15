#!/bin/bash
set -e

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

from="$1"
to="$2"
log_command=(git log "$from..$to" --first-parent)

"${log_command[@]}" > "changelog-log.txt"

# Check for diamond merges.
if "${log_command[@]}" --oneline --grep "Merge branch '" | grep ''
then
    # DO NOT ADD automated handling of diamond merges to this script.
    # It is an unsustainable way to work with git, and it MUST be visible.
    echo Warning: suspected diamond merges above.
    echo Some commits will be missed, review these manually.
fi

# NOTE keep in sync with ./backport.sh.
# Search for PR numbers in commit messages. First variant is normal merge, and second
# variant is squashed. Next are some backport message variants.
find_prs=(sed -n "s/^.*merg[eding]*.*#\([[:digit:]]\+\).*$/\1/Ip;
                  s/^.*(#\([[:digit:]]\+\))$/\1/p;
                  s/^.*back[- ]*port[ed of]*.*#\([[:digit:]]\+\).*$/\1/Ip;
                  s/^.*cherry[- ]*pick[ed of]*.*#\([[:digit:]]\+\).*$/\1/Ip")

# awk is to filter out small task numbers from different task tracker, which are
# referenced by documentation commits like '* DOCSUP-824: query log (#115)'.
"${find_prs[@]}" "changelog-log.txt" | sort -rn | uniq | awk '$0 > 1000 { print $0 }' > "changelog-prs.txt"

echo "$(wc -l < "changelog-prs.txt") PRs added between $from and $to."
if [ $(wc -l < "changelog-prs.txt") -eq 0 ] ; then exit 0 ; fi

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

rm changelog-prs-filtered.txt &> /dev/null ||:
for pr in $(cat "changelog-prs.txt")
do
    # Download PR info from github.
    file="pr$pr.json"
    github_download "https://api.github.com/repos/ClickHouse/ClickHouse/pulls/$pr" "$file" || continue

    if ! [ "$pr" == "$(jq -r .number "$file")" ]
    then
        >&2 echo "Got wrong data for PR #$pr (please check and remove '$file')."
        continue
    fi

    # Filter out PRs by bots.
    user_login=$(jq -r .user.login "$file")
    if echo "$user_login" | grep "\[bot\]$" > /dev/null
    then
        continue
    fi

    # Download author info from github.
    user_id=$(jq -r .user.id "$file")
    user_file="user$user_id.json"
    github_download "$(jq -r .user.url "$file")" "$user_file" || continue

    if ! [ "$user_id" == "$(jq -r .id "$user_file")" ]
    then
        >&2 echo "Got wrong data for user #$user_id (please check and remove '$user_file')."
        continue
    fi

    echo "$pr" >> changelog-prs-filtered.txt
done

echo "### ClickHouse release $to FIXME as compared to $from
" > changelog.md
"$script_dir/format-changelog.py" changelog-prs-filtered.txt >> changelog.md
cat changelog.md
