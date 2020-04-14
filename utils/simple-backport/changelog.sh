#!/bin/bash
set -e

from="$1"
to="$2"

git log "$from..$to" --first-parent > "changelog-log.txt"

# NOTE keep in sync with ./backport.sh.
# Search for PR numbers in commit messages. First variant is normal merge, and second
# variant is squashed. Next are some backport message variants.
find_prs=(sed -n "s/^.*Merge pull request #\([[:digit:]]\+\).*$/\1/p;
                  s/^.*(#\([[:digit:]]\+\))$/\1/p;
                  s/^.*back[- ]*port[ed of]*#\([[:digit:]]\+\).*$/\1/Ip;
                  s/^.*cherry[- ]*pick[ed of]*#\([[:digit:]]\+\).*$/\1/Ip")

"${find_prs[@]}" "changelog-log.txt" | sort -rn > "changelog-prs.txt"


echo "$(wc -l < "changelog-prs.txt") PRs added between $from and $to."

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
            >&2 echo "Failed to download '$url' to '$file'. Contents: '"
            >&2 cat "$file"
            >&2 echo "'."
            rm "$file"
            return 1
        fi
        sleep 0.1
    fi
}

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

    # Download author info from github.
    user_id=$(jq -r .user.id "$file")
    user_file="user$user_id.json"
    github_download "$(jq -r .user.url "$file")" "$user_file" || continue

    if ! [ "$user_id" == "$(jq -r .id "$user_file")" ]
    then
        >&2 echo "Got wrong data for user #$user_id (please check and remove '$user_file')."
        continue
    fi
done

echo "### ClickHouse release $to FIXME as compared to $from
" > changelog.md
./format-changelog.py changelog-prs.txt >> changelog.md
cat changelog.md
