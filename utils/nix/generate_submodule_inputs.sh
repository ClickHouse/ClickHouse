#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
gitmodules="$repo_root/.gitmodules"

# Get all submodule keys like submodule.contrib/foo.path
submodules=$(git config -f "$gitmodules" --get-regexp '^submodule\..*\.path$' | cut -d' ' -f1)

for key in $submodules; do
    path=$(git config -f "$gitmodules" --get "$key")
    url=$(git config -f "$gitmodules" --get "${key/.path/.url}")

    # Strip .git suffix if present
    url_no_git=${url%.git}

    # Regex: capture org, repo
    #   \1 = org (e.g. ClickHouse)
    #   \2 = repo (e.g. jwt-cpp)
    if [[ ! "$url_no_git" =~ github.com[:/]+([^/]+)/([^/]+)$ ]]; then
        echo "❌ Error: submodule '$key' has non-GitHub URL: $url" >&2
        exit 1
    fi

    org="${BASH_REMATCH[1]}"
    repo="${BASH_REMATCH[2]}"
    rev=$(git -C "$repo_root/$path" rev-parse HEAD)

    input_name=$(basename "$path")
    echo "  contrib-${input_name} = { url = \"github:${org}/${repo}/${rev}\"; flake = false; };"
done
