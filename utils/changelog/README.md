## How To Generate Changelog

Generate github token:
* https://github.com/settings/tokens - keep all checkboxes unchecked, no scopes need to be enabled.

Dependencies:
```
sudo apt-get update
sudo apt-get install git python3 python3-fuzzywuzzy python3-github
python3 changelog.py -h
```

Usage example:

```
git fetch --tags # changelog.py depends on having the tags available, this will fetch them

python3 changelog.py --output=changelog-v22.4.1.2305-prestable.md --gh-user-or-token="$GITHUB_TOKEN" v21.6.2.7-prestable
python3 changelog.py --output=changelog-v22.4.1.2305-prestable.md --gh-user-or-token="$USER" --gh-password="$PASSWORD" v21.6.2.7-prestable
```
