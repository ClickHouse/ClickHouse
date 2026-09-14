## How To Generate Changelog

Generate github token:
* https://github.com/settings/tokens - keep all checkboxes unchecked, no scopes need to be enabled.

Dependencies:
```
sudo apt-get update
sudo apt-get install git python3 python3-thefuzz python3-github
python3 changelog.py -h
```

Usage example:

Note: The working directory is ClickHouse/utils/changelog

```bash
GITHUB_TOKEN="<your token>"

python3 changelog.py --output=changelog-v22.4.1.2305-prestable.md --gh-user-or-token="$GITHUB_TOKEN" v21.6.2.7-prestable
python3 changelog.py --output=changelog-v22.4.1.2305-prestable.md --gh-user-or-token="$USER" --gh-password="$PASSWORD" v21.6.2.7-prestable
```

## How to use it on Mac

Python has notoriously poor developer experience - it is a pile of garbage.

Here is how to use it:

```bash
brew install python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
./changelog.py --gh-user-or-token ghp_... --from a79faf5da8027c201a94cdc5c9a6a1d7852d69b5 95ccb0e1a86fd26a50b11a9479586e973dba66ce
```
