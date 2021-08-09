#!/usr/bin/python3

import argparse
import collections
import fuzzywuzzy.fuzz
import itertools
import json
import os
import re
import sys

parser = argparse.ArgumentParser(description='Format changelog for given PRs.')
parser.add_argument('file', metavar='FILE', type=argparse.FileType('r', encoding='utf-8'), nargs='?', default=sys.stdin, help='File with PR numbers, one per line.')
args = parser.parse_args()

# This function mirrors the PR description checks in ClickhousePullRequestTrigger.
# Returns False if the PR should not be mentioned changelog.
def parse_one_pull_request(item):
    description = item['body']
    # Don't skip empty lines because they delimit parts of description
    lines = [line for line in [x.strip() for x in (description.split('\n') if description else [])]]
    lines = [re.sub(r'\s+', ' ', l) for l in lines]

    category = ''
    entry = ''

    if lines:
        i = 0
        while i < len(lines):
            if re.match(r'(?i)^[>*_ ]*change\s*log\s*category', lines[i]):
                i += 1
                if i >= len(lines):
                    break
                # Can have one empty line between header and the category itself. Filter it out.
                if not lines[i]:
                    i += 1
                    if i >= len(lines):
                        break
                category = re.sub(r'^[-*\s]*', '', lines[i])
                i += 1
            elif re.match(r'(?i)^[>*_ ]*(short\s*description|change\s*log\s*entry)', lines[i]):
                i += 1
                # Can have one empty line between header and the entry itself. Filter it out.
                if i < len(lines) and not lines[i]:
                    i += 1
                # All following lines until empty one are the changelog entry.
                entry_lines = []
                while i < len(lines) and lines[i]:
                    entry_lines.append(lines[i])
                    i += 1
                entry = ' '.join(entry_lines)
            else:
                i += 1

    if not category:
        # Shouldn't happen, because description check in CI should catch such PRs.
        # Fall through, so that it shows up in output and the user can fix it.
        category = "NO CL CATEGORY"

    # Filter out the PR categories that are not for changelog.
    if re.match(r'(?i)doc|((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)', category):
        return False

    if not entry:
        # Shouldn't happen, because description check in CI should catch such PRs.
        category = "NO CL ENTRY"
        entry = "NO CL ENTRY:  '" + item['title'] + "'"

    entry = entry.strip()
    if entry[-1] != '.':
        entry += '.'

    item['entry'] = entry
    item['category'] = category

    return True

# This array gives the preferred category order, and is also used to
# normalize category names.
categories_preferred_order = ['Backward Incompatible Change',
    'New Feature', 'Performance Improvement', 'Improvement', 'Bug Fix',
    'Build/Testing/Packaging Improvement', 'Other']

category_to_pr = collections.defaultdict(lambda: [])
users = {}
for line in args.file:
    pr = json.loads(open(f'pr{line.strip()}.json').read())
    assert(pr['number'])
    if not parse_one_pull_request(pr):
        continue

    assert(pr['category'])

    # Normalize category name
    for c in categories_preferred_order:
        if fuzzywuzzy.fuzz.ratio(pr['category'].lower(), c.lower()) >= 90:
            pr['category'] = c
            break

    category_to_pr[pr['category']].append(pr)
    user_id = pr['user']['id']
    users[user_id] = json.loads(open(f'user{user_id}.json').read())

def print_category(category):
    print(("#### " + category))
    print()
    for pr in category_to_pr[category]:
        user = users[pr["user"]["id"]]
        user_name = user["name"] if user["name"] else user["login"]

        # Substitute issue links.
        # 1) issue number w/o markdown link
        pr["entry"] = re.sub(r'([^[])#([0-9]{4,})', r'\1[#\2](https://github.com/ClickHouse/ClickHouse/issues/\2)', pr["entry"])
        # 2) issue URL w/o markdown link
        pr["entry"] = re.sub(r'([^(])https://github.com/ClickHouse/ClickHouse/issues/([0-9]{4,})', r'\1[#\2](https://github.com/ClickHouse/ClickHouse/issues/\2)', pr["entry"])

        print(f'* {pr["entry"]} [#{pr["number"]}]({pr["html_url"]}) ([{user_name}]({user["html_url"]})).')

    print()

# Print categories in preferred order
for category in categories_preferred_order:
    if category in category_to_pr:
        print_category(category)
        category_to_pr.pop(category)

# Print the rest of the categories
for category in category_to_pr:
    print_category(category)
