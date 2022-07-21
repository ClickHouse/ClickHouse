#!/usr/bin/env python3
# In our CI this script runs in style-test containers

import argparse
import logging
import os.path as p
import os
import re
from datetime import date, timedelta
from subprocess import CalledProcessError, DEVNULL
from typing import Dict, List, Optional, TextIO

from fuzzywuzzy.fuzz import ratio  # type: ignore
from github_helper import GitHub, PullRequest, PullRequests, Repository
from github.GithubException import RateLimitExceededException, UnknownObjectException
from github.NamedUser import NamedUser
from git_helper import is_shallow, git_runner as runner

# This array gives the preferred category order, and is also used to
# normalize category names.
# Categories are used in .github/PULL_REQUEST_TEMPLATE.md, keep comments there
# updated accordingly
categories_preferred_order = (
    "Backward Incompatible Change",
    "New Feature",
    "Performance Improvement",
    "Improvement",
    "Bug Fix",
    "Build/Testing/Packaging Improvement",
    "Other",
)

FROM_REF = ""
TO_REF = ""
SHA_IN_CHANGELOG = []  # type: List[str]
gh = GitHub()
CACHE_PATH = p.join(p.dirname(p.realpath(__file__)), "gh_cache")


class Description:
    def __init__(
        self, number: int, user: NamedUser, html_url: str, entry: str, category: str
    ):
        self.number = number
        self.html_url = html_url
        self.user = gh.get_user_cached(user._rawData["login"])  # type: ignore
        self.entry = entry
        self.category = category

    @property
    def formatted_entry(self) -> str:
        # Substitute issue links.
        # 1) issue number w/o markdown link
        entry = re.sub(
            r"([^[])#([0-9]{4,})",
            r"\1[#\2](https://github.com/ClickHouse/ClickHouse/issues/\2)",
            self.entry,
        )
        # 2) issue URL w/o markdown link
        entry = re.sub(
            r"([^(])https://github.com/ClickHouse/ClickHouse/issues/([0-9]{4,})",
            r"\1[#\2](https://github.com/ClickHouse/ClickHouse/issues/\2)",
            entry,
        )
        # It's possible that we face a secondary rate limit.
        # In this case we should sleep until we get it
        while True:
            try:
                user_name = self.user.name if self.user.name else self.user.login
                break
            except UnknownObjectException:
                user_name = self.user.login
                break
            except RateLimitExceededException:
                gh.sleep_on_rate_limit()
        return (
            f"* {entry} [#{self.number}]({self.html_url}) "
            f"([{user_name}]({self.user.html_url}))."
        )

    # Sort PR descriptions by numbers
    def __eq__(self, other) -> bool:
        if not isinstance(self, type(other)):
            return NotImplemented
        return self.number == other.number

    def __lt__(self, other: "Description") -> bool:
        return self.number < other.number


def get_descriptions(prs: PullRequests) -> Dict[str, List[Description]]:
    descriptions = {}  # type: Dict[str, List[Description]]
    repos = {}  # type: Dict[str, Repository]
    for pr in prs:
        # See https://github.com/PyGithub/PyGithub/issues/2202,
        # obj._rawData doesn't spend additional API requests
        # We'll save some requests
        # pylint: disable=protected-access
        repo_name = pr._rawData["base"]["repo"]["full_name"]  # type: ignore
        # pylint: enable=protected-access
        if repo_name not in repos:
            repos[repo_name] = pr.base.repo
        in_changelog = False
        merge_commit = pr.merge_commit_sha
        try:
            runner.run(f"git rev-parse '{merge_commit}'")
        except CalledProcessError:
            # It's possible that commit not in the repo, just continue
            logging.info("PR %s does not belong to the repo", pr.number)
            continue

        in_changelog = merge_commit in SHA_IN_CHANGELOG
        if in_changelog:
            desc = generate_description(pr, repos[repo_name])
            if desc is not None:
                if desc.category not in descriptions:
                    descriptions[desc.category] = []
                descriptions[desc.category].append(desc)

    for descs in descriptions.values():
        descs.sort()

    return descriptions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Generate a changelog in MD format between given tags. "
        "It fetches all tags and unshallow the git repositore automatically",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="set the script verbosity, could be used multiple",
    )
    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="add debug logging for git_helper and github_helper",
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        default="-",
        help="output file for changelog",
    )
    parser.add_argument(
        "--repo",
        default="ClickHouse/ClickHouse",
        help="a repository to query for pull-requests from GitHub",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=10,
        help="number of jobs to get pull-requests info from GitHub API",
    )
    parser.add_argument(
        "--gh-user-or-token",
        help="user name or GH token to authenticate",
    )
    parser.add_argument(
        "--gh-password",
        help="a password that should be used when user is given",
    )
    parser.add_argument(
        "--with-testing-tags",
        action="store_true",
        help="by default '*-testing' tags are ignored, this argument enables them too",
    )
    parser.add_argument(
        "--from",
        dest="from_ref",
        help="git ref for a starting point of changelog, by default is calculated "
        "automatically to match a previous tag in history",
    )
    parser.add_argument(
        "to_ref",
        metavar="TO_REF",
        help="git ref for the changelog end",
    )
    args = parser.parse_args()
    return args


# This function mirrors the PR description checks in ClickhousePullRequestTrigger.
# Returns False if the PR should not be mentioned changelog.
def generate_description(item: PullRequest, repo: Repository) -> Optional[Description]:
    backport_number = item.number
    if item.head.ref.startswith("backport/"):
        branch_parts = item.head.ref.split("/")
        if len(branch_parts) == 3:
            try:
                item = gh.get_pull_cached(repo, int(branch_parts[-1]))
            except Exception as e:
                logging.warning("unable to get backpoted PR, exception: %s", e)
        else:
            logging.warning(
                "The branch %s doesn't match backport template, using PR %s as is",
                item.head.ref,
                item.number,
            )
    description = item.body
    # Don't skip empty lines because they delimit parts of description
    lines = [x.strip() for x in (description.split("\n") if description else [])]
    lines = [re.sub(r"\s+", " ", ln) for ln in lines]

    category = ""
    entry = ""

    if lines:
        i = 0
        while i < len(lines):
            if re.match(r"(?i)^[#>*_ ]*change\s*log\s*category", lines[i]):
                i += 1
                if i >= len(lines):
                    break
                # Can have one empty line between header and the category itself.
                # Filter it out.
                if not lines[i]:
                    i += 1
                    if i >= len(lines):
                        break
                category = re.sub(r"^[-*\s]*", "", lines[i])
                i += 1
            elif re.match(
                r"(?i)^[#>*_ ]*(short\s*description|change\s*log\s*entry)", lines[i]
            ):
                i += 1
                # Can have one empty line between header and the entry itself.
                # Filter it out.
                if i < len(lines) and not lines[i]:
                    i += 1
                # All following lines until empty one are the changelog entry.
                entry_lines = []
                while i < len(lines) and lines[i]:
                    entry_lines.append(lines[i])
                    i += 1
                entry = " ".join(entry_lines)
            else:
                i += 1

    if not category:
        # Shouldn't happen, because description check in CI should catch such PRs.
        # Fall through, so that it shows up in output and the user can fix it.
        category = "NO CL CATEGORY"

    # Filter out the PR categories that are not for changelog.
    if re.match(
        r"(?i)((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)",
        category,
    ):
        category = "NOT FOR CHANGELOG / INSIGNIFICANT"
        return Description(item.number, item.user, item.html_url, item.title, category)

    # Filter out documentations changelog
    if re.match(
        r"(?i)doc",
        category,
    ):
        return None

    if backport_number != item.number:
        entry = f"Backported in #{backport_number}: {entry}"

    if not entry:
        # Shouldn't happen, because description check in CI should catch such PRs.
        category = "NO CL ENTRY"
        entry = "NO CL ENTRY:  '" + item.title + "'"

    entry = entry.strip()
    if entry[-1] != ".":
        entry += "."

    for c in categories_preferred_order:
        if ratio(category.lower(), c.lower()) >= 90:
            category = c
            break

    return Description(item.number, item.user, item.html_url, entry, category)


def write_changelog(fd: TextIO, descriptions: Dict[str, List[Description]]):
    year = date.today().year
    to_commit = runner(f"git rev-parse {TO_REF}^{{}}")[:11]
    from_commit = runner(f"git rev-parse {FROM_REF}^{{}}")[:11]
    fd.write(
        f"---\nsidebar_position: 1\nsidebar_label: {year}\n---\n\n"
        f"# {year} Changelog\n\n"
        f"### ClickHouse release {TO_REF} ({to_commit}) FIXME "
        f"as compared to {FROM_REF} ({from_commit})\n\n"
    )

    seen_categories = []  # type: List[str]
    for category in categories_preferred_order:
        if category in descriptions:
            seen_categories.append(category)
            fd.write(f"#### {category}\n")
            for desc in descriptions[category]:
                fd.write(f"{desc.formatted_entry}\n")

            fd.write("\n")

    for category in sorted(descriptions):
        if category not in seen_categories:
            fd.write(f"#### {category}\n\n")
            for desc in descriptions[category]:
                fd.write(f"{desc.formatted_entry}\n")

            fd.write("\n")


def check_refs(from_ref: Optional[str], to_ref: str, with_testing_tags: bool):
    global FROM_REF, TO_REF
    TO_REF = to_ref

    # Check TO_REF
    runner.run(f"git rev-parse {TO_REF}")

    # Check from_ref
    if from_ref is None:
        # Get all tags pointing to TO_REF
        tags = runner.run(f"git tag --points-at '{TO_REF}^{{}}'").split("\n")
        logging.info("All tags pointing to %s:\n%s", TO_REF, tags)
        if not with_testing_tags:
            tags.append("*-testing")
        exclude = " ".join([f"--exclude='{tag}'" for tag in tags])
        cmd = f"git describe --abbrev=0 --tags {exclude} '{TO_REF}'"
        FROM_REF = runner.run(cmd)
    else:
        runner.run(f"git rev-parse {FROM_REF}")
        FROM_REF = from_ref


def set_sha_in_changelog():
    global SHA_IN_CHANGELOG
    SHA_IN_CHANGELOG = runner.run(
        f"git log --format=format:%H {FROM_REF}..{TO_REF}"
    ).split("\n")


def main():
    log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
    args = parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:\n%(message)s",
        level=log_levels[min(args.verbose, 2)],
    )
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
    # Create a cache directory
    if not p.isdir(CACHE_PATH):
        os.mkdir(CACHE_PATH, 0o700)

    # Get the full repo
    if is_shallow():
        logging.info("Unshallow repository")
        runner.run("git fetch --unshallow", stderr=DEVNULL)
    logging.info("Fetching all tags")
    runner.run("git fetch --tags", stderr=DEVNULL)

    check_refs(args.from_ref, args.to_ref, args.with_testing_tags)
    set_sha_in_changelog()

    logging.info("Using %s..%s as changelog interval", FROM_REF, TO_REF)

    # Get starting and ending dates for gathering PRs
    # Add one day after and before to mitigate TZ possible issues
    # `tag^{}` format gives commit ref when we have annotated tags
    # format %cs gives a committer date, works better for cherry-picked commits
    from_date = runner.run(f"git log -1 --format=format:%cs '{FROM_REF}^{{}}'")
    to_date = runner.run(f"git log -1 --format=format:%cs '{TO_REF}^{{}}'")
    merged = (
        date.fromisoformat(from_date) - timedelta(1),
        date.fromisoformat(to_date) + timedelta(1),
    )

    # Get all PRs for the given time frame
    global gh
    gh = GitHub(
        args.gh_user_or_token, args.gh_password, per_page=100, pool_size=args.jobs
    )
    gh.cache_path = CACHE_PATH
    query = f"type:pr repo:{args.repo} is:merged"
    prs = gh.get_pulls_from_search(query=query, merged=merged, sort="created")

    descriptions = get_descriptions(prs)

    write_changelog(args.output, descriptions)


if __name__ == "__main__":
    main()
