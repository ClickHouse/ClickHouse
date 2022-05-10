#!/usr/bin/env python3
# In our CI this script runs in style-test containers

import argparse
import logging
import re
from datetime import date, timedelta
from queue import Empty, Queue
from subprocess import CalledProcessError, DEVNULL
from threading import Thread
from typing import Dict, List, Optional, TextIO

from fuzzywuzzy.fuzz import ratio  # type: ignore
from github import Github
from github.NamedUser import NamedUser
from github.PullRequest import PullRequest
from github.Repository import Repository
from git_helper import is_shallow, git_runner as runner

# This array gives the preferred category order, and is also used to
# normalize category names.
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


class Description:
    def __init__(
        self, number: int, user: NamedUser, html_url: str, entry: str, category: str
    ):
        self.number = number
        self.html_url = html_url
        self.user = user
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
        user_name = self.user.name if self.user.name else self.user.login
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


class Worker(Thread):
    def __init__(self, request_queue: Queue, repo: Repository):
        Thread.__init__(self)
        self.queue = request_queue
        self.repo = repo
        self.response = []  # type: List[Description]

    def run(self):
        while not self.queue.empty():
            try:
                number = self.queue.get()
            except Empty:
                break  # possible race condition, just continue
            api_pr = self.repo.get_pull(number)
            in_changelog = False
            merge_commit = api_pr.merge_commit_sha
            try:
                runner.run(f"git rev-parse '{merge_commit}'")
            except CalledProcessError:
                # It's possible that commit not in the repo, just continue
                logging.info("PR %s does not belong to the repo", api_pr.number)
                continue

            try:
                runner.run(
                    f"git merge-base --is-ancestor '{merge_commit}' '{TO_REF}'",
                    stderr=DEVNULL,
                )
                runner.run(
                    f"git merge-base --is-ancestor '{FROM_REF}' '{merge_commit}'",
                    stderr=DEVNULL,
                )
                in_changelog = True
            except CalledProcessError:
                # Commit is not between from and to refs
                continue
            if in_changelog:
                desc = generate_description(api_pr, self.repo)
                if desc is not None:
                    self.response.append(desc)

            self.queue.task_done()


def get_descriptions(
    repo: Repository, numbers: List[int], jobs: int
) -> Dict[str, List[Description]]:
    workers = []  # type: List[Worker]
    queue = Queue()  # type: Queue # (!?!?!?!??!)
    for number in numbers:
        queue.put(number)
    for _ in range(jobs):
        worker = Worker(queue, repo)
        worker.start()
        workers.append(worker)

    descriptions = {}  # type: Dict[str, List[Description]]
    for worker in workers:
        worker.join()
        for desc in worker.response:
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
            item = repo.get_pull(int(branch_parts[-1]))
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
        r"(?i)doc|((non|in|not|un)[-\s]*significant)|(not[ ]*for[ ]*changelog)",
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
    fd.write(f"### ClickHouse release {TO_REF} FIXME as compared to {FROM_REF}\n\n")

    seen_categories = []  # type: List[str]
    for category in categories_preferred_order:
        if category in descriptions:
            seen_categories.append(category)
            fd.write(f"#### {category}\n")
            for desc in descriptions[category]:
                fd.write(f"{desc.formatted_entry}\n")

            fd.write("\n")

    for category in descriptions:
        if category not in seen_categories:
            fd.write(f"#### {category}\n\n")
            for desc in descriptions[category]:
                fd.write(f"{desc.formatted_entry}\n")

            fd.write("\n")


def check_refs(from_ref: Optional[str], to_ref: str):
    global FROM_REF, TO_REF
    TO_REF = to_ref

    # Check TO_REF
    runner.run(f"git rev-parse {TO_REF}")

    # Check from_ref
    if from_ref is None:
        FROM_REF = runner.run(f"git describe --abbrev=0 --tags '{TO_REF}~'")
        # Check if the previsous tag is different for merge commits
        # I __assume__ we won't have octopus merges, at least for the tagged commits
        try:
            alternative_tag = runner.run(
                f"git describe --abbrev=0 --tags '{TO_REF}^2'", stderr=DEVNULL
            )
            if FROM_REF != alternative_tag:
                raise Exception(
                    f"Unable to get unified parent tag for {TO_REF}, "
                    f"define it manually, get {FROM_REF} and {alternative_tag}"
                )
        except CalledProcessError:
            pass
    else:
        runner.run(f"git rev-parse {FROM_REF}")
        FROM_REF = from_ref


def main():
    log_levels = [logging.CRITICAL, logging.WARN, logging.INFO, logging.DEBUG]
    args = parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:\n%(message)s",
        level=log_levels[min(args.verbose, 3)],
    )
    # Get the full repo
    if is_shallow():
        logging.info("Unshallow repository")
        runner.run("git fetch --unshallow", stderr=DEVNULL)
    logging.info("Fetching all tags")
    runner.run("git fetch --tags", stderr=DEVNULL)

    check_refs(args.from_ref, args.to_ref)

    logging.info("Using %s..%s as changelog interval", FROM_REF, TO_REF)

    # Get starting and ending dates for gathering PRs
    # Add one day after and before to mitigate TZ possible issues
    # `tag^{}` format gives commit ref when we have annotated tags
    from_date = runner.run(f"git log -1 --format=format:%as '{FROM_REF}^{{}}'")
    from_date = (date.fromisoformat(from_date) - timedelta(1)).isoformat()
    to_date = runner.run(f"git log -1 --format=format:%as '{TO_REF}^{{}}'")
    to_date = (date.fromisoformat(to_date) + timedelta(1)).isoformat()

    # Get all PRs for the given time frame
    gh = Github(
        args.gh_user_or_token, args.gh_password, per_page=100, pool_size=args.jobs
    )
    query = f"type:pr repo:{args.repo} is:merged merged:{from_date}..{to_date}"
    repo = gh.get_repo(args.repo)
    api_prs = gh.search_issues(query=query, sort="created")
    logging.info("Found %s PRs for the query: '%s'", api_prs.totalCount, query)

    pr_numbers = [pr.number for pr in api_prs]

    descriptions = get_descriptions(repo, pr_numbers, args.jobs)

    write_changelog(args.output, descriptions)


if __name__ == "__main__":
    main()
