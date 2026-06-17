"""Shared parsing of the ``-- Tags:`` / ``# Tags:`` line in functional tests.

Imported by both ``tests/clickhouse-test`` (its own directory is on ``sys.path``)
and the CI helper ``ci/jobs/scripts/find_tests.py`` (repo root on ``sys.path``) so
tag detection has a single source of truth.
"""
from typing import List, Set


def get_comment_sign(filename: str) -> str:
    if filename.endswith(".sql") or filename.endswith(".sql.j2"):
        return "--"
    if (
        filename.endswith(".sh")
        or filename.endswith(".py")
        or filename.endswith(".expect")
    ):
        return "#"
    raise ValueError(f"Unknown file_extension: {filename}")


def parse_tags_from_line(line: str, comment_sign: str) -> Set[str]:
    if not line.startswith(comment_sign):
        return set()
    tags_str = line[len(comment_sign) :].lstrip()  # noqa: ignore E203
    tags_prefix = "Tags:"
    if not tags_str.startswith(tags_prefix):
        return set()
    tags_str = tags_str[len(tags_prefix) :]  # noqa: ignore E203
    tags = {tag.strip() for tag in tags_str.split(",")}
    if "no-stress" in tags:
        raise ValueError(
            "The 'no-stress' tag is not supported: stress tests must be "
            "able to run every test. Remove it from the test."
        )
    return tags


def find_tag_line(lines: List[str], comment_sign: str) -> str:
    for line in lines:
        if line.startswith(comment_sign) and line[
            len(comment_sign) :
        ].lstrip().startswith("Tags:"):
            return line
    return ""


def read_test_tags(filepath: str) -> Set[str]:
    """Return the tags declared on the test file's ``Tags:`` line (empty set if
    none). Raises on an unknown extension; callers handle I/O errors."""
    comment_sign = get_comment_sign(filepath)
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    return parse_tags_from_line(find_tag_line(lines, comment_sign), comment_sign)
