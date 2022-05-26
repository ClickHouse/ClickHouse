#!/bin/bash

# This is a script to automate the SECURITY.md generation in the repository root.
# The logic is the following:
# We support the latest ClickHouse Y.M stable release,
# the two releases before the latest stable,
# and the two latest LTS releases (which may be already included by the criteria above).
# The LTS releases are every Y.3 and Y.8 stable release.

echo "
# Security Policy

## Security Announcements
Security fixes will be announced by posting them in the [security changelog](https://clickhouse.com/docs/en/whats-new/security-changelog/).

## Scope and Supported Versions

The following versions of ClickHouse server are currently being supported with security updates:
"

clickhouse-local --query "
SELECT
    y::String || '.' || (y < toYear(today()) - 2000 - 1 ? '*' : m::String) AS Version,
    (n <= 3 OR (is_lts AND lts_n <= 2)) ? '✔️' : '❌' AS Supported
FROM
(
    SELECT
        y,
        m,
        count() OVER (ORDER BY y DESC, m DESC) AS n,
        m IN (3, 8) AS is_lts,
        countIf(is_lts) OVER (ORDER BY y DESC, m DESC) AS lts_n
    FROM
    (
        WITH
            extractGroups(version, 'v(\\d+).(\\d+)') AS v,
            v[1]::UInt8 AS y,
            v[2]::UInt8 AS m
        SELECT
            y,
            m
        FROM file('$(dirname "${BASH_SOURCE[0]}")/../list-versions/version_date.tsv', TSV, 'version String, date String')
        ORDER BY
            y DESC,
            m DESC
        LIMIT 1 BY
            y,
            m
    )
)
LIMIT 1 BY Version
FORMAT Markdown"

echo "
## Reporting a Vulnerability

We're extremely grateful for security researchers and users that report vulnerabilities to the ClickHouse Open Source Community. All reports are thoroughly investigated by developers.

To report a potential vulnerability in ClickHouse please send the details about it to [security@clickhouse.com](mailto:security@clickhouse.com).

### When Should I Report a Vulnerability?

- You think you discovered a potential security vulnerability in ClickHouse
- You are unsure how a vulnerability affects ClickHouse

### When Should I NOT Report a Vulnerability?

- You need help tuning ClickHouse components for security
- You need help applying security related updates
- Your issue is not security related

## Security Vulnerability Response

Each report is acknowledged and analyzed by ClickHouse maintainers within 5 working days.
As the security issue moves from triage, to identified fix, to release planning we will keep the reporter updated.

## Public Disclosure Timing

A public disclosure date is negotiated by the ClickHouse maintainers and the bug submitter. We prefer to fully disclose the bug as soon as possible once a user mitigation is available. It is reasonable to delay disclosure when the bug or the fix is not yet fully understood, the solution is not well-tested, or for vendor coordination. The timeframe for disclosure is from immediate (especially if it's already publicly known) to 90 days. For a vulnerability with a straightforward mitigation, we expect report date to disclosure date to be on the order of 7 days. 
"
