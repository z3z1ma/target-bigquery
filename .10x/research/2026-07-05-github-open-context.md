Status: active
Created: 2026-07-05
Updated: 2026-07-05

# GitHub Open Issue and PR Context

## Question

What open repository issues and pull requests should inform, but not define, the modernization pass?

## Sources and Methods

- `gh repo view --json nameWithOwner,defaultBranchRef,url,issues,pullRequests`
- `gh issue list --state open --limit 100 --json number,title,labels,createdAt,updatedAt,url,author`
- `gh pr list --state open --limit 100 --json number,title,headRefName,baseRefName,isDraft,mergeable,reviewDecision,createdAt,updatedAt,url,author`

Observed on 2026-07-05 against `z3z1ma/target-bigquery`.

## Findings

- Repository default branch is `main`.
- There are 18 open issues.
- There are 10 open pull requests.
- Open issue themes include install failures, protobuf compatibility, Python 3.12 support, stream map aliasing, overwrite/upsert config coercion, schema/key-property transform behavior, batch failure state handling, temporary table expiry, storage write API failures, nested records, JSON column handling, and generated views.
- Open PR themes include CREATE OR REPLACE overwrite behavior, resilient MERGE column handling, column-name transforms for key properties, SDK update, dependency bumps, and GitHub Actions version bumps.

## Conclusions

These items are diagnostic context for likely risk areas. They do not independently authorize feature implementation. Source-backed or tool-backed issues found during the modernization pass may be fixed under `.10x/specs/repository-quality-modernization.md`.

## Limits

Issue and PR bodies/comments were not read in this initial context pass. The user explicitly requested that open issues and PRs not be used as implementation ideas.
