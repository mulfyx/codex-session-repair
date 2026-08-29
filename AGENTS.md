# Project instructions

## Scope

This repository contains one standalone Codex skill and its deterministic local
repair helper. Keep changes narrowly focused on diagnosing and repairing stale
paginated session projections.

## Safety invariants

- Treat rollout JSONL as the durable transcript and history SQLite as derived
  state.
- Never rewrite, truncate, delete, renumber, or normalize a rollout.
- Never delete a thread row, a complete database, logs, queues, goals, or
  unrelated thread rows.
- Default to read-only diagnosis. Mutation requires explicit authorization,
  `--apply`, and `--confirm-closed`.
- Hold the target per-thread writer lock for the entire applied workflow.
- Permit unrelated Codex sessions only through normal SQLite WAL concurrency;
  the target session itself must be closed and stable.
- Keep the final write transaction small, guarded by the exact old cursor, and
  require exactly one changed row.
- Fail closed on unknown fingerprints, schema drift, malformed records,
  unstable files, occupied replay ranges, lock ambiguity, backup failure, or
  integrity failure.
- Never add a `--force` mode.
- Never use real transcripts, databases, home paths, thread IDs, credentials,
  or other private artifacts as committed fixtures.

## Code

- Support Python 3.11 and newer using only the standard library at runtime.
- Keep JSON output stable and machine-readable.
- Use parameterized SQL values. Dynamic identifiers are allowed only when
  derived from inspected SQLite schema and quoted defensively.
- Preserve backup permissions: directories `0700`, files `0600`.
- Do not add network access, browser automation, model calls, or automatic
  `codex resume` behavior.

## Tests

- Every supported fingerprint needs a synthetic behavioral fixture.
- Every mutation change needs a test proving rollout hash preservation, target
  row count preservation, SQLite integrity, and exact cursor transition.
- Concurrency changes must test both the target writer lock and an unrelated
  WAL reader/writer.
- Regression tests must use temporary synthetic Codex homes and clean them up.
- Do not weaken a fail-closed gate merely to make a fixture pass.

Run before handoff:

```bash
python3 -B -m unittest discover -s tests -v
ruff format --check .
ruff check .
```

## Documentation

- Keep `SKILL.md` concise and action-oriented.
- Put detailed fingerprints and conditional recovery guidance under
  `references/`.
- Keep `README.md` user-facing and avoid private incident details.
- Update safety, usage, and tests together when behavior changes.

## Git

- Use Conventional Commits, for example:
  `fix(repair): hold the target writer lock during backup`.
- Keep commits reviewable and single-purpose.
- Do not push, publish, change repository visibility, tag, or release without
  explicit authorization for that action.
- Do not add a license without the owner's explicit choice.
