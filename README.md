# Codex Session Repair

[![CI](https://github.com/mulfyx/codex-session-repair/actions/workflows/ci.yml/badge.svg)](https://github.com/mulfyx/codex-session-repair/actions/workflows/ci.yml)

A backup-first Codex skill and deterministic helper for diagnosing and narrowly
repairing stale local paginated session projections.

Codex stores the durable transcript in a rollout JSONL file and serves resumed
history from a derived SQLite projection. If the projection cursor becomes
wedged at a duplicated rollout ordinal, `codex resume` can show an old turn even
though later completed work is still present on disk. This project verifies
that mismatch and repairs only the affected projection cursor when the local
evidence matches a supported fingerprint.

## Safety model

- Diagnosis is read-only by default.
- The rollout JSONL is never rewritten, truncated, or renumbered.
- Repair is limited to one guarded row in
  `thread_history_projection_state`.
- The target session must be closed, but unrelated Codex sessions may continue
  using other rows in the shared WAL databases.
- The target writer lock is held through stability checks, backup, transaction,
  and verification.
- A private backup of the complete rollout and target-owned SQLite rows is
  created before mutation.
- Unknown corruption, malformed JSONL, occupied replay ranges, schema drift,
  failed integrity checks, and unstable files fail closed.
- The helper never starts `codex resume` or contacts a model endpoint.

The current implementation recognizes three narrow resume-boundary patterns.
See [the fingerprint reference](references/fingerprints.md) for the exact
contracts.

## Requirements

- Linux
- Python 3.11 or newer
- `lsof`
- A local Codex profile using paginated thread history

Only the Python standard library is required at runtime.

## Validated versions

This revision was validated with:

- `codex-cli 0.151.0` on Linux for real paginated-session recovery;
- Python 3.14.7 for the complete local behavioral suite;
- Ruff 0.16.0, Prettier 3.9.6, and markdownlint-cli2 0.23.2 for local quality
  gates.

GitHub Actions targets Python 3.11, 3.12, and 3.13. Later Codex storage schemas
are accepted only when the helper's existing structural and safety checks still
pass; unknown changes fail closed.

## Install as a Codex skill

Clone the repository into the user skills directory:

```bash
git clone https://github.com/mulfyx/codex-session-repair.git \
  "$HOME/.agents/skills/codex-session-repair"
```

Codex detects user skills under `$HOME/.agents/skills`. If the skill does not
appear immediately, restart Codex. Invoke it explicitly with:

```text
$codex-session-repair
```

The repository root follows the official standalone skill layout: `SKILL.md`
plus optional `scripts/`, `references/`, and `agents/openai.yaml`.

## Use the helper directly

Diagnose one exact session UUID:

```bash
python3 scripts/session_repair.py SESSION_ID
```

Review the JSON report. A repairable result includes the original cursor, the
proposed cursor, a named fingerprint, and an empty `blockers` list.

Close only the target session, then apply the reported repair:

```bash
python3 scripts/session_repair.py SESSION_ID --apply --confirm-closed
```

On success, the helper reports `cursor_repaired_resume_required`. Resume the
session normally:

```bash
codex resume SESSION_ID
```

Finally, run diagnosis again. Full recovery is confirmed only when the status
is `healthy_caught_up`, the projection byte cursor equals rollout size, and the
next projection ordinal equals the final rollout ordinal plus one.

## Backups

By default, an applied repair stores its recovery bundle under:

```text
$CODEX_HOME/backups/session-repair/<timestamp>-<thread-prefix>/
```

The bundle contains:

- the byte-for-byte rollout JSONL;
- target-owned rows from the state and history databases;
- a manifest with source paths, hashes, cursor transition, and row counts.

Backup directories are mode `0700`; their files are mode `0600`. Do not attach
these artifacts to public issues because they can contain private prompts,
paths, and tool output.

## What this project does not do

- It does not reconstruct missing model output.
- It does not repair malformed or truncated rollouts.
- It does not delete or rebuild an entire history database.
- It does not mark orphaned turns complete or interrupted without a durable
  terminal event.
- It does not tolerate arbitrary duplicate ordinals.
- It does not repair the session hosting the current agent.

## Development

Run the behavioral suite:

```bash
python3 -B -m unittest discover -s tests -v
```

Run the same static checks as CI:

```bash
ruff format --check .
ruff check .
```

The tests cover all supported fingerprints, fail-closed boundaries, backup
integrity, clean-shutdown lock creation, and serialization with an unrelated
WAL writer.

## Status

The project is experimental and intentionally conservative. It has recovered
real stale projections without modifying their durable rollouts, but support is
limited to the evidence-backed fingerprints documented in this repository.

## License

This project is licensed under the [MIT License](LICENSE).

## References

- [Official OpenAI documentation: Build skills](https://developers.openai.com/codex/skills)
- [openai/codex#35746](https://github.com/openai/codex/issues/35746)
- [openai/codex#38792](https://github.com/openai/codex/issues/38792)
- [openai/codex#40836](https://github.com/openai/codex/issues/40836)
