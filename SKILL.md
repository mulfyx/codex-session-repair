---
name: codex-session-repair
description: Diagnose and safely repair local Codex CLI sessions whose resumed history is stale, truncated, or inconsistent with the durable rollout JSONL. Use for session IDs, paginated thread-history SQLite projection wedges, and duplicate or regressing rollout ordinals; do not use for deleting chats or reconstructing missing model output.
---

# Codex Session Repair

Treat the rollout JSONL as the durable transcript and `thread_history_*.sqlite` as a derived projection. Never infer transcript loss from the TUI alone.

## Workflow

1. Run the helper without `--apply`:

    ```bash
    python3 scripts/session_repair.py <SESSION_ID>
    ```

2. Compare the raw rollout tail, SQLite projection cursor, and latest persisted turns. Report whether the problem is:
    - a healthy or merely not-yet-materialized projection;
    - one of the helper's narrowly supported duplicate-ordinal signatures;
    - an unsupported or damaged rollout that must remain read-only.
3. If the target session is open or the user says it is in use, stop. Do not modify its SQLite row, rollout, or process state.
4. Before mutation, explain the exact guarded cursor change and obtain explicit authorization for that repair. Require only the target session to be closed. Unrelated Codex sessions may keep using other rows in the shared WAL databases; the helper relies on SQLite serialization and a guarded one-row transaction. Never repair the session that is hosting the current agent.

    ```bash
    python3 scripts/session_repair.py <SESSION_ID> --apply --confirm-closed
    ```

5. The helper must create and verify a local backup before the guarded transaction. Never bypass its blocker or add a force mode.
6. Do not automatically call `codex resume`, `thread/resume`, or a model endpoint. After repair, tell the user to resume normally. Re-run diagnosis after resume to verify that the projection cursor reached rollout EOF.

## Hard boundaries

- Never edit, delete, truncate, renumber, or normalize the rollout JSONL in place.
- Never delete the thread row, the whole history database, goals, queues, logs, or unrelated thread rows.
- Do not use browser automation, `gh`, `curl`, or Codex/model endpoints during diagnosis or repair. Local rollout and SQLite artifacts are the authority; links in the reference are background only unless the user separately asks for research.
- Never apply while host-level `lsof` reports the target rollout open, when rollout identity/size/mtime is changing, or when target-writer state cannot be established. Open state/history database or WAL/SHM handles from unrelated sessions are expected and are not blockers.
- Acquire and hold the target per-thread writer lock for the entire repair. If a clean target shutdown removed that file, create it atomically only while holding Codex's `.coordination.lock`; never create or replace it outside that protocol.
- Refuse malformed JSONL anywhere, unknown earlier anomalies, multiple actionable anomalies at or after the cursor, gaps, mid-record cursors, unknown duplicated event types, or candidates with already-projected rows in the replay range. Earlier known anomalies may remain in a rollout after a prior repair and are reported without blocking a later independent diagnosis.
- Keep backups under the selected Codex home and report their path. Do not leave the only rollback artifact in `/tmp`.
- A historical turn may remain `inProgress` when its terminal event is absent from the rollout. Do not invent a completion or interruption event merely to make the UI look cleaner.

For the supported fingerprints, repair rationale, and stop conditions, read [references/fingerprints.md](references/fingerprints.md) only when diagnosis finds an ordinal anomaly or proposes a candidate.

## Verification

For an applied repair, require all of the following:

- the helper reports one guarded SQLite row changed;
- SQLite `quick_check` is `ok` before and after;
- the rollout hash is unchanged;
- turn/item row counts are unchanged before the user's resume;
- after resume, the projection byte cursor equals rollout size and its next ordinal equals the final raw ordinal plus one.

Restart any already-running client that loads this skill after installing or updating it.
