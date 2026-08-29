# Supported Codex session-repair fingerprints

Read this reference only after `scripts/session_repair.py` reports an ordinal anomaly or a repair candidate.

## Storage model

- `sessions/**/rollout-*.jsonl` is the durable transcript.
- `state_*.sqlite` maps a thread ID to its rollout and history mode.
- `thread_history_*.sqlite` contains rebuildable `thread_turns`, `thread_items`, realtime items, and `thread_history_projection_state`.
- A stale TUI can therefore disagree with a complete rollout without model context being deleted.

The helper supports only cursor realignment where the relevant raw records are valid JSON, there is exactly one actionable ordinal discontinuity at or after the cursor, and the duplicated range contains no already-projected row. Earlier known duplicate groups may remain in the canonical rollout after a successful repair.

## Fingerprint A: duplicated settings metadata

```text
token_count(N)
thread_settings_applied(N)  <- cursor points here, expects N+1
next_record(N+1)
```

`thread_settings_applied` produces no thread-history item or turn change in the verified projector. The safe repair keeps the byte cursor and rewinds `next_rollout_ordinal` to `N`, allowing the metadata record to replay before normal projection continues at `N+1`. The rollout remains untouched.

## Fingerprint B: duplicated turn start

```text
token_count(N)
task_started(N)             <- cursor points here, expects N+1
next_record(N+1)
```

The turn start is semantically meaningful and must be replayed. The safe repair keeps the byte cursor and rewinds only `next_rollout_ordinal` to `N`, provided the turn and ordinal have no projected rows.

## Fingerprint C: settings plus duplicated turn start

```text
token_count(N)
thread_settings_applied(N-1) <- cursor points here, expects N+1
task_started(N)
next_record(N+1)
```

Replay both physical records by keeping the byte cursor and rewinding `next_rollout_ordinal` to `N-1`. This is the signature validated against the original double-rewind case; the helper is data-driven and does not hardcode a thread ID or offsets.

## Refuse automatic repair

Remain read-only when any of these is true:

- malformed/non-object JSONL lines anywhere, an unknown earlier anomaly, or another actionable anomaly at or after the cursor;
- the cursor is in the middle of a line;
- a duplicated user/assistant message, tool result, completion, abort, or unknown event;
- projected turns/items/realtime rows already occupy a replayed ordinal;
- the rollout path is missing or outside the selected Codex home;
- the target rollout is open or changes during the stability check;
- the stored cursor no longer matches the diagnosed cursor;
- SQLite integrity checks fail.

Do not fall back to deleting all projection rows or temporarily rewriting `history_mode`: an already-paginated rollout can still contain the same ordinal anomaly and fail again during replay.

Unrelated Codex sessions may continue using the shared SQLite databases. A repair remains target-scoped when the target rollout is closed and stable, the helper atomically acquires or creates the per-thread lock under `.coordination.lock`, target rows are backed up, `BEGIN IMMEDIATE` acquires the SQLite writer slot, the cursor is selected again inside that transaction, and the `UPDATE ... WHERE` guard changes exactly one row.

## Evidence and upstream tracking

- [openai/codex#35746](https://github.com/openai/codex/issues/35746) tracks flattened `token_count` decoding and reused resume ordinals.
- [openai/codex#38792](https://github.com/openai/codex/issues/38792) documents frozen derived projections and backup-first recovery.
- [openai/codex#40836](https://github.com/openai/codex/issues/40836) covers orphaned turns hiding later completed turns.
- [Official Codex commands](https://learn.chatgpt.com/docs/developer-commands?surface=cli) documents `codex resume SESSION_ID`.

These links explain the failure family; local artifacts remain the authority for a particular repair.
