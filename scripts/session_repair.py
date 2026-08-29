#!/usr/bin/env python3
"""Diagnose and narrowly repair stale Codex paginated-history projections."""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as dt
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat as stat_module
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from typing import Any

MAX_REPORTED_ANOMALIES = 20
CURSOR_LOOKAHEAD = 4
MIN_BACKUP_MARGIN = 128 * 1024 * 1024
REQUIRED_COLUMNS = {
    "threads": {"id", "rollout_path", "history_mode", "cli_version"},
    "thread_history_projection_state": {
        "thread_id",
        "next_rollout_byte_offset",
        "next_rollout_ordinal",
    },
    "thread_turns": {
        "thread_id",
        "turn_id",
        "rollout_ordinal",
        "rollout_end_ordinal",
    },
    "thread_items": {
        "thread_id",
        "rollout_ordinal",
        "updated_at_ordinal",
    },
    "thread_realtime_items": {"thread_id", "rollout_ordinal"},
}


class RepairError(RuntimeError):
    """A safe repair cannot continue."""


class TargetWriterBusy(RepairError):
    """The target Codex thread owns its per-thread writer lock."""


class TargetWriterLockMissing(RepairError):
    """The closed target has no lock file yet; apply may create it under coordination."""


@dataclasses.dataclass(frozen=True)
class RecordMeta:
    """Non-sensitive structural metadata for one rollout record."""

    line: int
    start: int
    end: int
    ordinal: int | None
    record_type: str | None
    payload_type: str | None
    turn_id: str | None
    timestamp: str | None
    structured_fractional_rate_limit: bool


@dataclasses.dataclass(frozen=True)
class RepairCandidate:
    """One guarded cursor-only repair supported by the helper."""

    kind: str
    old_offset: int
    old_ordinal: int
    new_ordinal: int
    replay_ordinal_start: int | None
    replay_ordinal_end: int | None
    rationale: str


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Diagnose or repair one local Codex session projection.",
    )
    parser.add_argument("thread_id", help="Exact Codex thread/session UUID")
    parser.add_argument(
        "--codex-home",
        type=Path,
        default=Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")),
        help="Codex home (default: CODEX_HOME or ~/.codex)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the single detected safe cursor repair after backup",
    )
    parser.add_argument(
        "--confirm-closed",
        action="store_true",
        help="Confirm the target session was closed before --apply",
    )
    parser.add_argument(
        "--backup-root",
        type=Path,
        help="Backup parent (default: <codex-home>/backups/session-repair)",
    )
    parser.add_argument(
        "--stability-seconds",
        type=float,
        default=2.0,
        help="Rollout stability interval before apply (default: 2 seconds)",
    )
    return parser.parse_args(argv)


def quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


@contextlib.contextmanager
def connect_read_only(path: Path) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout = 30000")
    try:
        yield connection
    finally:
        connection.close()


def locate_databases(codex_home: Path, prefix: str) -> list[Path]:
    pattern = re.compile(re.escape(prefix) + r"_(\d+)\.sqlite")
    candidates: list[tuple[int, Path]] = []
    for path in codex_home.glob(f"{prefix}_*.sqlite"):
        match = pattern.fullmatch(path.name)
        if match:
            candidates.append((int(match.group(1)), path))
    if not candidates:
        raise RepairError(f"no {prefix}_N.sqlite found under {codex_home}")
    return [path for _, path in sorted(candidates)]


def validate_required_columns(
    connection: sqlite3.Connection,
    tables: tuple[str, ...],
) -> None:
    for table in tables:
        actual = {
            row["name"]
            for row in connection.execute(
                f"PRAGMA table_info({quote_identifier(table)})"
            ).fetchall()
        }
        missing = REQUIRED_COLUMNS[table] - actual
        if missing:
            raise RepairError(f"unsupported schema for {table}; missing {sorted(missing)}")


def is_under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    else:
        return True


def stable_identity(path: Path) -> tuple[int, int, int, int, int]:
    value = path.stat()
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def nested_used_percent_is_float(value: object) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            if key == "used_percent" and isinstance(nested, float):
                return True
            if nested_used_percent_is_float(nested):
                return True
    elif isinstance(value, list):
        return any(nested_used_percent_is_float(item) for item in value)
    return False


def to_meta(line: int, start: int, end: int, value: dict[str, Any]) -> RecordMeta:
    payload = value.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    ordinal = value.get("ordinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal < 0:
        ordinal = None
    record_type = value.get("type")
    payload_type = payload.get("type")
    turn_id = payload.get("turn_id")
    timestamp = value.get("timestamp")
    return RecordMeta(
        line=line,
        start=start,
        end=end,
        ordinal=ordinal,
        record_type=record_type if isinstance(record_type, str) else None,
        payload_type=payload_type if isinstance(payload_type, str) else None,
        turn_id=turn_id if isinstance(turn_id, str) else None,
        timestamp=timestamp if isinstance(timestamp, str) else None,
        structured_fractional_rate_limit=(
            payload_type == "token_count" and nested_used_percent_is_float(payload)
        ),
    )


def public_meta(meta: RecordMeta | None) -> dict[str, Any] | None:
    return dataclasses.asdict(meta) if meta is not None else None


def timestamps_non_decreasing(records: list[RecordMeta]) -> bool:
    timestamps = [record.timestamp for record in records]
    if any(timestamp is None for timestamp in timestamps):
        return False
    return timestamps == sorted(timestamps)


def is_known_prior_anomaly(previous: RecordMeta, current: RecordMeta) -> bool:
    return (
        previous.payload_type == "token_count"
        and previous.structured_fractional_rate_limit
        and current.payload_type in {"thread_settings_applied", "task_started"}
        and previous.ordinal is not None
        and current.ordinal is not None
        and current.ordinal <= previous.ordinal
        and timestamps_non_decreasing([previous, current])
    )


def scan_rollout(path: Path, cursor_offset: int | None) -> dict[str, Any]:
    malformed_samples: list[dict[str, Any]] = []
    anomaly_samples: list[dict[str, Any]] = []
    cursor_records: list[RecordMeta] = []
    cursor_midline = False
    cursor_anomaly: dict[str, Any] | None = None
    previous_at_cursor: RecordMeta | None = None
    previous_ordinal_record: RecordMeta | None = None
    final_ordinal_record: RecordMeta | None = None
    first_ordinal: int | None = None
    ordinal_count = 0
    line_count = 0
    malformed_count = 0
    malformed_at_or_after_cursor = 0
    invalid_ordinal_count = 0
    invalid_ordinals_at_or_after_cursor = 0
    anomaly_count = 0
    anomalies_at_or_after_cursor = 0
    unknown_anomalies_before_cursor = 0
    session_meta: dict[str, Any] = {}
    capture_cursor = False

    with path.open("rb") as handle:
        while True:
            start = handle.tell()
            raw = handle.readline()
            if not raw:
                break
            end = handle.tell()
            line_count += 1
            if cursor_offset is not None and start < cursor_offset < end:
                cursor_midline = True
            try:
                value = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                malformed_count += 1
                if cursor_offset is None or start >= cursor_offset:
                    malformed_at_or_after_cursor += 1
                if len(malformed_samples) < 5:
                    malformed_samples.append(
                        {"line": line_count, "start": start, "error": type(error).__name__}
                    )
                continue
            if not isinstance(value, dict):
                malformed_count += 1
                if cursor_offset is None or start >= cursor_offset:
                    malformed_at_or_after_cursor += 1
                if len(malformed_samples) < 5:
                    malformed_samples.append(
                        {"line": line_count, "start": start, "error": "non_object"}
                    )
                continue
            meta = to_meta(line_count, start, end, value)
            if line_count == 1 and meta.record_type == "session_meta":
                payload = value.get("payload")
                if isinstance(payload, dict):
                    session_meta = {
                        "id": payload.get("id"),
                        "session_id": payload.get("session_id"),
                        "history_mode": payload.get("history_mode"),
                        "cli_version": payload.get("cli_version"),
                    }
            if cursor_offset is not None and end == cursor_offset:
                previous_at_cursor = meta
            if cursor_offset is not None and start == cursor_offset:
                capture_cursor = True
            if capture_cursor and len(cursor_records) < CURSOR_LOOKAHEAD:
                cursor_records.append(meta)
            if meta.ordinal is None:
                invalid_ordinal_count += 1
                if cursor_offset is None or start >= cursor_offset:
                    invalid_ordinals_at_or_after_cursor += 1
                continue
            ordinal_count += 1
            if first_ordinal is None:
                first_ordinal = meta.ordinal
            if (
                previous_ordinal_record is not None
                and meta.ordinal != previous_ordinal_record.ordinal + 1
            ):
                anomaly_count += 1
                anomaly = {
                    "previous": public_meta(previous_ordinal_record),
                    "current": public_meta(meta),
                }
                if len(anomaly_samples) < MAX_REPORTED_ANOMALIES:
                    anomaly_samples.append(anomaly)
                if cursor_offset is None or start >= cursor_offset:
                    anomalies_at_or_after_cursor += 1
                elif not is_known_prior_anomaly(previous_ordinal_record, meta):
                    unknown_anomalies_before_cursor += 1
                if cursor_offset is not None and start == cursor_offset:
                    cursor_anomaly = anomaly
            previous_ordinal_record = meta
            final_ordinal_record = meta

    stat = path.stat()
    return {
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "line_count": line_count,
        "ordinal_count": ordinal_count,
        "first_ordinal": first_ordinal,
        "last_ordinal": final_ordinal_record.ordinal if final_ordinal_record else None,
        "malformed_count": malformed_count,
        "malformed_at_or_after_cursor": malformed_at_or_after_cursor,
        "malformed_samples": malformed_samples,
        "invalid_ordinal_count": invalid_ordinal_count,
        "invalid_ordinals_at_or_after_cursor": invalid_ordinals_at_or_after_cursor,
        "anomaly_count": anomaly_count,
        "anomalies_at_or_after_cursor": anomalies_at_or_after_cursor,
        "unknown_anomalies_before_cursor": unknown_anomalies_before_cursor,
        "anomaly_samples": anomaly_samples,
        "cursor_anomaly": cursor_anomaly,
        "cursor_midline": cursor_midline,
        "previous_at_cursor": previous_at_cursor,
        "cursor_records": cursor_records,
        "session_meta": session_meta,
    }


def check_paths_open(paths: list[Path]) -> dict[str, Any]:
    executable = shutil.which("lsof")
    if executable is None:
        return {"status": "unknown", "reason": "lsof_not_found", "process_count": None}
    existing = [path for path in paths if path.exists()]
    result = subprocess.run(  # noqa: S603 -- fixed lsof argv; no shell is used.
        [executable, "-Fpc", "--", *[str(path) for path in existing]],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 1 and not result.stdout.strip():
        return {"status": "closed", "reason": None, "process_count": 0}
    if result.returncode not in (0, 1):
        return {
            "status": "unknown",
            "reason": f"lsof_exit_{result.returncode}",
            "process_count": None,
        }
    pids = {
        line[1:]
        for line in result.stdout.splitlines()
        if line.startswith("p") and line[1:].isdigit()
    }
    return {
        "status": "open" if pids else "closed",
        "reason": None,
        "process_count": len(pids),
        "checked_path_count": len(existing),
    }


@contextlib.contextmanager
def hold_thread_writer_lock(
    codex_home: Path,
    thread_id: str,
    *,
    create_missing: bool = False,
) -> Iterator[Path]:
    """Hold Codex's existing per-thread writer lock without creating a new file."""
    lock_dir = codex_home / "thread-writer-locks"
    coordination_path = lock_dir / ".coordination.lock"
    lock_path = lock_dir / f"{thread_id}.lock"

    def open_regular_lock(path: Path):
        if path.is_symlink() or not path.is_file():
            raise RepairError(f"missing regular writer lock: {path}")
        flags = os.O_RDONLY | os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        opened = os.fdopen(descriptor, "rb", closefd=True)
        if not stat_module.S_ISREG(os.fstat(opened.fileno()).st_mode):
            opened.close()
            raise RepairError(f"writer lock is not regular: {path}")
        return opened

    coordination = open_regular_lock(coordination_path)
    handle = None
    try:
        try:
            fcntl.flock(coordination.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TargetWriterBusy(
                f"writer-lock coordination is busy: {coordination_path}"
            ) from error
        if not lock_path.exists():
            if not create_missing:
                raise TargetWriterLockMissing(f"target writer lock is absent: {lock_path}")
            flags = os.O_RDWR | os.O_CLOEXEC | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(lock_path, flags, 0o644)
            handle = os.fdopen(descriptor, "r+b", closefd=True)
        else:
            handle = open_regular_lock(lock_path)
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TargetWriterBusy(f"target writer lock is busy: {lock_path}") from error
        fcntl.flock(coordination.fileno(), fcntl.LOCK_UN)
        coordination.close()
        yield lock_path
    finally:
        if not coordination.closed:
            try:
                fcntl.flock(coordination.fileno(), fcntl.LOCK_UN)
            finally:
                coordination.close()
        if handle is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


def probe_thread_writer_lock(codex_home: Path, thread_id: str) -> dict[str, Any]:
    """Report whether the target's per-thread writer lock can be acquired now."""
    try:
        with hold_thread_writer_lock(codex_home, thread_id) as lock_path:
            return {"status": "available", "path": str(lock_path)}
    except TargetWriterLockMissing:
        return {
            "status": "available_missing",
            "path": str(codex_home / "thread-writer-locks" / f"{thread_id}.lock"),
        }
    except TargetWriterBusy:
        return {
            "status": "busy",
            "path": str(codex_home / "thread-writer-locks" / f"{thread_id}.lock"),
        }
    except (OSError, RepairError) as error:
        return {
            "status": "unknown",
            "path": str(codex_home / "thread-writer-locks" / f"{thread_id}.lock"),
            "reason": str(error),
        }


def projected_rows_in_range(
    history: sqlite3.Connection,
    thread_id: str,
    start: int,
    end: int,
) -> dict[str, int]:
    return {
        "items": history.execute(
            """
            SELECT COUNT(*) FROM thread_items
            WHERE thread_id = ?
              AND (rollout_ordinal BETWEEN ? AND ? OR updated_at_ordinal BETWEEN ? AND ?)
            """,
            (thread_id, start, end, start, end),
        ).fetchone()[0],
        "turns": history.execute(
            """
            SELECT COUNT(*) FROM thread_turns
            WHERE thread_id = ?
              AND (rollout_ordinal BETWEEN ? AND ?
                   OR rollout_end_ordinal BETWEEN ? AND ?)
            """,
            (thread_id, start, end, start, end),
        ).fetchone()[0],
        "realtime": history.execute(
            """
            SELECT COUNT(*) FROM thread_realtime_items
            WHERE thread_id = ? AND rollout_ordinal BETWEEN ? AND ?
            """,
            (thread_id, start, end),
        ).fetchone()[0],
    }


def turn_exists(history: sqlite3.Connection, thread_id: str, turn_id: str | None) -> bool:
    if turn_id is None:
        return True
    return (
        history.execute(
            "SELECT 1 FROM thread_turns WHERE thread_id = ? AND turn_id = ?",
            (thread_id, turn_id),
        ).fetchone()
        is not None
    )


def detect_candidate(
    history: sqlite3.Connection,
    thread_id: str,
    scan: dict[str, Any],
    projection: tuple[int, int] | None,
) -> tuple[RepairCandidate | None, list[str]]:
    if projection is None:
        return None, ["projection_state_missing"]
    old_offset, expected = projection
    if (
        old_offset == scan["size"]
        and scan["last_ordinal"] is not None
        and expected == scan["last_ordinal"] + 1
    ):
        return None, []
    records: list[RecordMeta] = scan["cursor_records"]
    if records and records[0].ordinal == expected:
        return None, []

    blockers: list[str] = []
    if scan["malformed_count"]:
        blockers.append("rollout_has_malformed_records")
    if scan["invalid_ordinal_count"]:
        blockers.append("rollout_has_invalid_or_missing_ordinals")
    if scan["unknown_anomalies_before_cursor"]:
        blockers.append("rollout_has_unknown_anomaly_before_cursor")
    if scan["anomalies_at_or_after_cursor"] != 1:
        blockers.append("repair_requires_one_actionable_anomaly_at_cursor")
    if scan["cursor_midline"]:
        blockers.append("projection_cursor_is_mid_record")
    if scan["cursor_anomaly"] is None:
        blockers.append("ordinal_anomaly_does_not_start_at_projection_cursor")
    if blockers:
        return None, blockers

    previous: RecordMeta | None = scan["previous_at_cursor"]
    if previous is None or previous.payload_type != "token_count":
        return None, ["cursor_is_not_after_token_count"]
    if not previous.structured_fractional_rate_limit:
        blockers.append("token_count_lacks_fractional_structured_rate_limit_fingerprint")
    if len(records) < 2:
        return None, [*blockers, "insufficient_records_after_cursor"]

    first = records[0]
    second = records[1]
    candidate: RepairCandidate | None = None
    if (
        first.payload_type == "thread_settings_applied"
        and first.ordinal == expected - 1
        and second.ordinal == expected
    ):
        candidate = RepairCandidate(
            kind="replay_duplicate_thread_settings",
            old_offset=old_offset,
            old_ordinal=expected,
            new_ordinal=expected - 1,
            replay_ordinal_start=expected - 1,
            replay_ordinal_end=expected - 1,
            rationale="replay one projection-neutral duplicated settings record",
        )
    elif (
        first.payload_type == "task_started"
        and first.ordinal == expected - 1
        and second.ordinal == expected
    ):
        candidate = RepairCandidate(
            kind="replay_duplicate_task_start",
            old_offset=old_offset,
            old_ordinal=expected,
            new_ordinal=expected - 1,
            replay_ordinal_start=expected - 1,
            replay_ordinal_end=expected - 1,
            rationale="replay the meaningful duplicated task start",
        )
    elif len(records) >= 3:
        third = records[2]
        if (
            first.payload_type == "thread_settings_applied"
            and first.ordinal == expected - 2
            and second.payload_type == "task_started"
            and second.ordinal == expected - 1
            and third.ordinal == expected
        ):
            candidate = RepairCandidate(
                kind="replay_settings_and_duplicate_task_start",
                old_offset=old_offset,
                old_ordinal=expected,
                new_ordinal=expected - 2,
                replay_ordinal_start=expected - 2,
                replay_ordinal_end=expected - 1,
                rationale="replay duplicated settings and the meaningful task start",
            )

    if candidate is None:
        return None, [*blockers, "unsupported_ordinal_boundary_shape"]
    relevant_records = [
        previous,
        *[
            record
            for record in records
            if record.ordinal is not None and record.ordinal <= expected
        ],
    ]
    if not timestamps_non_decreasing(relevant_records):
        blockers.append("candidate_timestamps_are_missing_or_regress")
    realtime_count = history.execute(
        "SELECT COUNT(*) FROM thread_realtime_items WHERE thread_id = ?",
        (thread_id,),
    ).fetchone()[0]
    if realtime_count:
        blockers.append("target_has_realtime_history_items")
    if candidate.replay_ordinal_start is not None:
        occupied = projected_rows_in_range(
            history,
            thread_id,
            candidate.replay_ordinal_start,
            candidate.replay_ordinal_end,
        )
        if any(occupied.values()):
            blockers.append(f"replay_range_already_projected:{occupied}")
        task_start = next(
            (
                record
                for record in records
                if record.payload_type == "task_started"
                and record.ordinal is not None
                and candidate.replay_ordinal_start
                <= record.ordinal
                <= candidate.replay_ordinal_end
            ),
            None,
        )
        if task_start is not None and turn_exists(history, thread_id, task_start.turn_id):
            blockers.append("replayed_task_start_already_projected")
    return (candidate if not blockers else None), blockers


def query_projection_summary(
    history: sqlite3.Connection,
    thread_id: str,
) -> dict[str, Any]:
    projection_rows = history.execute(
        """
        SELECT next_rollout_byte_offset, next_rollout_ordinal
        FROM thread_history_projection_state WHERE thread_id = ?
        """,
        (thread_id,),
    ).fetchall()
    if len(projection_rows) > 1:
        raise RepairError("multiple projection-state rows found for one thread")
    projection = tuple(projection_rows[0]) if projection_rows else None
    return {
        "cursor": projection,
        "turn_count": history.execute(
            "SELECT COUNT(*) FROM thread_turns WHERE thread_id = ?", (thread_id,)
        ).fetchone()[0],
        "item_count": history.execute(
            "SELECT COUNT(*) FROM thread_items WHERE thread_id = ?", (thread_id,)
        ).fetchone()[0],
        "realtime_count": history.execute(
            "SELECT COUNT(*) FROM thread_realtime_items WHERE thread_id = ?", (thread_id,)
        ).fetchone()[0],
    }


def build_report(thread_id: str, codex_home: Path) -> dict[str, Any]:
    try:
        if str(uuid.UUID(thread_id)) != thread_id.lower():
            raise ValueError
    except ValueError as error:
        raise RepairError("thread_id must be a canonical UUID") from error
    codex_home = codex_home.expanduser().resolve()
    state_paths = locate_databases(codex_home, "state")
    history_paths = locate_databases(codex_home, "thread_history")
    state_path = state_paths[-1]
    history_path = history_paths[-1]
    for database_path in (state_path, history_path):
        if database_path.is_symlink() or not stat_module.S_ISREG(database_path.stat().st_mode):
            raise RepairError(f"database is not a regular non-symlink file: {database_path}")
    with connect_read_only(state_path) as state:
        validate_required_columns(state, ("threads",))
        state_journal_mode = state.execute("PRAGMA journal_mode").fetchone()[0]
        thread_rows = state.execute(
            """
            SELECT id, rollout_path, history_mode, cli_version
            FROM threads WHERE id = ?
            """,
            (thread_id,),
        ).fetchall()
    if len(thread_rows) != 1:
        if not thread_rows:
            raise RepairError(f"thread {thread_id} is absent from {state_path.name}")
        raise RepairError("multiple thread rows found for one UUID")
    thread = thread_rows[0]
    recorded_rollout_path = Path(thread["rollout_path"]).expanduser()
    if recorded_rollout_path.is_symlink():
        raise RepairError("rollout path itself is a symlink")
    rollout_path = recorded_rollout_path.resolve()
    sessions_root = (codex_home / "sessions").resolve()
    if not is_under(rollout_path, sessions_root):
        raise RepairError("rollout path resolves outside the selected Codex sessions directory")
    if not rollout_path.name.endswith(f"-{thread_id}.jsonl"):
        raise RepairError("rollout filename does not end with the thread UUID")
    if not rollout_path.exists() or not stat_module.S_ISREG(rollout_path.stat().st_mode):
        raise RepairError(f"rollout does not exist: {rollout_path}")

    with connect_read_only(history_path) as history:
        history_journal_mode = history.execute("PRAGMA journal_mode").fetchone()[0]
        validate_required_columns(
            history,
            (
                "thread_history_projection_state",
                "thread_turns",
                "thread_items",
                "thread_realtime_items",
            ),
        )
        projection_summary = query_projection_summary(history, thread_id)
        projection = projection_summary["cursor"]
        scan = scan_rollout(rollout_path, projection[0] if projection else None)
        candidate, blockers = detect_candidate(
            history,
            thread_id,
            scan,
            projection,
        )

    meta_ids = {scan["session_meta"].get("id"), scan["session_meta"].get("session_id")}
    if thread_id not in meta_ids:
        blockers.append("session_meta_id_mismatch")
        candidate = None
    if thread["history_mode"] != "paginated":
        blockers.append("state_history_mode_is_not_paginated")
        candidate = None
    if scan["session_meta"].get("history_mode") != "paginated":
        blockers.append("rollout_history_mode_is_not_paginated")
        candidate = None
    if len(state_paths) != 1 or len(history_paths) != 1:
        blockers.append("multiple_state_or_history_database_shards")
        candidate = None
    if state_journal_mode.lower() != "wal" or history_journal_mode.lower() != "wal":
        blockers.append("concurrent_repair_requires_wal")

    if projection is None:
        status = "projection_missing"
    elif (
        projection[0] == scan["size"]
        and scan["last_ordinal"] is not None
        and projection[1] == scan["last_ordinal"] + 1
    ):
        status = "healthy_caught_up"
    elif scan["cursor_records"] and scan["cursor_records"][0].ordinal == projection[1]:
        status = "aligned_but_not_materialized"
    elif candidate is not None:
        status = "repairable_duplicate_ordinal_projection"
    else:
        status = "unsupported_or_inconsistent"

    target_access = check_paths_open([rollout_path])
    writer_lock = probe_thread_writer_lock(codex_home, thread_id)
    if target_access["status"] != "closed":
        blockers.append(f"target_rollout_{target_access['status']}")
        if candidate is not None:
            status = "busy"
    if writer_lock["status"] not in {"available", "available_missing"}:
        blockers.append(f"target_writer_lock_{writer_lock['status']}")
        if candidate is not None:
            status = "busy"

    rollout_report = {
        key: value
        for key, value in scan.items()
        if key not in {"previous_at_cursor", "cursor_records"}
    }
    rollout_report["previous_at_cursor"] = public_meta(scan["previous_at_cursor"])
    rollout_report["cursor_records"] = [public_meta(record) for record in scan["cursor_records"]]
    return {
        "mode": "diagnose",
        "thread_id": thread_id,
        "status": status,
        "paths": {
            "codex_home": str(codex_home),
            "state_db": str(state_path),
            "history_db": str(history_path),
            "rollout": str(rollout_path),
        },
        "database_shards": {
            "state": [str(path) for path in state_paths],
            "thread_history": [str(path) for path in history_paths],
        },
        "database_journal_modes": {
            "state": state_journal_mode,
            "thread_history": history_journal_mode,
        },
        "thread": {
            "history_mode": thread["history_mode"],
            "cli_version": thread["cli_version"],
        },
        "rollout": rollout_report,
        "projection": projection_summary,
        "candidate": dataclasses.asdict(candidate) if candidate else None,
        "target_access": target_access,
        "writer_lock": writer_lock,
        "blockers": sorted(set(blockers)),
        "next_action": (
            "close the target session, obtain repair authorization, then use "
            "--apply --confirm-closed"
            if candidate
            and target_access["status"] == "closed"
            and writer_lock["status"] in {"available", "available_missing"}
            and not any(blocker == "concurrent_repair_requires_wal" for blocker in blockers)
            else "remain read-only or resume normally; do not guess a repair"
        ),
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_table(
    source: sqlite3.Connection,
    destination: sqlite3.Connection,
    source_table: str,
    destination_table: str,
    thread_id: str,
) -> int:
    columns = source.execute(f"PRAGMA table_info({quote_identifier(source_table)})").fetchall()
    if not columns:
        raise RepairError(f"missing table {source_table}")
    definitions = ", ".join(
        f"{quote_identifier(column['name'])} {column['type'] or 'BLOB'}" for column in columns
    )
    destination.execute(f"CREATE TABLE {quote_identifier(destination_table)} ({definitions})")
    names = [column["name"] for column in columns]
    quoted = ", ".join(quote_identifier(name) for name in names)
    placeholders = ", ".join("?" for _ in names)
    id_column = "thread_id" if "thread_id" in names else "id"
    select_query = (
        f"SELECT {quoted} FROM {quote_identifier(source_table)} "  # noqa: S608 -- quoted names.
        f"WHERE {quote_identifier(id_column)} = ?"
    )
    rows = source.execute(
        select_query,
        (thread_id,),
    ).fetchall()
    insert_query = (
        f"INSERT INTO {quote_identifier(destination_table)} ({quoted}) VALUES ({placeholders})"  # noqa: S608 -- every identifier comes from the inspected SQLite schema.
    )
    destination.executemany(
        insert_query,
        [tuple(row[name] for name in names) for row in rows],
    )
    return len(rows)


def copy_rollout(source: Path, destination: Path) -> None:
    cp = shutil.which("cp")
    result: subprocess.CompletedProcess[str] | None = None
    if cp is not None:
        result = subprocess.run(  # noqa: S603 -- fixed cp argv; no shell is used.
            [
                cp,
                "--reflink=auto",
                "--preserve=timestamps",
                "--",
                str(source),
                str(destination),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    if result is None or result.returncode != 0:
        shutil.copy2(source, destination)


def fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_backup(report: dict[str, Any], backup_root: Path) -> tuple[Path, dict[str, Any]]:
    thread_id = report["thread_id"]
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    rollout_path = Path(report["paths"]["rollout"])
    free_bytes = shutil.disk_usage(Path(report["paths"]["codex_home"])).free
    required_bytes = rollout_path.stat().st_size + MIN_BACKUP_MARGIN
    if free_bytes < required_bytes:
        raise RepairError(
            f"insufficient backup space: need at least {required_bytes}, have {free_bytes}"
        )
    backup_dir = backup_root / f"{timestamp}-{thread_id[:8]}"
    backup_dir.mkdir(parents=True, mode=0o700, exist_ok=False)
    backup_dir.chmod(0o700)

    rollout_backup = backup_dir / "rollout.jsonl"
    source_identity = stable_identity(rollout_path)
    source_hash = sha256_file(rollout_path)
    copy_rollout(rollout_path, rollout_backup)
    rollout_backup.chmod(0o600)
    backup_hash = sha256_file(rollout_backup)
    if source_identity != stable_identity(rollout_path):
        raise RepairError("rollout changed while the backup was being created")
    if source_hash != backup_hash or source_hash != sha256_file(rollout_path):
        raise RepairError("rollout backup hash mismatch")
    fsync_file(rollout_backup)

    rows_backup = backup_dir / "target-rows.sqlite"
    destination = sqlite3.connect(rows_backup)
    counts: dict[str, int] = {}
    with connect_read_only(Path(report["paths"]["state_db"])) as state:
        state.execute("BEGIN")
        counts["state_thread"] = copy_table(
            state, destination, "threads", "state_thread", thread_id
        )
        state.rollback()
    with connect_read_only(Path(report["paths"]["history_db"])) as history:
        history.execute("BEGIN")
        for source_table, destination_table in (
            ("thread_history_projection_state", "history_projection"),
            ("thread_turns", "history_turns"),
            ("thread_items", "history_items"),
            ("thread_realtime_items", "history_realtime_items"),
        ):
            counts[destination_table] = copy_table(
                history,
                destination,
                source_table,
                destination_table,
                thread_id,
            )
        history.rollback()
    destination.commit()
    quick_check = destination.execute("PRAGMA quick_check").fetchone()[0]
    destination.close()
    rows_backup.chmod(0o600)
    if quick_check != "ok":
        raise RepairError(f"target-row backup quick_check failed: {quick_check}")
    if counts["state_thread"] != 1 or counts["history_projection"] != 1:
        raise RepairError("target-row backup does not contain exactly one state/projection row")
    fsync_file(rows_backup)

    manifest = {
        "created_at": dt.datetime.now(dt.UTC).isoformat(),
        "thread_id": thread_id,
        "source_paths": report["paths"],
        "rollout_sha256": source_hash,
        "rollout_size": rollout_path.stat().st_size,
        "rollout_identity": source_identity,
        "candidate": report["candidate"],
        "row_counts": counts,
        "rows_backup_quick_check": quick_check,
    }
    manifest_path = backup_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    manifest_path.chmod(0o600)
    fsync_file(manifest_path)
    fsync_directory(backup_dir)
    return backup_dir, manifest


def validate_stability(path: Path, seconds: float) -> None:
    if seconds < 0 or seconds > 30:
        raise RepairError("stability-seconds must be between 0 and 30")
    before = stable_identity(path)
    time.sleep(seconds)
    after = stable_identity(path)
    if before != after:
        raise RepairError("rollout changed during the stability check")


def quick_check_database(path: Path) -> str:
    with connect_read_only(path) as connection:
        return connection.execute("PRAGMA quick_check").fetchone()[0]


def apply_repair(
    initial: dict[str, Any],
    codex_home: Path,
    backup_root: Path,
    stability_seconds: float,
) -> dict[str, Any]:
    """Acquire and hold the target writer lock for the complete repair."""
    if initial["candidate"] is None:
        raise RepairError("diagnosis did not produce a supported repair candidate")
    if initial["target_access"]["status"] != "closed":
        raise RepairError("target rollout is open or handle state is unknown")
    if initial["writer_lock"]["status"] not in {"available", "available_missing"}:
        raise RepairError("target writer lock is busy or unavailable")
    if any(mode.lower() != "wal" for mode in initial["database_journal_modes"].values()):
        raise RepairError("concurrent repair requires WAL mode for both databases")
    resolved_home = codex_home.expanduser().resolve()
    with hold_thread_writer_lock(
        resolved_home,
        initial["thread_id"],
        create_missing=True,
    ):
        return _apply_repair_locked(
            initial,
            resolved_home,
            backup_root,
            stability_seconds,
        )


def _apply_repair_locked(
    initial: dict[str, Any],
    codex_home: Path,
    backup_root: Path,
    stability_seconds: float,
) -> dict[str, Any]:
    """Apply one target cursor repair while its Codex writer lock is held."""
    rollout_path = Path(initial["paths"]["rollout"])
    validate_stability(rollout_path, stability_seconds)
    refreshed = build_report(initial["thread_id"], codex_home)
    if refreshed["candidate"] != initial["candidate"]:
        raise RepairError("repair candidate changed after the stability check")
    if refreshed["target_access"]["status"] != "closed":
        raise RepairError("target rollout became open before apply")
    if any(mode.lower() != "wal" for mode in refreshed["database_journal_modes"].values()):
        raise RepairError("database journal mode changed before apply")
    current = refreshed

    history_path = Path(current["paths"]["history_db"])
    state_path = Path(current["paths"]["state_db"])
    state_quick_check = quick_check_database(state_path)
    if state_quick_check != "ok":
        raise RepairError(f"state database quick_check failed: {state_quick_check}")
    before_quick_check = quick_check_database(history_path)
    if before_quick_check != "ok":
        raise RepairError(f"history database quick_check failed: {before_quick_check}")
    before_counts = current["projection"].copy()
    backup_dir, manifest = create_backup(current, backup_root)
    source_hash = manifest["rollout_sha256"]

    candidate = RepairCandidate(**current["candidate"])
    final_access = check_paths_open([rollout_path])
    if final_access["status"] != "closed":
        raise RepairError(f"target rollout opened after backup; backup: {backup_dir}")
    if stable_identity(rollout_path) != tuple(manifest["rollout_identity"]):
        raise RepairError(f"rollout identity changed after backup; backup: {backup_dir}")
    connection = sqlite3.connect(history_path, timeout=30)
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("BEGIN IMMEDIATE")
    transaction_summary = query_projection_summary(connection, current["thread_id"])
    current_cursor = transaction_summary["cursor"]
    if current_cursor != (candidate.old_offset, candidate.old_ordinal):
        connection.rollback()
        connection.close()
        raise RepairError(f"projection cursor changed before update; backup: {backup_dir}")
    for field in ("turn_count", "item_count", "realtime_count"):
        if transaction_summary[field] != before_counts[field]:
            connection.rollback()
            connection.close()
            raise RepairError(f"{field} changed before update; backup: {backup_dir}")
    if candidate.replay_ordinal_start is not None:
        occupied = projected_rows_in_range(
            connection,
            current["thread_id"],
            candidate.replay_ordinal_start,
            candidate.replay_ordinal_end,
        )
        if any(occupied.values()):
            connection.rollback()
            connection.close()
            raise RepairError(f"replay range became occupied; backup: {backup_dir}")
    cursor = connection.execute(
        """
        UPDATE thread_history_projection_state
        SET next_rollout_ordinal = ?
        WHERE thread_id = ?
          AND next_rollout_byte_offset = ?
          AND next_rollout_ordinal = ?
        """,
        (
            candidate.new_ordinal,
            current["thread_id"],
            candidate.old_offset,
            candidate.old_ordinal,
        ),
    )
    if cursor.rowcount != 1:
        connection.rollback()
        connection.close()
        raise RepairError(
            f"guarded update changed {cursor.rowcount} rows instead of 1; backup: {backup_dir}"
        )
    connection.commit()
    connection.close()

    after_quick_check = quick_check_database(history_path)
    if after_quick_check != "ok":
        raise RepairError(
            f"post-repair quick_check failed: {after_quick_check}; backup: {backup_dir}"
        )
    if sha256_file(rollout_path) != source_hash:
        raise RepairError(f"rollout hash changed during repair; backup: {backup_dir}")
    after = build_report(current["thread_id"], codex_home)
    after_counts = after["projection"]
    for field in ("turn_count", "item_count", "realtime_count"):
        if after_counts[field] != before_counts[field]:
            raise RepairError(f"{field} changed before resume; backup: {backup_dir}")

    return {
        "mode": "apply",
        "thread_id": current["thread_id"],
        "status": "cursor_repaired_resume_required",
        "updated_rows": 1,
        "backup_dir": str(backup_dir),
        "before_quick_check": before_quick_check,
        "state_quick_check": state_quick_check,
        "after_quick_check": after_quick_check,
        "rollout_sha256_unchanged": True,
        "old_cursor": [candidate.old_offset, candidate.old_ordinal],
        "new_cursor": [candidate.old_offset, candidate.new_ordinal],
        "turn_count": after_counts["turn_count"],
        "item_count": after_counts["item_count"],
        "realtime_count": after_counts["realtime_count"],
        "next_action": (
            f"run `codex resume {current['thread_id']}` normally, then diagnose again"
        ),
    }


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    try:
        report = build_report(args.thread_id, args.codex_home)
        if not args.apply:
            print(json.dumps(report, indent=2, default=str))
            return 0
        if not args.confirm_closed:
            raise RepairError("--apply requires --confirm-closed")
        backup_root = (
            args.backup_root.expanduser().resolve()
            if args.backup_root
            else args.codex_home.expanduser().resolve() / "backups" / "session-repair"
        )
        result = apply_repair(
            report,
            args.codex_home,
            backup_root,
            args.stability_seconds,
        )
        print(json.dumps(result, indent=2, default=str))
        return 0
    except (OSError, RepairError, sqlite3.Error) as error:
        print(
            json.dumps(
                {
                    "mode": "apply" if args.apply else "diagnose",
                    "thread_id": args.thread_id,
                    "status": "refused",
                    "error": str(error),
                },
                indent=2,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
