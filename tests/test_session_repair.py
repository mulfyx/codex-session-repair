"""Exercise safe diagnosis and cursor-only repair on synthetic Codex homes."""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

THREAD_ID = "01a11111-2222-7333-8444-555555555555"
OTHER_THREAD_ID = "01a99999-8888-7777-8666-555555555555"
MODULE_PATH = Path(__file__).parents[1] / "scripts" / "session_repair.py"
SPEC = importlib.util.spec_from_file_location("session_repair", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("could not load session_repair.py")
session_repair = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = session_repair
SPEC.loader.exec_module(session_repair)


def record(timestamp: str, ordinal: int, record_type: str, payload: dict) -> dict:
    return {
        "timestamp": timestamp,
        "ordinal": ordinal,
        "type": record_type,
        "payload": payload,
    }


def token_count(timestamp: str, ordinal: int) -> dict:
    return record(
        timestamp,
        ordinal,
        "event_msg",
        {
            "type": "token_count",
            "rate_limits": {"primary": {"used_percent": 53.0}},
        },
    )


def base_records() -> list[dict]:
    return [
        record(
            "2026-08-30T00:00:00Z",
            0,
            "session_meta",
            {
                "id": THREAD_ID,
                "session_id": THREAD_ID,
                "history_mode": "paginated",
                "cli_version": "0.151.0",
            },
        ),
        record(
            "2026-08-30T00:00:01Z",
            1,
            "event_msg",
            {"type": "task_started", "turn_id": "old-turn"},
        ),
    ]


def double_rewind_records() -> list[dict]:
    return [
        *base_records(),
        token_count("2026-08-30T00:00:02Z", 2),
        token_count("2026-08-30T00:00:03Z", 3),
        record(
            "2026-08-30T00:00:04Z",
            2,
            "event_msg",
            {"type": "thread_settings_applied"},
        ),
        record(
            "2026-08-30T00:00:05Z",
            3,
            "event_msg",
            {"type": "task_started", "turn_id": "new-turn"},
        ),
        record(
            "2026-08-30T00:00:06Z",
            4,
            "response_item",
            {"type": "message", "role": "user", "content": []},
        ),
        record(
            "2026-08-30T00:00:07Z",
            5,
            "event_msg",
            {"type": "task_complete", "turn_id": "new-turn"},
        ),
    ]


def repeated_known_group_records() -> list[dict]:
    return [
        *base_records(),
        token_count("2026-08-30T00:00:02Z", 2),
        record(
            "2026-08-30T00:00:03Z",
            2,
            "event_msg",
            {"type": "thread_settings_applied"},
        ),
        record(
            "2026-08-30T00:00:04Z",
            3,
            "response_item",
            {"type": "reasoning"},
        ),
        token_count("2026-08-30T00:00:05Z", 4),
        record(
            "2026-08-30T00:00:06Z",
            4,
            "event_msg",
            {"type": "task_started", "turn_id": "later-turn"},
        ),
        record(
            "2026-08-30T00:00:07Z",
            5,
            "response_item",
            {"type": "message", "role": "user", "content": []},
        ),
    ]


def settings_duplicate_records() -> list[dict]:
    return [
        *base_records(),
        token_count("2026-08-30T00:00:02Z", 2),
        token_count("2026-08-30T00:00:03Z", 3),
        record(
            "2026-08-30T00:00:04Z",
            3,
            "event_msg",
            {"type": "thread_settings_applied"},
        ),
        record(
            "2026-08-30T00:00:05Z",
            4,
            "event_msg",
            {"type": "task_started", "turn_id": "new-turn"},
        ),
        record(
            "2026-08-30T00:00:06Z",
            5,
            "response_item",
            {"type": "message", "role": "user", "content": []},
        ),
    ]


def two_unprojected_groups_records() -> list[dict]:
    return [
        *double_rewind_records(),
        token_count("2026-08-30T00:00:08Z", 6),
        record(
            "2026-08-30T00:00:09Z",
            6,
            "event_msg",
            {"type": "task_started", "turn_id": "another-turn"},
        ),
        record(
            "2026-08-30T00:00:10Z",
            7,
            "response_item",
            {"type": "message", "role": "user", "content": []},
        ),
    ]


class Fixture:
    """Create one minimal paginated Codex home for a behavioral test."""

    def __init__(
        self,
        root: Path,
        records: list[dict],
        cursor_line: int,
        expected: int,
    ) -> None:
        """Materialize the requested rollout and derived-state cursor."""
        self.home = root / ".codex"
        lock_dir = self.home / "thread-writer-locks"
        lock_dir.mkdir(parents=True)
        (lock_dir / ".coordination.lock").touch()
        self.writer_lock = lock_dir / f"{THREAD_ID}.lock"
        self.writer_lock.touch()
        rollout_dir = self.home / "sessions" / "2026" / "08" / "30"
        rollout_dir.mkdir(parents=True)
        self.rollout = rollout_dir / f"rollout-2026-08-30T00-00-00-{THREAD_ID}.jsonl"
        offsets: list[int] = []
        with self.rollout.open("wb") as handle:
            for value in records:
                offsets.append(handle.tell())
                handle.write(json.dumps(value, separators=(",", ":")).encode() + b"\n")
        self.cursor = offsets[cursor_line]

        state = sqlite3.connect(self.home / "state_5.sqlite")
        state.execute("PRAGMA journal_mode = WAL")
        state.execute(
            """
            CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                rollout_path TEXT NOT NULL,
                history_mode TEXT NOT NULL,
                cli_version TEXT NOT NULL
            )
            """
        )
        state.execute(
            "INSERT INTO threads VALUES (?, ?, 'paginated', '0.151.0')",
            (THREAD_ID, str(self.rollout)),
        )
        state.commit()
        state.close()

        history = sqlite3.connect(self.home / "thread_history_1.sqlite")
        history.execute("PRAGMA journal_mode = WAL")
        history.executescript(
            """
            CREATE TABLE thread_history_projection_state (
                thread_id TEXT PRIMARY KEY,
                next_rollout_byte_offset INTEGER NOT NULL,
                next_rollout_ordinal INTEGER NOT NULL
            );
            CREATE TABLE thread_turns (
                thread_id TEXT NOT NULL,
                turn_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL,
                rollout_end_ordinal INTEGER
            );
            CREATE TABLE thread_items (
                thread_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL,
                updated_at_ordinal INTEGER NOT NULL
            );
            CREATE TABLE thread_realtime_items (
                thread_id TEXT NOT NULL,
                rollout_ordinal INTEGER NOT NULL
            );
            """
        )
        history.execute(
            "INSERT INTO thread_history_projection_state VALUES (?, ?, ?)",
            (THREAD_ID, self.cursor, expected),
        )
        history.execute(
            "INSERT INTO thread_turns VALUES (?, 'old-turn', 1, NULL)",
            (THREAD_ID,),
        )
        history.commit()
        history.close()


class SessionRepairTests(unittest.TestCase):
    """Verify supported candidates, refusal gates, backup, and apply behavior."""

    def closed_profile(self) -> mock._patch:
        """Replace the OS handle probe with a known-closed profile."""
        return mock.patch.object(
            session_repair,
            "check_paths_open",
            return_value={
                "status": "closed",
                "reason": None,
                "process_count": 0,
                "checked_path_count": 3,
            },
        )

    def test_double_rewind_candidate(self) -> None:
        """Replay both settings and task start for the validated double rewind."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertEqual(report["status"], "repairable_duplicate_ordinal_projection")
            self.assertEqual(
                report["candidate"]["kind"],
                "replay_settings_and_duplicate_task_start",
            )
            self.assertEqual(report["candidate"]["new_ordinal"], 2)

    def test_settings_duplicate_replays_without_moving_byte_cursor(self) -> None:
        """Keep the byte cursor when replaying duplicated settings metadata."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), settings_duplicate_records(), 4, 4)
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertEqual(
                report["candidate"]["kind"],
                "replay_duplicate_thread_settings",
            )
            self.assertEqual(report["candidate"]["new_ordinal"], 3)

    def test_apply_changes_only_cursor_and_creates_backup(self) -> None:
        """Change one cursor field while preserving raw bytes and making a backup."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            before_hash = hashlib.sha256(fixture.rollout.read_bytes()).hexdigest()
            backup_root = Path(directory) / "backups"
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
                result = session_repair.apply_repair(
                    report,
                    fixture.home,
                    backup_root,
                    0,
                )
            self.assertEqual(result["updated_rows"], 1)
            history = sqlite3.connect(fixture.home / "thread_history_1.sqlite")
            cursor = history.execute(
                "SELECT next_rollout_byte_offset, next_rollout_ordinal "
                "FROM thread_history_projection_state WHERE thread_id = ?",
                (THREAD_ID,),
            ).fetchone()
            history.close()
            self.assertEqual(cursor, (fixture.cursor, 2))
            self.assertEqual(hashlib.sha256(fixture.rollout.read_bytes()).hexdigest(), before_hash)
            backup_dir = Path(result["backup_dir"])
            self.assertTrue((backup_dir / "rollout.jsonl").is_file())
            self.assertTrue((backup_dir / "target-rows.sqlite").is_file())
            self.assertTrue((backup_dir / "manifest.json").is_file())
            with self.closed_profile():
                after = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertEqual(after["status"], "aligned_but_not_materialized")
            self.assertIsNone(after["candidate"])

    def test_unrelated_sqlite_writer_may_finish_before_target_update(self) -> None:
        """Serialize behind an unrelated WAL writer and still update only the target row."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            history_path = fixture.home / "thread_history_1.sqlite"
            history = sqlite3.connect(history_path)
            history.execute(
                "INSERT INTO thread_history_projection_state VALUES (?, 900, 9)",
                (OTHER_THREAD_ID,),
            )
            history.commit()
            history.close()

            writer_ready = threading.Event()

            def write_unrelated_row() -> None:
                connection = sqlite3.connect(history_path, timeout=5)
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "UPDATE thread_history_projection_state "
                    "SET next_rollout_ordinal = 10 WHERE thread_id = ?",
                    (OTHER_THREAD_ID,),
                )
                writer_ready.set()
                time.sleep(1)
                connection.commit()
                connection.close()

            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
                writer = threading.Thread(target=write_unrelated_row)
                writer.start()
                self.assertTrue(writer_ready.wait(timeout=2))
                result = session_repair.apply_repair(
                    report,
                    fixture.home,
                    Path(directory) / "backups",
                    0,
                )
                writer.join(timeout=2)

            self.assertFalse(writer.is_alive())
            self.assertEqual(result["updated_rows"], 1)
            history = sqlite3.connect(history_path)
            target_cursor = history.execute(
                "SELECT next_rollout_ordinal FROM thread_history_projection_state "
                "WHERE thread_id = ?",
                (THREAD_ID,),
            ).fetchone()[0]
            other_cursor = history.execute(
                "SELECT next_rollout_ordinal FROM thread_history_projection_state "
                "WHERE thread_id = ?",
                (OTHER_THREAD_ID,),
            ).fetchone()[0]
            history.close()
            self.assertEqual(target_cursor, 2)
            self.assertEqual(other_cursor, 10)

    def test_known_earlier_group_does_not_block_later_candidate(self) -> None:
        """Allow a repaired earlier group before one actionable cursor anomaly."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), repeated_known_group_records(), 6, 5)
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertEqual(report["rollout"]["anomaly_count"], 2)
            self.assertEqual(report["rollout"]["unknown_anomalies_before_cursor"], 0)
            self.assertEqual(report["candidate"]["kind"], "replay_duplicate_task_start")
            self.assertEqual(report["candidate"]["new_ordinal"], 4)

    def test_open_target_rollout_blocks_apply_guidance(self) -> None:
        """Mark a repair candidate busy when the target rollout is open."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            with mock.patch.object(
                session_repair,
                "check_paths_open",
                return_value={"status": "open", "reason": None, "process_count": 1},
            ):
                report = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertEqual(report["status"], "busy")
            self.assertIn("target_rollout_open", report["blockers"])

    def test_busy_target_writer_lock_blocks_candidate(self) -> None:
        """Treat Codex's per-thread writer lock as the authoritative busy guard."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            with fixture.writer_lock.open("rb") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.closed_profile():
                    report = session_repair.build_report(THREAD_ID, fixture.home)
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            self.assertEqual(report["status"], "busy")
            self.assertIn("target_writer_lock_busy", report["blockers"])

    def test_missing_target_lock_is_created_only_during_apply(self) -> None:
        """Create a cleanly removed target lock under coordination during apply."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            fixture.writer_lock.unlink()
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
                self.assertEqual(report["writer_lock"]["status"], "available_missing")
                self.assertFalse(fixture.writer_lock.exists())
                result = session_repair.apply_repair(
                    report,
                    fixture.home,
                    Path(directory) / "backups",
                    0,
                )
            self.assertEqual(result["updated_rows"], 1)
            self.assertTrue(fixture.writer_lock.is_file())

    def test_coordination_lock_blocks_missing_lock_creation(self) -> None:
        """Refuse before backup when Codex owns writer-lock coordination."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            fixture.writer_lock.unlink()
            coordination = fixture.home / "thread-writer-locks" / ".coordination.lock"
            with coordination.open("rb") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.closed_profile():
                    report = session_repair.build_report(THREAD_ID, fixture.home)
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            self.assertEqual(report["status"], "busy")
            self.assertIn("target_writer_lock_busy", report["blockers"])
            self.assertFalse(fixture.writer_lock.exists())

    def test_realtime_rows_refuse_candidate(self) -> None:
        """Refuse cursor replay when realtime history exists for the thread."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), double_rewind_records(), 4, 4)
            history = sqlite3.connect(fixture.home / "thread_history_1.sqlite")
            history.execute(
                "INSERT INTO thread_realtime_items VALUES (?, 1)",
                (THREAD_ID,),
            )
            history.commit()
            history.close()
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertIsNone(report["candidate"])
            self.assertIn("target_has_realtime_history_items", report["blockers"])

    def test_multiple_unprojected_groups_refuse_candidate(self) -> None:
        """Refuse a single repair when multiple suffix anomalies remain."""
        with tempfile.TemporaryDirectory() as directory:
            fixture = Fixture(Path(directory), two_unprojected_groups_records(), 4, 4)
            with self.closed_profile():
                report = session_repair.build_report(THREAD_ID, fixture.home)
            self.assertIsNone(report["candidate"])
            self.assertIn(
                "repair_requires_one_actionable_anomaly_at_cursor",
                report["blockers"],
            )


if __name__ == "__main__":
    unittest.main()
