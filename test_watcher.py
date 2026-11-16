#!/usr/bin/env python3
"""Unit tests for unlocked-sample filtering, queue-release, and SPS schedule filter.

No network. Run: python3 test_watcher.py
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Keep the module off the real state directory before import.
TMP = Path(tempfile.mkdtemp(prefix="watcher-test-"))
os.environ["WATCHER_STATE_DIR"] = str(TMP)
os.environ.pop("CALEB_WATCHER_WEBHOOK_URL", None)
os.environ.pop("CALEB_WATCHER_WEBHOOK_KEY", None)

import watcher as W  # noqa: E402

UTC = timezone.utc


def ts(s: str) -> str:
    return s if s.endswith("Z") else s + "Z"


def contact(sc, dish, active, power, rate=0.0, band="X", station="Canberra"):
    return {
        "spacecraft": sc,
        "dish": dish,
        "station": station,
        "signal_active": active,
        "signals": [
            {
                "direction": "down",
                "active": active,
                "signal_type": "data" if active else "none",
                "data_rate_bps": rate,
                "power_dbm": power,
                "band": band,
            }
        ],
    }


def rec(time, contacts, ok=True):
    return {"ok": ok, "ts_utc": time, "feed_ts_utc": time, "contacts": contacts, "anomalous_or_offline": []}


def ev(name, detail, time):
    return W.make_event(name, detail, time)


class TestUnlockedSampleFilter(unittest.TestCase):
    def test_is_locked(self):
        self.assertFalse(W.is_locked(False, -470))
        self.assertFalse(W.is_locked(False, -160))
        self.assertFalse(W.is_locked(True, -480))
        self.assertTrue(W.is_locked(True, -160))
        self.assertTrue(W.is_locked(True, -170))

    def test_build_sessions_drops_unlocked_and_keeps_locks(self):
        rows = [
            rec("2026-11-16T03:45:49Z", [contact("VGR1", "DSS-43", False, -480, 0)]),
            rec("2026-11-16T05:12:50Z", [contact("VGR1", "DSS-43", True, -160, 40)]),
            rec("2026-11-16T05:20:00Z", [contact("VGR1", "DSS-43", True, -160, 40)]),
            rec("2026-11-16T09:29:59Z", [contact("VGR1", "DSS-63", False, -470, 0)]),
            rec("2026-11-16T10:57:46Z", [contact("VGR1", "DSS-63", False, -160, 0)]),
        ]
        sessions = W.build_sessions(rows)
        self.assertEqual(len(sessions), 1)
        sess = sessions[0]
        self.assertEqual(sess["spacecraft"], "VGR1")
        self.assertEqual(sess["dish"], "DSS-43")
        self.assertEqual(sess["samples"], 2)
        self.assertEqual(sess["start"], datetime(2026, 11, 16, 5, 12, 50, tzinfo=UTC))
        self.assertEqual(sess["end"], datetime(2026, 11, 16, 5, 20, 0, tzinfo=UTC))
        self.assertEqual(sess["rates"], [40, 40])

    def test_build_sessions_empty_when_only_unlocked(self):
        rows = [rec("2026-11-16T09:29:59Z", [contact("VGR1", "DSS-63", False, -470, 0)])]
        self.assertEqual(W.build_sessions(rows), [])


class TestQueueRelease(unittest.TestCase):
    def test_event_spacecraft(self):
        self.assertEqual(W.event_spacecraft("VGR2 signal lost after a gap (was on DSS-34)."), "VGR2")
        self.assertEqual(W.event_spacecraft("VGR1 signal acquired after a gap on DSS-43."), "VGR1")
        self.assertIsNone(W.event_spacecraft("PHNL crosswind component of 15 kt"))

    def test_drops_queued_lost_when_later_acquired(self):
        state = {
            "queue": {
                "event": "voyager_lost",
                "detail": "VGR2 signal lost after a gap (was on DSS-34). No dish is tracking it in the latest DSN snapshot.",
                "time_utc": "2026-11-14T14:22:00Z",
            }
        }
        events = [
            ev(
                "voyager_acquired",
                "VGR2 signal acquired after a gap on DSS-34 (Canberra, X 160 bps, -160 dBm).",
                "2026-11-14T18:00:00Z",
            )
        ]
        W.release_superseded_queue(state, events)
        self.assertIsNone(state["queue"])

    def test_keeps_queued_lost_when_other_spacecraft_acquired(self):
        state = {
            "queue": {
                "event": "voyager_lost",
                "detail": "VGR2 signal lost after a gap (was on DSS-34). No dish is tracking it.",
                "time_utc": "2026-11-14T14:22:00Z",
            }
        }
        events = [
            ev(
                "voyager_acquired",
                "VGR1 signal acquired after a gap on DSS-43 (Canberra, X 40 bps, -160 dBm).",
                "2026-11-14T18:00:00Z",
            )
        ]
        W.release_superseded_queue(state, events)
        self.assertIsNotNone(state["queue"])
        self.assertEqual(state["queue"]["event"], "voyager_lost")

    def test_drops_queued_acquired_when_later_lost(self):
        state = {
            "queue": {
                "event": "voyager_acquired",
                "detail": "VGR1 signal acquired after a gap on DSS-43.",
                "time_utc": "2026-11-16T05:00:00Z",
            }
        }
        events = [ev("voyager_lost", "VGR1 signal lost after a gap (was on DSS-43).", "2026-11-16T08:10:00Z")]
        W.release_superseded_queue(state, events)
        self.assertIsNone(state["queue"])

    def test_ignores_non_lock_queue(self):
        state = {
            "queue": {
                "event": "dsn_anomaly",
                "detail": "DSN anomaly on DSS-14 (Goldstone): flagged.",
                "time_utc": "2026-11-16T05:00:00Z",
            }
        }
        events = [ev("voyager_acquired", "VGR1 signal acquired after a gap on DSS-43.", "2026-11-16T05:10:00Z")]
        W.release_superseded_queue(state, events)
        self.assertEqual(state["queue"]["event"], "dsn_anomaly")


class TestSpsScheduleFilter(unittest.TestCase):
    def setUp(self):
        # VGR1 SCIENCE on DSS-43: setup 03:35, BOT 04:00, EOT 08:00, teardown 08:15 UTC, 2026-320 (Nov 16).
        self.passes = W.normalize_sps_passes(
            [
                {
                    "projuser": "VGR1",
                    "facility": 43,
                    "activity": "SCIENCE         ",
                    "starttime": "2026-320T03:35:00",
                    "bot": "2026-320T04:00:00",
                    "eot": "2026-320T08:00:00",
                    "endtime": "2026-320T08:15:00",
                }
            ]
        )
        self.assertEqual(len(self.passes), 1)
        self.assertEqual(self.passes[0]["dish"], "DSS-43")
        self.assertEqual(self.passes[0]["bot"], datetime(2026, 11, 16, 4, 0, 0, tzinfo=UTC))

    def test_parse_sps_time(self):
        self.assertEqual(W.parse_sps_time("2026-320T04:00:00"), datetime(2026, 11, 16, 4, 0, 0, tzinfo=UTC))
        self.assertEqual(W.parse_sps_time("2026-001T00:00:00"), datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC))
        self.assertIsNone(W.parse_sps_time(""))
        self.assertIsNone(W.parse_sps_time("2026-400T00:00:00"))

    def test_acquire_on_bot_is_scheduled(self):
        ev_ = ev("voyager_acquired", "VGR1 signal acquired after a gap on DSS-43 (Canberra, X 40 bps, -160 dBm).", "2026-11-16T04:05:00Z")
        reason = W.scheduled_lock_reason(ev_, self.passes)
        self.assertIsNotNone(reason)
        self.assertIn("not a surprise acquire", reason)

    def test_acquire_during_setup_is_scheduled(self):
        ev_ = ev("voyager_acquired", "VGR1 signal acquired after a gap on DSS-43.", "2026-11-16T03:40:00Z")
        self.assertIsNotNone(W.scheduled_lock_reason(ev_, self.passes))

    def test_lost_at_eot_is_scheduled(self):
        ev_ = ev("voyager_lost", "VGR1 signal lost after a gap (was on DSS-43). No dish is tracking it.", "2026-11-16T08:02:00Z")
        reason = W.scheduled_lock_reason(ev_, self.passes)
        self.assertIsNotNone(reason)
        self.assertIn("not a surprise loss", reason)

    def test_off_schedule_acquire_is_not_suppressed(self):
        ev_ = ev("voyager_acquired", "VGR1 signal acquired after a gap on DSS-63.", "2026-11-16T12:00:00Z")
        self.assertIsNone(W.scheduled_lock_reason(ev_, self.passes))

    def test_other_spacecraft_not_suppressed(self):
        ev_ = ev("voyager_acquired", "VGR2 signal acquired after a gap on DSS-34.", "2026-11-16T04:05:00Z")
        self.assertIsNone(W.scheduled_lock_reason(ev_, self.passes))

    def test_filter_drops_scheduled_keeps_off_schedule(self):
        scheduled = ev("voyager_acquired", "VGR1 signal acquired after a gap on DSS-43.", "2026-11-16T04:05:00Z")
        surprise = ev("voyager_lost", "VGR2 signal lost after a gap (was on DSS-34).", "2026-11-16T04:05:00Z")
        wind = ev("phnl_crosswind", "PHNL crosswind component of 15 kt or more on 26L/08R.", "2026-11-16T04:05:00Z")
        # Bypass the network by patching load_sps_passes.
        orig = W.load_sps_passes
        W.load_sps_passes = lambda now=None, missions=("VGR1", "VGR2"): self.passes
        try:
            kept = W.filter_scheduled_lock_events([scheduled, surprise, wind], datetime(2026, 11, 16, 4, 10, tzinfo=UTC))
        finally:
            W.load_sps_passes = orig
        names_details = [(e["event"], e["detail"][:4]) for e in kept]
        self.assertEqual([e["event"] for e in kept], ["voyager_lost", "phnl_crosswind"])
        self.assertTrue(any(e["detail"].startswith("VGR2") for e in kept))
        self.assertFalse(any(e["event"] == "voyager_acquired" for e in kept))

    def test_filter_fails_open_when_schedule_unavailable(self):
        ev_ = ev("voyager_acquired", "VGR1 signal acquired after a gap on DSS-43.", "2026-11-16T04:05:00Z")
        orig = W.load_sps_passes
        W.load_sps_passes = lambda now=None, missions=("VGR1", "VGR2"): None
        try:
            kept = W.filter_scheduled_lock_events([ev_], datetime(2026, 11, 16, 4, 10, tzinfo=UTC))
        finally:
            W.load_sps_passes = orig
        self.assertEqual(kept, [ev_])


if __name__ == "__main__":
    unittest.main()
