from __future__ import annotations

import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import tests.test_env  # noqa: F401

from app.cgm_service import fetch_realtime_points_for_user
from app.providers.registry import resolve_provider_order


class DummyDb:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


class ProviderSelectionTests(unittest.TestCase):
    def test_resolve_provider_order_prefers_medtronic(self):
        user = SimpleNamespace(
            glucose_source_active="medtronic_carelink",
            glucose_provider="medtronic_carelink",
            cgm_source=None,
            libre_credentials=SimpleNamespace(email="libre@example.com"),
            dexcom_tokens=[],
            carelink_credentials=SimpleNamespace(username="carelink@example.com"),
            nightscout_credentials=None,
        )

        order = resolve_provider_order(user)
        self.assertEqual(order, ["medtronic_carelink"])

    def test_unknown_provider_raises(self):
        from app.providers.registry import _fetch_from_provider

        with self.assertRaises(ValueError):
            _fetch_from_provider(None, SimpleNamespace(), "unknown", dt.datetime.now(), dt.datetime.now())

    def test_fetch_realtime_points_uses_medtronic_when_selected(self):
        user = SimpleNamespace(
            id=42,
            glucose_source_active="medtronic_carelink",
            glucose_provider="medtronic_carelink",
            cgm_source="medtronic_carelink",
            libre_credentials=None,
            dexcom_tokens=[],
            carelink_credentials=SimpleNamespace(
                username="carelink@example.com",
                status=None,
                error_message=None,
                last_sync_at=None,
            ),
            nightscout_credentials=None,
        )
        db = DummyDb()
        now = dt.datetime(2026, 7, 9, 8, 5, tzinfo=dt.timezone.utc)

        with patch("app.cgm_service.fetch_carelink_glucose", return_value=[
            {
                "timestamp": now,
                "glucose": 123.0,
                "unit": "mg/dL",
                "trend": "FLAT",
                "source": "medtronic_carelink",
                "raw": {"sg": 123},
            }
        ]), patch("app.cgm_service._reserve_global_call_slot", return_value=None):
            points, source_label, meta = fetch_realtime_points_for_user(db, user, context="test")

        self.assertEqual(source_label, "medtronic_carelink")
        self.assertEqual(len(points), 1)
        self.assertEqual(points[0]["mgdl"], 123.0)
        self.assertIn("medtronic_carelink", meta["attempted_sources"])

    def test_fetch_realtime_points_does_not_fallback_when_active_source_missing(self):
        user = SimpleNamespace(
            id=99,
            glucose_source_active="nightscout",
            glucose_provider="nightscout",
            cgm_source="nightscout",
            libre_credentials=SimpleNamespace(email="libre@example.com"),
            dexcom_tokens=[],
            carelink_credentials=None,
            nightscout_credentials=None,
        )
        db = DummyDb()

        points, source_label, meta = fetch_realtime_points_for_user(db, user, context="test")

        self.assertEqual(points, [])
        self.assertIsNone(source_label)
        self.assertEqual(meta["attempted_sources"], [])


if __name__ == "__main__":
    unittest.main()
