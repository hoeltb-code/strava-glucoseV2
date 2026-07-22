from __future__ import annotations

import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import tests.test_env  # noqa: F401

from app.providers.nightscout import (
    NightscoutError,
    NightscoutNoRecentDataError,
    fetch_nightscout_glucose,
    normalize_base_url,
    normalize_entries,
    test_connection,
)
from app.secrets import encrypt_secret


class NightscoutProviderTests(unittest.TestCase):
    def test_normalize_base_url_strips_trailing_slash(self):
        self.assertEqual(
            normalize_base_url("https://demo.example.com/"),
            "https://demo.example.com",
        )

    def test_normalize_entries_filters_invalid_rows(self):
        start = dt.datetime(2026, 7, 9, 8, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2026, 7, 9, 8, 10, tzinfo=dt.timezone.utc)
        entries = [
            {"sgv": 121, "dateString": "2026-07-09T08:01:00+00:00", "direction": "Flat", "type": "sgv"},
            {"sgv": None, "date": 1_751_849_400_000, "type": "sgv"},
            {"mbg": 110, "date": 1_751_849_500_000, "type": "mbg"},
            {"sgv": 130, "dateString": "2026-07-09T08:06:00+00:00", "type": "sgv"},
            {"sgv": 140, "date": 1_751_849_900_000, "type": "note"},
        ]

        points = normalize_entries(entries, start=start, end=end)

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0]["glucose"], 121.0)
        self.assertEqual(points[1]["trend"], None)

    def test_fetch_nightscout_glucose_raises_when_no_points(self):
        user = SimpleNamespace(
            nightscout_credentials=SimpleNamespace(
                base_url="https://demo.example.com/",
                read_token_encrypted=encrypt_secret("read-token"),
            )
        )
        start = dt.datetime(2026, 7, 9, 8, 0, tzinfo=dt.timezone.utc)
        end = dt.datetime(2026, 7, 9, 8, 10, tzinfo=dt.timezone.utc)

        with patch("app.providers.nightscout._request_entries", return_value=[]):
            with self.assertRaises(NightscoutNoRecentDataError):
                fetch_nightscout_glucose(user, start, end)

    def test_connection_warns_when_nightscout_has_no_recent_data(self):
        user = SimpleNamespace(
            nightscout_credentials=SimpleNamespace(
                base_url="https://demo.example.com",
                read_token_encrypted=None,
            )
        )

        with patch(
            "app.providers.nightscout.fetch_nightscout_glucose",
            side_effect=NightscoutNoRecentDataError("Connexion établie, aucune donnée récente."),
        ):
            result = test_connection(user)

        self.assertTrue(result.ok)
        self.assertEqual(result.status, "warn")
        self.assertIn("aucune donnée récente", result.message)

    def test_test_connection_formats_latest_value(self):
        user = SimpleNamespace(
            nightscout_credentials=SimpleNamespace(
                base_url="https://demo.example.com",
                read_token_encrypted=None,
            )
        )
        points = [
            {
                "timestamp": dt.datetime(2026, 7, 9, 8, 5, tzinfo=dt.timezone.utc),
                "glucose": 143.0,
                "trend": "Flat",
                "source": "nightscout",
                "raw": {},
            }
        ]

        with patch("app.providers.nightscout.fetch_nightscout_glucose", return_value=points):
            result = test_connection(user)

        self.assertTrue(result.ok)
        self.assertEqual(result.status, "ok")
        self.assertIn("143 mg/dL", result.message)


if __name__ == "__main__":
    unittest.main()
