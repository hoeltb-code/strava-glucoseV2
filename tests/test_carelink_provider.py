from __future__ import annotations

import datetime as dt
import json
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import tests.test_env  # noqa: F401

from app.providers.medtronic_carelink import (
    CareLinkNeedsReauthError,
    fetch_glucose,
    normalize_carelink_payload,
    test_connection,
)
from app.secrets import encrypt_secret


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "carelink_sample.json"


class CareLinkProviderTests(unittest.TestCase):
    def test_normalize_carelink_payload(self):
        raw = json.loads(FIXTURE_PATH.read_text())
        points = normalize_carelink_payload(raw)

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0]["glucose"], 112.0)
        self.assertEqual(points[0]["unit"], "mg/dL")
        self.assertEqual(points[0]["source"], "medtronic_carelink")
        self.assertEqual(points[1]["trend"], "UP")

    def test_fetch_glucose_returns_common_points(self):
        raw = json.loads(FIXTURE_PATH.read_text())
        user = SimpleNamespace(
            carelink_credentials=SimpleNamespace(
                username="carelink@example.com",
                access_token=encrypt_secret("header.payload.sig"),
                refresh_token=encrypt_secret("refresh"),
                client_id="client-id",
                client_secret=encrypt_secret("client-secret"),
                mag_identifier="mag-id",
                scope="openid",
                region="EU",
                patient_id=None,
                token_expires_at=None,
            )
        )
        start = dt.datetime(2026, 7, 9, 7, 55, tzinfo=dt.timezone.utc)
        end = dt.datetime(2026, 7, 9, 8, 6, tzinfo=dt.timezone.utc)

        with patch("app.providers.medtronic_carelink._load_config", return_value={"baseUrlCareLink": "https://care", "baseUrlCumulus": "https://cumulus"}), \
             patch("app.providers.medtronic_carelink._load_user_profile", return_value={"username": "carelink@example.com", "role": "PATIENT"}), \
             patch("app.providers.medtronic_carelink._fetch_recent_payload", return_value=raw):
            points = fetch_glucose(user, start, end)

        self.assertEqual(len(points), 2)
        self.assertEqual(points[0]["source"], "medtronic_carelink")
        self.assertTrue(all("timestamp" in point for point in points))

    def test_test_connection_handles_invalid_credentials(self):
        user = SimpleNamespace(
            carelink_credentials=SimpleNamespace(username="carelink@example.com")
        )

        with patch(
            "app.providers.medtronic_carelink.fetch_glucose",
            side_effect=CareLinkNeedsReauthError("Session CareLink expirée ou invalide."),
        ):
            result = test_connection(user)

        self.assertFalse(result.ok)
        self.assertEqual(result.status, "needs_reauth")
        self.assertIn("CareLink", result.message)


if __name__ == "__main__":
    unittest.main()
