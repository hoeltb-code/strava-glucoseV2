from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import clubs


class ClubsTests(unittest.TestCase):
    def test_load_club_catalog_filters_invalid_and_duplicate_entries(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            clubs_path = Path(tmp_dir) / "clubs.json"
            clubs_path.write_text(
                json.dumps(
                    [
                        {"slug": "asptt-annecy", "name": "ASPTT Annecy", "logo": "asptt.png", "active": True},
                        {"slug": "asptt-annecy", "name": "Doublon", "logo": "other.png", "active": True},
                        {"slug": "", "name": "Sans slug"},
                        {"slug": "inactive-club", "name": "Inactive Club", "active": False},
                    ]
                ),
                encoding="utf-8",
            )
            with patch.object(clubs, "CLUBS_JSON_PATH", clubs_path):
                catalog = clubs.load_club_catalog()
                available = clubs.get_available_clubs()

        self.assertEqual(len(catalog), 2)
        self.assertEqual(catalog[0]["slug"], "asptt-annecy")
        self.assertEqual(catalog[0]["logo"], "asptt.png")
        self.assertEqual(len(available), 1)
        self.assertEqual(available[0]["slug"], "asptt-annecy")

    def test_build_club_payload_resolves_logo_url_when_file_exists(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            clubs_path = root / "clubs.json"
            logos_dir = root / "club_logos"
            logos_dir.mkdir()
            (logos_dir / "club-logo.png").write_bytes(b"png")
            clubs_path.write_text(
                json.dumps(
                    [
                        {
                            "slug": "team-glucose",
                            "name": "Team Glucose",
                            "logo": "club-logo.png",
                            "active": True,
                        }
                    ]
                ),
                encoding="utf-8",
            )
            with patch.object(clubs, "CLUBS_JSON_PATH", clubs_path), patch.object(clubs, "CLUB_LOGOS_DIR", logos_dir):
                payload = clubs.build_club_payload("team-glucose")

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["name"], "Team Glucose")
        self.assertEqual(payload["logo_url"], "/static/club_logos/club-logo.png")
        self.assertFalse(payload["missing"])

    def test_build_club_payload_marks_missing_slug(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            clubs_path = Path(tmp_dir) / "clubs.json"
            clubs_path.write_text("[]", encoding="utf-8")
            with patch.object(clubs, "CLUBS_JSON_PATH", clubs_path):
                payload = clubs.build_club_payload("unknown-club")

        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(payload["slug"], "unknown-club")
        self.assertTrue(payload["missing"])
        self.assertIsNone(payload["logo_url"])


if __name__ == "__main__":
    unittest.main()
