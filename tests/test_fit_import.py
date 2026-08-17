from __future__ import annotations

import datetime as dt
import unittest
from unittest.mock import patch

import tests.test_env  # noqa: F401

from app.main import parse_fit_to_act_and_streams


class FakeRecord:
    def __init__(self, name, values):
        self.name = name
        self.values = values

    def get_value(self, key):
        return self.values.get(key)

    def __iter__(self):
        return iter([])


class FakeSession:
    name = "session"

    def __iter__(self):
        return iter([])


class FakeFitFile:
    def __init__(self, _filepath):
        start = dt.datetime(2026, 1, 2, 9, 0, 0, tzinfo=dt.timezone.utc)
        self.messages = [
            FakeSession(),
            FakeRecord(
                "record",
                {
                    "timestamp": start,
                    "position_lat": 536870912,
                    "position_long": 1073741824,
                    "altitude": 100,
                    "distance": 0,
                    "heart_rate": 130,
                    "cadence": 80,
                    "speed": 2.5,
                },
            ),
            FakeRecord(
                "record",
                {
                    "timestamp": start + dt.timedelta(seconds=60),
                    "position_lat": 536871000,
                    "position_long": 1073741900,
                    "altitude": 110,
                    "distance": 150,
                    "heart_rate": 140,
                    "cadence": 82,
                    "speed": 2.7,
                },
            ),
        ]

    def get_messages(self):
        return self.messages


class FitImportTests(unittest.TestCase):
    def test_fit_records_are_converted_to_activity_streams(self):
        with patch("app.main.FitFile", FakeFitFile), patch("builtins.open", unittest.mock.mock_open(read_data=b"fit")):
            activity, streams = parse_fit_to_act_and_streams("activity.fit", user_id=7)

        self.assertEqual(activity["athlete"]["id"], 7)
        self.assertEqual(activity["elapsed_time"], 60)
        self.assertEqual(activity["distance"], 150.0)
        self.assertEqual(activity["total_elevation_gain"], 10.0)
        self.assertEqual(streams["time"]["data"], [0, 60])
        self.assertEqual(streams["distance"]["data"], [0.0, 150.0])
        self.assertAlmostEqual(streams["latlng"]["data"][0][0], 45.0)
        self.assertEqual(streams["heartrate"]["data"], [130.0, 140.0])


if __name__ == "__main__":
    unittest.main()
