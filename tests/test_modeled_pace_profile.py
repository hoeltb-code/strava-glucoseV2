import unittest

from app.main import (
    _build_modeled_pace_lookup_from_profile,
    _smooth_slope_pace_values,
)


class ModeledPaceProfileTests(unittest.TestCase):
    def test_modeled_lookup_uses_same_local_smoothing_as_runner_profile(self):
        raw = {
            "Sneg5_10": 360.0,
            "Sneg0_5": 330.0,
            "S0_5": 390.0,
            "S5_10": 540.0,
        }
        profile = {
            "zones": {
                "Zone 3": {
                    slope_id: {"avg_pace_s_per_km": pace}
                    for slope_id, pace in raw.items()
                }
            }
        }

        expected = _smooth_slope_pace_values(raw)
        lookup = _build_modeled_pace_lookup_from_profile(profile)

        self.assertEqual(set(lookup), set(expected))
        for slope_id, pace in expected.items():
            self.assertAlmostEqual(lookup[slope_id]["Zone 3"], pace)

    def test_flat_and_extreme_slopes_remain_strongly_anchored_to_observation(self):
        modeled = _smooth_slope_pace_values({
            "Sneg40p": 1800.0,
            "Sneg30_40": 1200.0,
            "Sneg25_30": 600.0,
            "Sneg0_5": 330.0,
            "S0_5": 390.0,
            "S5_10": 540.0,
        })

        self.assertGreater(modeled["Sneg40p"], 1700.0)
        self.assertLess(abs(modeled["Sneg0_5"] - 330.0), abs(390.0 - 330.0))
        self.assertLess(abs(modeled["S0_5"] - 390.0), abs(540.0 - 390.0))


if __name__ == "__main__":
    unittest.main()
