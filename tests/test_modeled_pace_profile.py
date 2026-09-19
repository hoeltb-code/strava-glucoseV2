import unittest

from app.main import (
    _build_modeled_pace_lookup_from_profile,
    _smooth_slope_pace_values,
    SLOPE_ORDER,
    HR_ZONES,
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

        self.assertEqual(set(lookup), {slope_id for slope_id, _ in SLOPE_ORDER})
        for slope_id, pace in expected.items():
            self.assertAlmostEqual(lookup[slope_id]["Zone 3"], pace)

    def test_missing_cells_are_estimated_from_the_model_and_anchors_are_preserved(self):
        raw = {"S0_5": 360.0, "S5_10": 720.0, "S10_15": 450.0}
        lookup = _build_modeled_pace_lookup_from_profile({"zones": {
            "Zone 2": {key: {"avg_pace_s_per_km": pace} for key, pace in raw.items()}
        }})
        smoothed = _smooth_slope_pace_values(raw)
        self.assertNotEqual(smoothed["S5_10"], raw["S5_10"])
        for key, pace in smoothed.items():
            self.assertAlmostEqual(lookup[key]["Zone 2"], pace)
            self.assertAlmostEqual(lookup[key]["Zone 3"], pace * 0.93)
        for slope_id, _ in SLOPE_ORDER:
            for zone, *_ in HR_ZONES:
                self.assertGreater(lookup[slope_id][zone], 0)

    def test_empty_or_invalid_profile_does_not_invent_a_projection(self):
        self.assertEqual(_build_modeled_pace_lookup_from_profile(None), {})
        self.assertEqual(_build_modeled_pace_lookup_from_profile({"zones": {
            "Zone 2": {"S0_5": {"avg_pace_s_per_km": float("nan")}}
        }}), {})

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
