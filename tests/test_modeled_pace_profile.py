import unittest

from app.main import (
    _build_modeled_pace_lookup_from_profile,
    _smooth_slope_pace_values,
    SLOPE_ORDER,
    HR_ZONES,
)


class ModeledPaceProfileTests(unittest.TestCase):
    def test_modeled_lookup_uses_the_global_trend(self):
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

    def test_missing_cells_are_estimated_from_fitted_values(self):
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

    def test_unusual_flat_and_extreme_observations_can_move_to_the_trend(self):
        raw = {"Sneg40p": 400., "Sneg30_40": 470., "Sneg25_30": 480.,
               "Sneg10_15": 320., "Sneg0_5": 275., "S0_5": 300.,
               "S5_10": 650., "S10_15": 560., "S40p": 1160.}
        modeled = _smooth_slope_pace_values(raw)
        self.assertGreater(abs(modeled['S5_10'] - raw['S5_10']), 100)
        self.assertGreater(modeled['Sneg40p'], modeled['Sneg30_40'])
        self.assertLess(modeled['S0_5'], modeled['S5_10'])


if __name__ == "__main__":
    unittest.main()
