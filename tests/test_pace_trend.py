import math
import unittest
from app.pace_trend import fit, evaluate
from app.pace_reference import reference_model


GRADES = [-45, -35, -27.5, -22.5, -17.5, -12.5, -7.5, -2.5, 2.5, 7.5, 12.5, 17.5, 22.5, 27.5, 35, 45]
KNOWN = dict(version='asymmetric-quadratic-v1', minimum=300., left=400., right=260., optimum=-12., x_min=-45., x_max=45.)
NOISY = [400, 470, 480, 410, 350, 320, 355, 275, 300, 650, 560, 620, 760, 700, 970, 1160]


class PaceTrendTests(unittest.TestCase):
    def assert_single_minimum(self, model):
        previous = evaluate(model, model['x_min'])
        for i in range(1, 901):
            x = model['x_min'] + (model['x_max']-model['x_min'])*i/900
            value = evaluate(model, x)
            self.assertGreater(value, 0)
            if x <= model['optimum']:
                self.assertLessEqual(value, previous+1e-8)
            elif x - (model['x_max']-model['x_min'])/900 >= model['optimum']:
                self.assertGreaterEqual(value, previous-1e-8)
            previous = value

    def test_recovers_known_curve_and_horizontal_tangent(self):
        model = fit([dict(x=x, pace=evaluate(KNOWN, x)) for x in GRADES])
        for key in ('minimum', 'left', 'right', 'optimum'):
            self.assertAlmostEqual(model[key], KNOWN[key], places=6)
        opt = model['optimum']
        for delta in (-.0001, .0001):
            self.assertLess(abs((evaluate(model, opt+delta)-evaluate(model, opt))/delta), .001)
        self.assert_single_minimum(model)

    def test_noisy_personal_and_collective_join_has_no_extra_extrema(self):
        slopes = [f's{i}' for i in range(len(GRADES))]
        centers = dict(zip(slopes, GRADES))
        # Alternating reliable personal and median cells produce the old bumps.
        profile = {'zones': {'Z': {s: {'pace_duration_sec':300, 'num_points':20} for s in slopes[::2]}}}
        own = {s: {'Z':value} for s, value in zip(slopes, NOISY)}
        cohort = {'zones': {'Z': {s: {'p50':v, 'count':8} for s, v in zip(slopes, NOISY)}}}
        result = reference_model(profile, own, cohort, slopes, ['Z'], centers)
        model = result['curves']['Z']
        self.assert_single_minimum(model)
        self.assertEqual(result['counts'], dict(personal=8, median=8, missing=0))
        for slope, grade in centers.items():
            self.assertAlmostEqual(result['lookup'][slope]['Z'], evaluate(model, grade))
        # The isolated slow band no longer dictates either chart or projection.
        self.assertLess(result['lookup']['s9']['Z'], NOISY[9]-100)

    def test_scaling_preserves_the_shape(self):
        points = [dict(x=x, pace=p) for x, p in zip(GRADES, NOISY)]
        original = fit(points)
        for factor in (.02, .8, 1.6):
            scaled = fit([dict(x=p['x'], pace=p['pace']*factor) for p in points])
            self.assertEqual(scaled['optimum'], original['optimum'])
            for x in GRADES:
                self.assertAlmostEqual(evaluate(scaled, x), factor*evaluate(original, x), places=6)

    def test_sparse_and_invalid_data_do_not_invent_a_full_curve(self):
        self.assertIsNone(fit([]))
        self.assertIsNone(fit([dict(x=0, pace=300), dict(x=10, pace=400)]))
        self.assertIsNone(fit([dict(x=0, pace=math.nan)]))
        model = fit([dict(x=x, pace=evaluate(KNOWN, x)) for x in (0, 10, 20)])
        self.assertIsNone(evaluate(model, -5))
        self.assertIsNone(evaluate(model, 21))
        self.assertIsNone(evaluate(model, math.nan))
