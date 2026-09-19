import datetime as dt
import math
import unittest
from types import SimpleNamespace
from app.glucose_metrics import summarize, chart_series
from app.energy import terrain_energy, heart_rate_energy, activity_energy, strava_energy_block
from app.pace_reference import reference_model
from app.activity_analytics import downsample, build_summary
from app.athlete_summary import aggregate
from app.logic import compute_stats, merge_desc


class DataAnalyticsTests(unittest.TestCase):
    def test_time_weighting_bounds_and_gap_exclusion(self):
        samples=[{'ts':0,'mgdl':70},{'ts':60,'mgdl':180},{'ts':180,'mgdl':60},{'ts':240,'mgdl':200},{'ts':1200,'mgdl':100},{'ts':1260,'mgdl':100}]
        result=summarize(samples,0,1320,max_gap_seconds=180)
        self.assertEqual(result['observed_seconds'],300)
        self.assertEqual(result['low_seconds'],60)
        self.assertEqual(result['high_seconds'],0)
        self.assertEqual(result['pct_in_range'],80)
        self.assertAlmostEqual(result['avg'],118)
        self.assertEqual(result['missing_seconds'],1020)
        self.assertTrue(any(p['y'] is None for p in chart_series(samples,0,1320)))

    def test_empty_single_duplicate_and_nonfinite(self):
        self.assertIsNone(summarize([],0,100)['pct_in_range'])
        self.assertFalse(summarize([{'ts':0,'mgdl':120}],0,100)['available'])
        samples=[{'ts':0,'mgdl':110},{'ts':0,'mgdl':180},{'ts':60,'mgdl':100},{'ts':30,'mgdl':math.nan}]
        self.assertEqual(summarize(samples)['avg'],180)
        self.assertEqual(compute_stats(samples)['pct_in_range'],100)

    def test_clipping_windows_does_not_extrapolate(self):
        samples=[{'ts':0,'mgdl':50},{'ts':60,'mgdl':150},{'ts':120,'mgdl':180}]
        r=summarize(samples,30,180)
        self.assertEqual(r['observed_seconds'],90)
        self.assertEqual(r['low_seconds'],30)
        self.assertEqual(r['coverage_percent'],60)

    def test_terrain_cost_speed_weight_and_descent(self):
        flat=terrain_energy(70,0,360,10)
        self.assertAlmostEqual(flat['kcal'],3.6*70*10000/4184)
        self.assertEqual(terrain_energy(70,0,720,10)['kcal'],flat['kcal'])
        self.assertAlmostEqual(terrain_energy(70,0,720,10)['kcal_hour'],flat['kcal_hour']/2)
        self.assertAlmostEqual(terrain_energy(140,0,360,10)['kcal'],flat['kcal']*2)
        self.assertGreater(terrain_energy(70,10,360)['kcal_km'],flat['kcal_km'])
        self.assertLess(terrain_energy(70,-10,360)['kcal_km'],flat['kcal_km'])
        self.assertGreater(terrain_energy(70,-45,360)['kcal_km'],terrain_energy(70,-10,360)['kcal_km'])
        for args in [(None,0,300),(70,46,300),(70,0,0),(70,math.nan,300)]:self.assertIsNone(terrain_energy(*args))
        self.assertIsNone(heart_rate_energy(70,150,None,'male',190))
        self.assertIsNone(heart_rate_energy(70,150,55,'male',190))
        self.assertGreater(heart_rate_energy(70,150,35,'male',190),0)

    def test_activity_energy_skips_pauses_gaps_and_unknown_sports(self):
        points=[dict(elapsed_time=i*10,distance=i*20,slope_percent=10,heartrate=150,moving=True) for i in range(4)]
        points.append(dict(elapsed_time=100,distance=100,slope_percent=10,moving=True))
        r=activity_energy(points,70,max_hr=190)
        self.assertEqual(r['coverage_percent'],30)
        self.assertAlmostEqual(r['kcal'],terrain_energy(70,10,500,.06)['kcal'])
        self.assertIn('Montée +10 %',strava_energy_block(r,'terrain'))
        self.assertFalse(activity_energy(points,70,sport='ride')['available'])
        block=strava_energy_block(r)+'\nPour tous les fans de data —> Join us : https://example.com/'
        result=merge_desc('Mon texte\n\n'+block+'\n\nTexte préservé',block)
        self.assertEqual(result.count('🔥'),1)
        self.assertIn('Texte préservé',result)

    def test_reference_personal_threshold_median_privacy(self):
        profile={'zones':{'Z':{'s1':{'pace_duration_sec':300,'num_points':20},'s2':{'duration_sec':299,'num_points':100}}}}
        own={'s1':{'Z':350},'s2':{'Z':100}}
        cohort={'zones':{'Z':{'s1':{'count':8,'p50':400},'s2':{'count':8,'p50':500},'s3':{'count':7,'p50':600}}}}
        r=reference_model(profile,own,cohort,['s1','s2','s3'],['Z'])
        self.assertEqual(r['lookup'],{'s1':{'Z':350.},'s2':{'Z':500.}})
        self.assertEqual(r['counts'],{'personal':1,'median':1,'missing':1})

    def test_summary_aggregate_weights_observed_duration(self):
        base=dict(id=1,name='Run',start_date=dt.datetime(2026,1,1),distance=1000,elapsed_time=600)
        rows=[]
        for observed,tir in [(600,100),(60,0)]:
            rows.append(SimpleNamespace(**base,analytics_summary={'glycemia':{'version':'duration-v2','observed_seconds':observed,'pct_in_range':tir,'low_seconds':0,'high_seconds':0}}))
        r=aggregate(rows)
        self.assertAlmostEqual(r['tir'],100*600/660)
        self.assertEqual(r['coverage'],55)
        self.assertEqual(r['weeks'][0]['count'],2)

    def test_downsampling_preserves_peak_and_gap(self):
        p=[{'x':i,'y':120 if i!=333 else 250} for i in range(10000)]
        p[600]['y']=None
        r=downsample(p,400)
        self.assertIn(p[333],r)
        self.assertTrue(any(v['y'] is None for v in r))
        self.assertLessEqual(len(r),800)
        self.assertEqual(r[0],p[0]);self.assertEqual(r[-1],p[-1])

if __name__=='__main__':unittest.main()
