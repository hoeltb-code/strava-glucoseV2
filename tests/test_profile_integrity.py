"""In-memory regressions for correcting Strava performance archives."""
import asyncio
import datetime as dt
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import tests.test_env  # noqa: F401
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.database import Base
from app import logic, models
from app.routers import webhooks


class ProfileIntegrityTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://')
        Base.metadata.create_all(self.engine)
        self.factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.db = self.factory()
        self.month = dt.date(2026, 1, 1)
        for user_id in (10, 20):
            self.db.add(models.User(id=user_id,email=f'{user_id}@example.test',password_hash='unused'))
        self.db.add(models.StravaToken(user_id=10,athlete_id=789,access_token='test',refresh_token='test',expires_at=0))
        self.db.commit()

    def tearDown(self):
        self.db.close()
        self.engine.dispose()

    def contribute(self, user_id, activity_id, pace=300):
        self.db.add(models.RunnerProfileActivityContribution(
            user_id=user_id,strava_activity_id=activity_id,sport='run',
            activity_start_date=dt.datetime(2026,1,10),metric_scope='slope_zone',
            slope_band='S0_5',hr_zone='Zone 2',total_duration_sec=600,
            total_distance_m=2000,total_points=120,pace_duration_sec=600,
            sum_pace_x_duration=pace*600,
        ))
        self.db.flush()
        logic.rebuild_runner_profile_month_from_contributions(
            self.db,user_id=user_id,sport='run',month=self.month,
        )
        self.db.commit()

    def test_strava_deletion_after_retention_rebuilds_only_affected_profile(self):
        self.contribute(10, 100, 300)
        self.contribute(10, 101, 500)
        self.contribute(20, 100, 700)
        # No Activity rows: the detailed data was already purged.
        body=dict(object_type='activity',aspect_type='delete',owner_id=789,object_id=100)
        with patch.object(webhooks, 'SessionLocal', self.factory):
            for _ in range(2):
                self.assertEqual(asyncio.run(webhooks.webhook_event(body)), {'status':'deleted'})
        self.db.expire_all()
        self.assertEqual(self.db.query(models.RunnerProfileActivityContribution).filter_by(user_id=10).count(),1)
        own=self.db.query(models.RunnerProfileMonthly).filter_by(user_id=10,metric_scope='slope_zone').one()
        peer=self.db.query(models.RunnerProfileMonthly).filter_by(user_id=20,metric_scope='slope_zone').one()
        self.assertEqual(own.avg_pace_s_per_km,500)
        self.assertEqual(peer.avg_pace_s_per_km,700)
        logic.remove_archived_activity_contributions(self.db,user_id=10,strava_activity_id=101)
        self.assertEqual(self.db.query(models.RunnerProfileMonthly).filter_by(user_id=10).count(),0)

    def test_corrected_sport_and_month_remove_old_cache(self):
        self.contribute(10,100)
        activity=models.Activity(user_id=10,athlete_id=789,strava_activity_id=100,
                                 name='Corrected ride',sport='ride',activity_type='Ride',
                                 start_date=dt.datetime(2026,2,10),distance=2000,elapsed_time=600)
        self.db.add(activity)
        self.db.flush()
        logic.update_runner_profile_monthly_from_activity(self.db,activity=activity)
        self.assertEqual(self.db.query(models.RunnerProfileMonthly).filter_by(user_id=10,sport='run',year_month=self.month).count(),0)
        contribution=self.db.query(models.RunnerProfileActivityContribution).filter_by(user_id=10,metric_scope='activity_meta').one()
        self.assertEqual(contribution.sport,'ride')
        self.assertEqual(contribution.activity_start_date.month,2)

    def test_nonfinite_points_and_inconsistent_recorded_speed_are_excluded(self):
        previous=SimpleNamespace(elapsed_time=0,distance=0,altitude=100,velocity=2,moving=True)
        for field,value in [('elapsed_time',float('nan')),('distance',float('inf')),
                            ('altitude',float('nan')),('velocity',float('inf')),
                            ('velocity',50),('velocity',-2),('distance','invalid')]:
            current=SimpleNamespace(elapsed_time=5,distance=10,altitude=101,velocity=2,moving=True)
            setattr(current,field,value)
            with self.subTest(field=field,value=value):
                self.assertFalse(logic.is_valid_activity_stream_interval(previous,current))
        self.assertTrue(logic.is_valid_activity_stream_interval(previous,SimpleNamespace(
            elapsed_time=5,distance=10,altitude=101,velocity=2,moving=True)))


if __name__ == '__main__':
    unittest.main()
