"""Integration against an isolated in-memory DB. No application startup or external sync."""
import datetime as dt
import unittest
from unittest.mock import patch
from contextlib import ExitStack
from sqlalchemy import create_engine,event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app import main
from app.database import Base,get_db
from app.models import User,Activity,ActivityStreamPoint,GlucosePoint,RunnerProfileMonthly
from app.analytics_service import refresh_activity_summary


def make_fixture():
    engine=create_engine('sqlite://',connect_args={'check_same_thread':False},poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory=sessionmaker(bind=engine,expire_on_commit=False)
    with factory() as db:
        user=User(id=10,email='demo@example.test',password_hash='unused',first_name='Camille',weight_kg=70,max_heartrate=190,sex='female',birthdate=dt.date(1992,1,1))
        db.add(user)
        start=dt.datetime.utcnow().replace(microsecond=0)-dt.timedelta(days=1)
        activity=Activity(id=100,user_id=10,athlete_id=10,strava_activity_id=9900100,name='Les crêtes · sortie de démonstration',sport='run',activity_type='Run',start_date=start,elapsed_time=3600,distance=10000,total_elevation_gain=430,average_heartrate=148)
        db.add(activity);db.flush()
        import math
        for i in range(721):
            grade=10 if i<250 else -8 if i<500 else 0
            p=ActivityStreamPoint(activity_id=100,idx=i,elapsed_time=i*5,distance=i*10000/720,altitude=500+100*math.sin(i/150),heartrate=145+15*math.sin(i/35),velocity=2.78,grade=grade,slope_percent=grade,moving=True,glucose_mgdl=120+45*math.sin(i/70),hr_zone='Zone 3',cadence=80)
            db.add(p)
        for i in range(-6,97):
            if 25<i<32:continue
            db.add(GlucosePoint(user_id=10,ts=start+dt.timedelta(minutes=i*5),mgdl=120+55*math.sin(i/7),source='demo'))
        db.flush();refresh_activity_summary(db,activity,user)
        # Eight peers: no source identity appears in any UI or API payload.
        for uid in range(20,28):
            db.add(User(id=uid,email=f'demo{uid}@example.test',password_hash='unused'))
            for slope,_ in main.SLOPE_ORDER:
                for zone,*_ in main.HR_ZONES:
                    grade=main.SLOPE_BAND_CENTER[slope]
                    pace=350+max(grade,0)*25+max(-grade-8,0)*12+(uid-20)*2
                    db.add(RunnerProfileMonthly(user_id=uid,sport='run',year_month=start.date().replace(day=1),metric_scope='slope_zone',slope_band=slope,hr_zone=zone,total_duration_sec=600,pace_duration_sec=600,total_points=120,sum_pace_x_duration=pace*600))
        db.commit()
    app=FastAPI()
    app.include_router(main.app.router)
    app.mount('/static',main.StaticFiles(directory='static'),name='static')
    @app.middleware('http')
    async def session(request,call_next):
        request.scope['session']={'user_id':10}
        return await call_next(request)
    def db_dependency():
        with factory() as db:yield db
    app.dependency_overrides[get_db]=db_dependency
    return app,engine,factory


class DataPagesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app,cls.engine,cls.factory=make_fixture()
        cls.stack=ExitStack()
        for name,value in [('SessionLocal',cls.factory),('_guard_user_route',lambda *a:None),('_maybe_refresh_glucose_for_page_view',lambda *a,**kw:None),('_payment_pilot_allowed',lambda *a:False)]:
            cls.stack.enter_context(patch.object(main,name,value))
        cls.client=TestClient(cls.app)
        cls.statements=[]
        event.listen(cls.engine,'before_cursor_execute',lambda conn,cursor,statement,parameters,context,executemany:cls.statements.append(statement))

    @classmethod
    def tearDownClass(cls):cls.client.close();cls.stack.close();cls.engine.dispose()

    def test_read_only_pages_render_with_data(self):
        for url,text in [('/ui/user/10','Bonjour Camille'),('/ui/user/10/activities','Journal d’entraînement'),('/ui/user/10/energy','Et si la pente'),('/ui/user/10/runner-profile?tab=pace','Effort, glycémie et énergie'),('/ui/user/10/activity/100','Lire ensemble glycémie et effort')]:
            with self.subTest(url=url):
                self.statements.clear();r=self.client.get(url)
                self.assertEqual(r.status_code,200,r.text[:1000]);self.assertIn(text,r.text)
                self.assertFalse(any(q.lstrip().upper().startswith(('INSERT','UPDATE','DELETE')) for q in self.statements))
                if url.endswith('/activities'):
                    self.assertFalse(any('activity_stream_points' in q for q in self.statements))
                    self.assertLessEqual(len(self.statements),4)

    def test_median_reference_api_and_empty_states(self):
        r=self.client.get('/ui/user/10/runner-profile?format=json')
        self.assertEqual(r.status_code,200)
        payload=r.json()
        self.assertGreater(payload['pace_reference']['counts']['median'],0)
        self.assertNotIn('demo20',r.text)
        for url in ['/ui/user/20','/ui/user/20/energy','/ui/user/20/activities','/ui/user/20/runner-profile']:
            r=self.client.get(url);self.assertEqual(r.status_code,200,r.text[:200])

    def test_filtered_listing_has_no_stream_queries(self):
        r=self.client.get('/ui/user/10/activities?period=all&sport=ride&data=glucose')
        self.assertEqual(r.status_code,200)
        self.assertIn('Aucune activité ne correspond',r.text)

    def test_maintenance_archives_are_idempotent_and_survive_live_retention(self):
        from app import analytics_service
        from app.models import RunnerProfileActivityContribution
        from app.athlete_summary import load_summary
        with self.factory() as db:
            db.get(Activity,100).analytics_summary=None
            db.commit()
        with patch.object(analytics_service,'SessionLocal',self.factory):
            self.assertEqual(analytics_service.maintain_batch(1),1)
            self.assertEqual(analytics_service.maintain_batch(1),0)
        with self.factory() as db:
            entry=db.query(RunnerProfileActivityContribution).filter_by(user_id=10,metric_scope='activity_meta').one()
            self.assertTrue(entry.extra['analytics_summary']['glycemia']['available'])
            summary=load_summary(db,10)
            self.assertEqual(summary['count'],1)  # live + archive must not double count
            # Simulate missing live record without deleting fixture used by other tests.
            entry.strava_activity_id=123456789
            entry.activity_start_date=dt.datetime.utcnow()-dt.timedelta(days=31)
            db.flush()
            archived=load_summary(db,10,start=dt.datetime.utcnow()-dt.timedelta(days=60),end=dt.datetime.utcnow()-dt.timedelta(days=20))
            self.assertEqual(archived['count'],1)
            self.assertGreater(archived['observed_seconds'],0)
            self.assertIsNone(archived['latest'])
            db.rollback()

    def test_schema_upgrade_is_idempotent(self):
        from app import database
        from sqlalchemy import text,inspect
        isolated=create_engine('sqlite://')
        Base.metadata.create_all(isolated)
        with isolated.begin() as conn:
            conn.execute(text('ALTER TABLE activities DROP COLUMN analytics_summary'))
            conn.execute(text('ALTER TABLE user_settings DROP COLUMN desc_include_energy'))
            conn.execute(text('DROP INDEX ix_activity_user_start'))
            conn.execute(text('DROP INDEX ix_stream_activity_idx'))
        with patch.object(database,'engine',isolated):
            database.init_db();database.init_db()
        inspector=inspect(isolated)
        self.assertIn('analytics_summary',{c['name'] for c in inspector.get_columns('activities')})
        self.assertIn('desc_include_energy',{c['name'] for c in inspector.get_columns('user_settings')})
        self.assertIn('ix_activity_user_start',{c['name'] for c in inspector.get_indexes('activities')})
        self.assertIn('ix_stream_activity_idx',{c['name'] for c in inspector.get_indexes('activity_stream_points')})
        isolated.dispose()

    def test_settings_energy_is_opt_in_and_other_forms_preserve_it(self):
        from app.models import UserSettings
        common={'first_name':'Camille','weight_kg':'70','max_heartrate':'190','birthdate':'1992-01-01','sex':'female','desc_enable_auto_block':'1'}
        r=self.client.post('/ui/user/10/profile',data={**common,'desc_settings_present':'1','desc_include_energy':'1','desc_format':'terrain'},follow_redirects=False)
        self.assertEqual(r.status_code,302)
        self.client.post('/ui/user/10/profile',data=common,follow_redirects=False)
        with self.factory() as db:
            options=db.query(UserSettings).filter_by(user_id=10).one()
            self.assertTrue(options.desc_include_energy)
            self.assertEqual(options.desc_format,'terrain')
        self.client.post('/ui/user/10/profile',data={**common,'desc_settings_present':'1','desc_format':'compact'},follow_redirects=False)
        with self.factory() as db:self.assertFalse(db.query(UserSettings).filter_by(user_id=10).one().desc_include_energy)

    def test_plan_and_strava_settings_render(self):
        for url,text in [('/ui/user/10?view=plan','home-target-pacing-curve'),('/ui/user/10/profile','desc_include_energy')]:
            r=self.client.get(url);self.assertEqual(r.status_code,200,r.text[:1000]);self.assertIn(text,r.text)

if __name__=='__main__':unittest.main()
