"""Signup against an isolated database, without startup workers or external calls."""
import unittest
from unittest.mock import patch

import tests.test_env
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from starlette.middleware.sessions import SessionMiddleware

from app import main
from app.database import Base
from app.models import User


class SignupTests(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine('sqlite://', connect_args={'check_same_thread': False}, poolclass=StaticPool)
        Base.metadata.create_all(self.engine)
        self.factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        app = FastAPI()
        app.include_router(main.app.router)
        app.add_middleware(SessionMiddleware, secret_key='signup-test-only')
        self.client = TestClient(app)  # No lifespan: never start production workers.
        self.database_patch = patch.object(main, 'SessionLocal', self.factory)
        self.database_patch.start()

    def tearDown(self):
        self.client.close()
        self.database_patch.stop()
        self.engine.dispose()

    def test_anonymous_form_is_accessible(self):
        response = self.client.get('/ui/signup')
        self.assertEqual(response.status_code, 200)
        self.assertIn('autocomplete="new-password"', response.text)
        self.assertNotIn('signup-email-error', response.text)

    def test_duplicate_email_preserves_safe_fields_but_never_password(self):
        with self.factory() as db:
            db.add(User(email='runner@example.test', password_hash='unused'))
            db.commit()
        response = self.client.post('/ui/signup', data={
            'email': 'runner@example.test', 'password': 'never-echo-this-password',
            'first_name': '<script>alert(1)</script>', 'last_name': 'Martin', 'location': 'Annecy',
        })
        self.assertEqual(response.status_code, 400)
        self.assertIn('Cette adresse est déjà utilisée.', response.text)
        self.assertIn('value="runner@example.test"', response.text)
        self.assertIn('value="Martin"', response.text)
        self.assertIn('value="Annecy"', response.text)
        self.assertIn('&lt;script&gt;', response.text)
        self.assertNotIn('<script>alert(1)</script>', response.text)
        self.assertNotIn('never-echo-this-password', response.text)
        self.assertNotIn('session=', response.headers.get('set-cookie', ''))
        with self.factory() as db:
            self.assertEqual(db.query(User).count(), 1)

    def test_success_keeps_profile_and_starts_authenticated_welcome(self):
        response = self.client.post('/ui/signup', data={
            'email': 'new@example.test', 'password': 'a-test-password',
            'first_name': 'Camille', 'last_name': 'Martin', 'location': 'Annecy',
        }, follow_redirects=False)
        self.assertEqual(response.status_code, 302)
        with self.factory() as db:
            user = db.query(User).one()
            self.assertTrue(main.pwd_context.verify('a-test-password', user.password_hash))
            self.assertEqual((user.first_name, user.last_name, user.location), ('Camille', 'Martin', 'Annecy'))
            self.assertEqual(response.headers['location'], f'/ui/user/{user.id}/welcome')
        self.assertIn('session=', response.headers['set-cookie'])


if __name__ == '__main__':
    unittest.main()
