from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import tests.test_env  # noqa: F401

from app.routers import webhooks


class _DummyQuery:
    def __init__(self, token):
        self._token = token

    def filter(self, *_args, **_kwargs):
        return self

    def first(self):
        return self._token


class _DummyDb:
    def __init__(self, token):
        self._token = token
        self.closed = False

    def query(self, _model):
        return _DummyQuery(self._token)

    def close(self):
        self.closed = True


class WebhookTests(unittest.TestCase):
    def test_webhook_queues_enrichment_job(self):
        body = {
            "aspect_type": "create",
            "object_id": 123456,
            "object_type": "activity",
            "owner_id": 789,
            "subscription_id": 1,
            "updates": {},
        }
        token = SimpleNamespace(user_id=42)
        db = _DummyDb(token)

        with patch("app.routers.webhooks.SessionLocal", return_value=db), patch(
            "app.main.request_activity_enrichment",
            new=AsyncMock(return_value={"status": "queued", "job_id": 12}),
        ) as request_enrichment:
            result = asyncio.run(webhooks.webhook_event(body))

        self.assertEqual(result, {"status": "received"})
        self.assertTrue(db.closed)
        request_enrichment.assert_awaited_once_with(
            123456,
            user_id=42,
            trigger_source="webhook",
            immediate=False,
        )


if __name__ == "__main__":
    unittest.main()
