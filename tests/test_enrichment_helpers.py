from __future__ import annotations

import unittest
from types import SimpleNamespace

import tests.test_env  # noqa: F401

from app.main import (
    ENRICHMENT_RETRY_BASE_SECONDS,
    ENRICHMENT_RETRY_MAX_SECONDS,
    _acquire_activity_enrichment_lock,
    _compute_enrichment_retry_delay_seconds,
    _is_retryable_enrichment_reason,
    _release_activity_enrichment_lock,
    _schedule_enrichment_retry,
)


class EnrichmentHelperTests(unittest.TestCase):
    def test_retryable_reason_detection(self):
        self.assertTrue(_is_retryable_enrichment_reason("libre_cooldown"))
        self.assertTrue(_is_retryable_enrichment_reason("strava_fetch_error"))
        self.assertFalse(_is_retryable_enrichment_reason("no_glucose_coverage"))
        self.assertFalse(_is_retryable_enrichment_reason(None))

    def test_retry_delay_grows_and_caps(self):
        self.assertEqual(_compute_enrichment_retry_delay_seconds(1), ENRICHMENT_RETRY_BASE_SECONDS)
        self.assertGreaterEqual(
            _compute_enrichment_retry_delay_seconds(2),
            ENRICHMENT_RETRY_BASE_SECONDS,
        )
        self.assertLessEqual(
            _compute_enrichment_retry_delay_seconds(99),
            ENRICHMENT_RETRY_MAX_SECONDS,
        )

    def test_activity_lock_is_exclusive(self):
        user_id = 12
        activity_id = 345
        try:
            self.assertTrue(_acquire_activity_enrichment_lock(user_id, activity_id))
            self.assertFalse(_acquire_activity_enrichment_lock(user_id, activity_id))
        finally:
            _release_activity_enrichment_lock(user_id, activity_id)
        self.assertTrue(_acquire_activity_enrichment_lock(user_id, activity_id))
        _release_activity_enrichment_lock(user_id, activity_id)

    def test_schedule_retry_marks_job_retry(self):
        job = SimpleNamespace(
            user_id=1,
            strava_activity_id=99,
            attempts=1,
            status="processing",
            last_reason=None,
            last_error=None,
            next_retry_at=None,
            locked_at="x",
            completed_at=None,
            trigger_source=None,
        )

        _schedule_enrichment_retry(
            db=None,  # unused by the helper beyond the signature
            job=job,
            reason="libre_cooldown",
            last_error="temporary",
            trigger_source="webhook",
        )

        self.assertEqual(job.status, "retry")
        self.assertEqual(job.last_reason, "libre_cooldown")
        self.assertEqual(job.last_error, "temporary")
        self.assertIsNotNone(job.next_retry_at)
        self.assertIsNone(job.locked_at)
        self.assertEqual(job.trigger_source, "webhook")


if __name__ == "__main__":
    unittest.main()
