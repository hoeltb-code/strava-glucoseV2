from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import tests.test_env  # noqa: F401

from app.strava_client import STRAVA_OAUTH_REVOKE_URL, StravaClient


class StravaClientTests(unittest.TestCase):
    def test_revoke_authorization_calls_strava_before_deleting_tokens(self):
        token = SimpleNamespace(refresh_token="refresh-token")
        lookup_db = MagicMock()
        delete_db = MagicMock()
        client = StravaClient(user_id=42)
        client._get_token_record = MagicMock(return_value=token)

        response = MagicMock()
        http_client = MagicMock()
        http_client.post = AsyncMock(return_value=response)
        async_context = MagicMock()
        async_context.__aenter__ = AsyncMock(return_value=http_client)
        async_context.__aexit__ = AsyncMock(return_value=None)

        with patch("app.strava_client.SessionLocal", side_effect=[lookup_db, delete_db]), patch(
            "app.strava_client.httpx.AsyncClient", return_value=async_context
        ):
            revoked = asyncio.run(client.revoke_authorization())

        self.assertTrue(revoked)
        http_client.post.assert_awaited_once_with(
            STRAVA_OAUTH_REVOKE_URL,
            auth=("test", "test"),
            data={"token": "refresh-token", "token_type_hint": "refresh_token"},
            timeout=15.0,
        )
        response.raise_for_status.assert_called_once_with()
        delete_db.query.return_value.filter.return_value.delete.assert_called_once_with()
        delete_db.commit.assert_called_once_with()
        lookup_db.close.assert_called_once_with()
        delete_db.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
