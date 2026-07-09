from __future__ import annotations

import datetime as dt

from app.dexcom_client import DexcomClient, has_dexcom_share_credentials, test_dexcom_credentials
from app.providers.base import ProviderConnectionResult, legacy_to_common_points
from app.secrets import decrypt_secret


def is_configured(user) -> bool:
    return has_dexcom_share_credentials(getattr(user, "dexcom_tokens", None))


def _get_record(user):
    tokens = getattr(user, "dexcom_tokens", None) or []
    ordered = sorted(tokens, key=lambda item: item.id or 0, reverse=True)
    for token in ordered:
        if has_dexcom_share_credentials(token):
            return token
    return None


def test_connection(user) -> ProviderConnectionResult:
    token = _get_record(user)
    if token is None:
        return ProviderConnectionResult(
            ok=False,
            status="not_configured",
            message="Aucun compte Dexcom Share configuré.",
            provider="dexcom",
        )

    status, message = test_dexcom_credentials(
        username=token.share_username or "",
        password=decrypt_secret(token.share_password) or "",
        region=token.share_region or "ous",
        user_id=getattr(user, "id", None),
    )
    return ProviderConnectionResult(
        ok=status == "ok",
        status=status,
        message=message,
        provider="dexcom",
    )


def fetch_glucose(user, start: dt.datetime, end: dt.datetime, *, db) -> list[dict]:
    client = DexcomClient(user_id=user.id, db=db)
    points = client.get_graph(start=start, end=end) or []
    return legacy_to_common_points(points, source="dexcom")
