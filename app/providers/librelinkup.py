from __future__ import annotations

import datetime as dt

from app.libre_client import read_graph, test_libre_credentials
from app.providers.base import ProviderConnectionResult, ensure_utc, legacy_to_common_points
from app.secrets import decrypt_secret


def is_configured(user) -> bool:
    return bool(getattr(user, "libre_credentials", None))


def test_connection(user) -> ProviderConnectionResult:
    cred = getattr(user, "libre_credentials", None)
    if cred is None:
        return ProviderConnectionResult(
            ok=False,
            status="not_configured",
            message="Aucun compte LibreLinkUp configuré.",
            provider="abbott",
        )

    status, message = test_libre_credentials(
        email=cred.email,
        password=decrypt_secret(cred.password_encrypted) or "",
        region=cred.region or "fr",
        client_version=cred.client_version or "4.16.0",
        user_id=getattr(user, "id", None),
    )
    return ProviderConnectionResult(
        ok=status == "ok",
        status=status,
        message=message,
        provider="abbott",
    )


def fetch_glucose(user, start: dt.datetime, end: dt.datetime) -> list[dict]:
    points = read_graph(user_id=user.id) or []
    common = legacy_to_common_points(points, source="abbott")
    start_utc = ensure_utc(start)
    end_utc = ensure_utc(end)
    return [
        point for point in common
        if start_utc is None
        or end_utc is None
        or (start_utc <= point["timestamp"] <= end_utc)
    ]
