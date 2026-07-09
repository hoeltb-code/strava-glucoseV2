from __future__ import annotations

import datetime as dt
from typing import Optional

from app.providers.base import ProviderConnectionResult, common_to_legacy_points
from . import dexcom_share, librelinkup, medtronic_carelink


KNOWN_PROVIDERS = ("abbott", "dexcom", "medtronic_carelink")
_LEGACY_PROVIDER_MAP = {
    "libre": "abbott",
    "dexcom": "dexcom",
    "medtronic_carelink": "medtronic_carelink",
}


def get_user_provider_preference(user) -> Optional[str]:
    provider = (getattr(user, "glucose_provider", None) or "").strip().lower()
    if provider in KNOWN_PROVIDERS:
        return provider
    legacy = (getattr(user, "cgm_source", None) or "").strip().lower()
    return _LEGACY_PROVIDER_MAP.get(legacy)


def resolve_provider_order(user) -> list[str]:
    preferred = get_user_provider_preference(user)
    configured = []
    if librelinkup.is_configured(user):
        configured.append("abbott")
    if dexcom_share.is_configured(user):
        configured.append("dexcom")
    if medtronic_carelink.is_configured(user):
        configured.append("medtronic_carelink")

    if preferred in configured:
        return [preferred] + [provider for provider in configured if provider != preferred]
    return configured


def _fetch_from_provider(db, user, provider: str, start: dt.datetime, end: dt.datetime):
    if provider == "abbott":
        return librelinkup.fetch_glucose(user, start, end)
    if provider == "dexcom":
        return dexcom_share.fetch_glucose(user, start, end, db=db)
    if provider == "medtronic_carelink":
        return medtronic_carelink.fetch_glucose(user, start, end)
    raise ValueError(f"Provider inconnu: {provider}")


def fetch_common_glucose_points(db, user, start: dt.datetime, end: dt.datetime):
    attempted: list[str] = []
    errors: dict[str, str] = {}
    for provider in resolve_provider_order(user):
        attempted.append(provider)
        try:
            points = _fetch_from_provider(db, user, provider, start, end)
        except Exception as exc:  # noqa: BLE001
            errors[provider] = str(exc)
            continue
        if points:
            return points, provider, {"attempted_sources": attempted, "errors": errors, "reason": None}

    return [], None, {"attempted_sources": attempted, "errors": errors, "reason": None}


def fetch_legacy_glucose_points(db, user, start: dt.datetime, end: dt.datetime):
    points, provider, meta = fetch_common_glucose_points(db, user, start, end)
    return common_to_legacy_points(points), provider, meta


def test_provider_connection(user) -> ProviderConnectionResult:
    provider = get_user_provider_preference(user)
    if provider == "abbott":
        return librelinkup.test_connection(user)
    if provider == "dexcom":
        return dexcom_share.test_connection(user)
    if provider == "medtronic_carelink":
        return medtronic_carelink.test_connection(user)
    return ProviderConnectionResult(
        ok=False,
        status="not_configured",
        message="Aucun fournisseur CGM sélectionné.",
        provider="unknown",
    )
