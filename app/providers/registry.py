from __future__ import annotations

import datetime as dt
from typing import Optional

from app.providers.base import ProviderConnectionResult, common_to_legacy_points
from . import dexcom_share, librelinkup, medtronic_carelink, nightscout


KNOWN_PROVIDERS = ("abbott", "dexcom", "medtronic_carelink", "nightscout")
PROVIDER_LABELS = {
    "abbott": "LibreLinkUp",
    "dexcom": "Dexcom Share",
    "medtronic_carelink": "Medtronic CareLink",
    "nightscout": "Nightscout",
    "none": "Aucune source active",
}
_LEGACY_PROVIDER_MAP = {
    "libre": "abbott",
    "abbott": "abbott",
    "dexcom": "dexcom",
    "medtronic_carelink": "medtronic_carelink",
    "nightscout": "nightscout",
    "none": "none",
}
_LEGACY_CGM_MAP = {
    "abbott": "libre",
    "dexcom": "dexcom",
    "medtronic_carelink": "medtronic_carelink",
    "nightscout": "nightscout",
}


def get_configured_glucose_sources(user) -> list[str]:
    configured: list[str] = []
    if librelinkup.is_configured(user):
        configured.append("abbott")
    if dexcom_share.is_configured(user):
        configured.append("dexcom")
    if medtronic_carelink.is_configured(user):
        configured.append("medtronic_carelink")
    if nightscout.is_configured(user):
        configured.append("nightscout")
    return configured


def normalize_provider_name(provider: Optional[str]) -> Optional[str]:
    value = (provider or "").strip().lower()
    if not value:
        return None
    return _LEGACY_PROVIDER_MAP.get(value)


def get_active_glucose_source(user) -> Optional[str]:
    active = normalize_provider_name(getattr(user, "glucose_source_active", None))
    if active in KNOWN_PROVIDERS:
        return active

    provider = normalize_provider_name(getattr(user, "glucose_provider", None))
    if provider in KNOWN_PROVIDERS:
        return provider

    legacy = normalize_provider_name(getattr(user, "cgm_source", None))
    if legacy in KNOWN_PROVIDERS:
        return legacy

    configured = get_configured_glucose_sources(user)
    if len(configured) == 1:
        return configured[0]
    return None


def get_user_provider_preference(user) -> Optional[str]:
    return get_active_glucose_source(user)


def set_active_glucose_source(user, provider: Optional[str]) -> Optional[str]:
    normalized = normalize_provider_name(provider)
    if normalized not in KNOWN_PROVIDERS:
        user.glucose_source_active = None
        user.glucose_provider = None
        user.cgm_source = None
        return None

    user.glucose_source_active = normalized
    user.glucose_provider = normalized
    user.cgm_source = _LEGACY_CGM_MAP[normalized]
    return normalized


def get_glucose_source_label(provider: Optional[str]) -> str:
    return PROVIDER_LABELS.get(normalize_provider_name(provider) or "none", "Aucune source active")


def resolve_provider_order(user) -> list[str]:
    active = get_active_glucose_source(user)
    if active not in KNOWN_PROVIDERS:
        return []
    return [active]


def _fetch_from_provider(db, user, provider: str, start: dt.datetime, end: dt.datetime):
    if provider == "abbott":
        return librelinkup.fetch_glucose(user, start, end)
    if provider == "dexcom":
        return dexcom_share.fetch_glucose(user, start, end, db=db)
    if provider == "medtronic_carelink":
        return medtronic_carelink.fetch_glucose(user, start, end)
    if provider == "nightscout":
        return nightscout.fetch_glucose(user, start, end)
    raise ValueError(f"Provider inconnu: {provider}")


def fetch_common_glucose_points(db, user, start: dt.datetime, end: dt.datetime):
    attempted: list[str] = []
    errors: dict[str, str] = {}
    active = get_active_glucose_source(user)
    if active not in KNOWN_PROVIDERS:
        return [], None, {"attempted_sources": attempted, "errors": errors, "reason": "no_active_source"}

    attempted.append(active)
    try:
        points = _fetch_from_provider(db, user, active, start, end)
    except Exception as exc:  # noqa: BLE001
        errors[active] = str(exc)
        return [], None, {"attempted_sources": attempted, "errors": errors, "reason": "provider_error"}

    return points, active, {"attempted_sources": attempted, "errors": errors, "reason": None}


def fetch_legacy_glucose_points(db, user, start: dt.datetime, end: dt.datetime):
    points, provider, meta = fetch_common_glucose_points(db, user, start, end)
    return common_to_legacy_points(points), provider, meta


def get_glucose_data_for_user(db, user, start: dt.datetime, end: dt.datetime):
    return fetch_legacy_glucose_points(db, user, start, end)


def test_provider_connection(user, provider: Optional[str] = None) -> ProviderConnectionResult:
    selected = normalize_provider_name(provider) or get_active_glucose_source(user)
    if selected == "abbott":
        return librelinkup.test_connection(user)
    if selected == "dexcom":
        return dexcom_share.test_connection(user)
    if selected == "medtronic_carelink":
        return medtronic_carelink.test_connection(user)
    if selected == "nightscout":
        return nightscout.test_connection(user)
    return ProviderConnectionResult(
        ok=False,
        status="not_configured",
        message="Aucun fournisseur CGM sélectionné.",
        provider="unknown",
    )
