from __future__ import annotations

import datetime as dt
from typing import Any
from urllib.parse import urlparse

import requests

from app.providers.base import ProviderConnectionResult, common_point, ensure_utc
from app.secrets import decrypt_secret


NIGHTSCOUT_TIMEOUT_SECONDS = 20
NIGHTSCOUT_ALLOWED_TYPES = {"sgv", "mbg", "cal"}


class NightscoutError(RuntimeError):
    pass


def is_configured(user) -> bool:
    cred = getattr(user, "nightscout_credentials", None)
    return bool(cred and getattr(cred, "base_url", None))


def normalize_base_url(raw_url: str) -> str:
    value = (raw_url or "").strip()
    if not value:
        raise NightscoutError("URL Nightscout requise.")
    if not value.startswith(("http://", "https://")):
        raise NightscoutError("L'URL Nightscout doit commencer par http:// ou https://.")

    parsed = urlparse(value)
    if not parsed.scheme or not parsed.netloc:
        raise NightscoutError("URL Nightscout invalide.")
    return value.rstrip("/")


def _build_headers(token: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "strava-glucose-nightscout/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _request_entries(base_url: str, token: str | None, params: dict[str, Any]) -> list[dict[str, Any]]:
    headers = _build_headers(token)
    request_params = dict(params)
    if token:
        request_params.setdefault("token", token)

    try:
        response = requests.get(
            f"{base_url}/api/v1/entries.json",
            headers=headers,
            params=request_params,
            timeout=NIGHTSCOUT_TIMEOUT_SECONDS,
        )
    except requests.Timeout as exc:
        raise NightscoutError("Nightscout ne répond pas dans le délai imparti.") from exc
    except requests.RequestException as exc:
        raise NightscoutError(f"Impossible de joindre Nightscout : {exc}") from exc

    if response.status_code in {401, 403}:
        raise NightscoutError("Connexion impossible : vérifiez l'URL ou le token.")
    if response.status_code >= 400:
        raise NightscoutError(f"Nightscout a répondu avec une erreur HTTP {response.status_code}.")

    try:
        payload = response.json()
    except ValueError as exc:
        raise NightscoutError("Réponse Nightscout invalide (JSON illisible).") from exc

    if not isinstance(payload, list):
        raise NightscoutError("Réponse Nightscout invalide.")
    return payload


def _entry_timestamp(entry: dict[str, Any]) -> dt.datetime | None:
    raw_date = entry.get("date")
    if isinstance(raw_date, (int, float)) and raw_date > 0:
        if raw_date > 10_000_000_000:
            raw_date = raw_date / 1000
        return dt.datetime.fromtimestamp(raw_date, tz=dt.timezone.utc)
    return ensure_utc(entry.get("dateString"))


def _entry_is_usable(entry: dict[str, Any]) -> bool:
    raw_type = str(entry.get("type") or entry.get("entryType") or "sgv").strip().lower()
    return raw_type in NIGHTSCOUT_ALLOWED_TYPES


def normalize_entries(
    entries: list[dict[str, Any]],
    *,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    start_utc = ensure_utc(start)
    end_utc = ensure_utc(end)
    out: list[dict[str, Any]] = []

    for entry in entries:
        if not isinstance(entry, dict) or not _entry_is_usable(entry):
            continue

        sgv = entry.get("sgv")
        if not isinstance(sgv, (int, float)):
            continue

        ts = _entry_timestamp(entry)
        if ts is None:
            continue
        if start_utc and ts < start_utc:
            continue
        if end_utc and ts > end_utc:
            continue

        out.append(
            common_point(
                timestamp=ts,
                glucose=float(sgv),
                trend=entry.get("direction"),
                source="nightscout",
                raw=entry,
            )
        )

    out.sort(key=lambda row: row["timestamp"])
    return out


def fetch_nightscout_glucose(user, start: dt.datetime, end: dt.datetime) -> list[dict[str, Any]]:
    cred = getattr(user, "nightscout_credentials", None)
    if cred is None or not cred.base_url:
        return []

    base_url = normalize_base_url(cred.base_url)
    token = decrypt_secret(cred.read_token_encrypted) or None
    start_utc = ensure_utc(start)
    end_utc = ensure_utc(end)

    params: dict[str, Any] = {"count": 1000}
    if start_utc is not None and end_utc is not None:
        params["find[date][$gte]"] = int(start_utc.timestamp() * 1000)
        params["find[date][$lte]"] = int(end_utc.timestamp() * 1000)

    entries = _request_entries(base_url, token, params)
    points = normalize_entries(entries, start=start_utc, end=end_utc)
    if not points:
        raise NightscoutError("Aucune donnée glycémique exploitable trouvée sur cette période.")
    return points


def fetch_glucose(user, start: dt.datetime, end: dt.datetime) -> list[dict[str, Any]]:
    return fetch_nightscout_glucose(user, start, end)


def test_connection(user) -> ProviderConnectionResult:
    cred = getattr(user, "nightscout_credentials", None)
    if cred is None or not cred.base_url:
        return ProviderConnectionResult(
            ok=False,
            status="not_configured",
            message="Aucune instance Nightscout configurée.",
            provider="nightscout",
        )

    try:
        points = fetch_nightscout_glucose(
            user,
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24),
            dt.datetime.now(dt.timezone.utc),
        )
    except NightscoutError as exc:
        return ProviderConnectionResult(
            ok=False,
            status="error",
            message=str(exc),
            provider="nightscout",
        )

    latest = points[-1]
    latest_ts = latest["timestamp"].astimezone(dt.timezone.utc).strftime("%H:%M")
    return ProviderConnectionResult(
        ok=True,
        status="ok",
        message=(
            f"Connexion Nightscout réussie — dernière glycémie : "
            f"{int(round(latest['glucose']))} mg/dL à {latest_ts}"
        ),
        provider="nightscout",
        last_sync_at=dt.datetime.now(dt.timezone.utc),
    )
