from __future__ import annotations

import base64
import datetime as dt
import json
from typing import Any, Optional

import requests

from app.providers.base import (
    ProviderConnectionError,
    ProviderConnectionResult,
    common_point,
    ensure_utc,
)
from app.secrets import decrypt_secret, encrypt_secret


CARELINK_DISCOVERY_URL = "https://clcloud.minimed.eu/connect/carepartner/v11/discover/android/3.3"
CARELINK_AUTH_ERROR_CODES = {401, 403}
CARELINK_COMMON_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 10; Nexus 5X Build/QQ3A.200805.001)",
}


class CareLinkError(ProviderConnectionError):
    pass


class CareLinkNeedsReauthError(CareLinkError):
    pass


def _safe_json_loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def is_configured(user) -> bool:
    cred = getattr(user, "carelink_credentials", None)
    return cred is not None and bool(cred.username)


def _discovery_region(region: str | None) -> str:
    return "us" if (region or "").strip().upper() == "US" else "eu"


def _decode_access_token_country(access_token: str | None) -> Optional[str]:
    if not access_token:
        return None
    try:
        payload_b64 = access_token.split(".")[1]
        payload_b64_bytes = payload_b64.encode("ascii")
        missing_padding = (4 - len(payload_b64_bytes) % 4) % 4
        if missing_padding:
            payload_b64_bytes += b"=" * missing_padding
        payload_json = json.loads(base64.b64decode(payload_b64_bytes).decode("utf-8"))
        country = ((payload_json.get("token_details") or {}).get("country") or "").strip()
        return country or None
    except Exception:
        return None


def _load_config(region: str | None, access_token: str | None) -> dict[str, Any]:
    resp = requests.get(CARELINK_DISCOVERY_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    country = _decode_access_token_country(access_token)
    selected_region = None
    if country:
        for entry in data.get("supportedCountries", []):
            try:
                selected_region = entry[country.upper()]["region"]
                break
            except Exception:
                continue
    if selected_region is None:
        selected_region = _discovery_region(region).upper()

    config = None
    for entry in data.get("CP", []):
        if str(entry.get("region", "")).upper() == selected_region:
            config = dict(entry)
            break
    if config is None:
        raise CareLinkError(f"Region CareLink non supportée: {selected_region}")

    sso_resp = requests.get(config["SSOConfiguration"], timeout=20)
    sso_resp.raise_for_status()
    sso_config = sso_resp.json()
    server = sso_config["server"]
    token_url = (
        f"https://{server['hostname']}:{server['port']}/{server['prefix']}"
        f"{sso_config['system_endpoints']['token_endpoint_path']}"
    )
    config["token_url"] = token_url
    return config


def _auth_headers(token_data: dict[str, Any]) -> dict[str, str]:
    headers = dict(CARELINK_COMMON_HEADERS)
    headers["mag-identifier"] = token_data["mag_identifier"]
    headers["Authorization"] = f"Bearer {token_data['access_token']}"
    return headers


def _parse_token_payload(cred) -> dict[str, Any]:
    payload = {
        "access_token": decrypt_secret(cred.access_token) or "",
        "refresh_token": decrypt_secret(cred.refresh_token) or "",
        "client_id": cred.client_id or "",
        "client_secret": decrypt_secret(cred.client_secret) or "",
        "mag_identifier": cred.mag_identifier or "",
        "scope": cred.scope or "",
    }
    missing = [key for key, value in payload.items() if not value]
    if missing:
        raise CareLinkNeedsReauthError(
            "Connexion CareLink incomplète. Un bootstrap CareLink Connect interactif est requis."
        )
    return payload


def _refresh_tokens(cred) -> None:
    token_data = _parse_token_payload(cred)
    config = _load_config(cred.region, token_data["access_token"])
    data = {
        "refresh_token": token_data["refresh_token"],
        "client_id": token_data["client_id"],
        "client_secret": token_data["client_secret"],
        "grant_type": "refresh_token",
    }
    headers = {"mag-identifier": token_data["mag_identifier"]}
    resp = requests.post(config["token_url"], headers=headers, data=data, timeout=20)
    if resp.status_code != 200:
        raise CareLinkNeedsReauthError("Impossible de rafraîchir les tokens CareLink.")
    refreshed = resp.json()
    cred.access_token = encrypt_secret(refreshed.get("access_token"))
    cred.refresh_token = encrypt_secret(refreshed.get("refresh_token"))
    expires_in = refreshed.get("expires_in")
    if expires_in:
        cred.token_expires_at = dt.datetime.utcnow() + dt.timedelta(seconds=int(expires_in))


def _request_json(method: str, url: str, **kwargs) -> tuple[Any, int]:
    resp = requests.request(method, url, timeout=20, **kwargs)
    status = resp.status_code
    if status in CARELINK_AUTH_ERROR_CODES:
        return None, status
    resp.raise_for_status()
    return resp.json(), status


def _load_user_profile(config: dict[str, Any], token_data: dict[str, Any]) -> dict[str, Any]:
    data, status = _request_json(
        "GET",
        config["baseUrlCareLink"] + "/users/me",
        headers=_auth_headers(token_data),
    )
    if status in CARELINK_AUTH_ERROR_CODES or not data:
        raise CareLinkNeedsReauthError("Session CareLink expirée ou invalide.")
    return data


def _resolve_patient_id(config: dict[str, Any], token_data: dict[str, Any], role: str) -> Optional[str]:
    if role not in {"CARE_PARTNER", "CARE_PARTNER_OUS"}:
        return None
    data, status = _request_json(
        "GET",
        config["baseUrlCareLink"] + "/links/patients",
        headers=_auth_headers(token_data),
    )
    if status in CARELINK_AUTH_ERROR_CODES:
        raise CareLinkNeedsReauthError("Session CareLink expirée ou invalide.")
    if isinstance(data, list) and data:
        first = data[0]
        return first.get("username") or first.get("patientId")
    return None


def _fetch_recent_payload(config: dict[str, Any], token_data: dict[str, Any], username: str, role: str, patient_id: str | None):
    headers = _auth_headers(token_data)
    body: dict[str, Any] = {"username": username}
    if role in {"CARE_PARTNER", "CARE_PARTNER_OUS"}:
        body["role"] = "carepartner"
        body["patientId"] = patient_id
    else:
        body["role"] = "patient"

    data, status = _request_json(
        "POST",
        config["baseUrlCumulus"] + "/display/message",
        headers=headers,
        data=json.dumps(body),
    )
    if status in CARELINK_AUTH_ERROR_CODES:
        raise CareLinkNeedsReauthError("Session CareLink expirée ou invalide.")
    return data


def _walk_payload(value: Any):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_payload(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_payload(nested)


def _extract_timestamp(entry: dict[str, Any]) -> dt.datetime | None:
    keys = (
        "timestamp",
        "dateTime",
        "datetime",
        "date",
        "time",
        "sg",
        "sensorTime",
        "lastSGTrendDate",
    )
    for key in keys:
        raw = entry.get(key)
        if isinstance(raw, str):
            ts = ensure_utc(raw)
            if ts is not None:
                return ts
    for key in ("timestamp", "dateTime", "datetime", "date"):
        raw = entry.get(key)
        if isinstance(raw, (int, float)) and raw > 0:
            # CareLink frequently uses milliseconds.
            if raw > 10_000_000_000:
                raw = raw / 1000
            return dt.datetime.fromtimestamp(raw, tz=dt.timezone.utc)
    return None


def _extract_glucose(entry: dict[str, Any]) -> float | None:
    for key in ("sg", "glucose", "sensorGlucoseValue", "value", "mgdl", "mgDl"):
        value = entry.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _extract_trend(entry: dict[str, Any]) -> str | None:
    for key in ("trend", "trendArrow", "trendDescription", "trendDesc"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def normalize_carelink_payload(
    raw_payload: Any,
    *,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> list[dict[str, Any]]:
    start_utc = ensure_utc(start)
    end_utc = ensure_utc(end)
    out: list[dict[str, Any]] = []
    seen: set[tuple[dt.datetime, float]] = set()

    for entry in _walk_payload(raw_payload):
        ts = _extract_timestamp(entry)
        glucose = _extract_glucose(entry)
        if ts is None or glucose is None:
            continue
        if start_utc and ts < start_utc:
            continue
        if end_utc and ts > end_utc:
            continue
        key = (ts, glucose)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            common_point(
                timestamp=ts,
                glucose=glucose,
                trend=_extract_trend(entry),
                source="medtronic_carelink",
                raw=entry,
            )
        )

    out.sort(key=lambda row: row["timestamp"])
    return out


def test_connection(user) -> ProviderConnectionResult:
    cred = getattr(user, "carelink_credentials", None)
    if cred is None or not cred.username:
        return ProviderConnectionResult(
            ok=False,
            status="not_configured",
            message="Aucun compte CareLink configuré.",
            provider="medtronic_carelink",
        )

    try:
        data = fetch_glucose(
            user,
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24),
            dt.datetime.now(dt.timezone.utc),
            refresh_if_needed=True,
        )
    except CareLinkNeedsReauthError as exc:
        return ProviderConnectionResult(
            ok=False,
            status="needs_reauth",
            message=str(exc),
            provider="medtronic_carelink",
        )
    except requests.RequestException:
        return ProviderConnectionResult(
            ok=False,
            status="error",
            message="Impossible de joindre CareLink pour le moment.",
            provider="medtronic_carelink",
        )
    except Exception as exc:
        return ProviderConnectionResult(
            ok=False,
            status="error",
            message=f"Erreur CareLink: {exc}",
            provider="medtronic_carelink",
        )

    message = "Connexion CareLink vérifiée."
    if data:
        latest = data[-1]["timestamp"].astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        message = f"Connexion CareLink vérifiée ({len(data)} points, dernière synchro {latest})."
    return ProviderConnectionResult(
        ok=True,
        status="connected",
        message=message,
        provider="medtronic_carelink",
        last_sync_at=dt.datetime.utcnow(),
    )


def fetch_glucose(
    user,
    start: dt.datetime,
    end: dt.datetime,
    *,
    refresh_if_needed: bool = True,
) -> list[dict[str, Any]]:
    cred = getattr(user, "carelink_credentials", None)
    if cred is None:
        return []

    token_data = _parse_token_payload(cred)
    config = _load_config(cred.region, token_data["access_token"])

    if refresh_if_needed and cred.token_expires_at and cred.token_expires_at <= dt.datetime.utcnow():
        raise CareLinkNeedsReauthError(
            "La session CareLink doit être réauthentifiée via CareLink Connect."
        )

    user_profile = _load_user_profile(config, token_data)
    patient_id = cred.patient_id or _resolve_patient_id(config, token_data, user_profile.get("role", ""))
    if not patient_id and user_profile.get("role") in {"CARE_PARTNER", "CARE_PARTNER_OUS"}:
        raise CareLinkError("Aucun patient lié au compte CareLink.")

    payload = _fetch_recent_payload(
        config,
        token_data,
        user_profile.get("username") or cred.username,
        user_profile.get("role", ""),
        patient_id,
    )
    return normalize_carelink_payload(payload, start=start, end=end)


def refresh_tokens(user) -> None:
    cred = getattr(user, "carelink_credentials", None)
    if cred is None:
        raise CareLinkNeedsReauthError("Aucun compte CareLink configuré.")
    _refresh_tokens(cred)
