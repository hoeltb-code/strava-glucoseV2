# app/dexcom_client.py
# -----------------------------------------------------------------------------
# Client Dexcom Share pour l'application "Strava x Glucose".
#
# Rôles :
#   - Vérifier des identifiants Dexcom Share par utilisateur.
#   - Stocker localement ces identifiants dans `dexcom_tokens`.
#   - Fournir `get_graph(start, end)` au même format que LibreLinkUp.
# -----------------------------------------------------------------------------

from __future__ import annotations

import datetime as dt
import importlib
import math
import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from .settings import settings
from .secrets import decrypt_secret
from app.models import DexcomToken


_DEXCOM_DATE_RE = re.compile(r"Date\((\d+)(?:[+-]\d{4})?\)")
LAST_DEXCOM_STATUS: dict[int, Tuple[str, str]] = {}


class DexcomClientError(RuntimeError):
    pass


def set_dexcom_status_flag(user_id: Optional[int], status: str, message: str):
    if user_id is None:
        return
    LAST_DEXCOM_STATUS[user_id] = (status, message)


def get_last_dexcom_status(user_id: int) -> Optional[Tuple[str, str]]:
    return LAST_DEXCOM_STATUS.get(user_id)


def has_dexcom_share_credentials(tokens: Any) -> bool:
    if tokens is None:
        return False
    if isinstance(tokens, (list, tuple, set)):
        return any(has_dexcom_share_credentials(token) for token in tokens)
    return bool(
        getattr(tokens, "share_username", None)
        and getattr(tokens, "share_password", None)
    )


def _normalize_share_region(region: Optional[str]) -> str:
    value = (region or settings.DEXCOM_SHARE_REGION_DEFAULT or "ous").strip().lower()
    aliases = {
        "fr": "ous",
        "eu": "ous",
        "de": "ous",
        "it": "ous",
        "gb": "ous",
        "uk": "ous",
        "ous": "ous",
        "us": "us",
        "usa": "us",
        "jp": "jp",
        "japan": "jp",
    }
    return aliases.get(value, "ous")


def _load_pydexcom():
    try:
        module = importlib.import_module("pydexcom")
    except ModuleNotFoundError as exc:
        raise DexcomClientError(
            "Le package pydexcom n'est pas installé. Mets à jour les dépendances du projet."
        ) from exc
    const_module = importlib.import_module("pydexcom.const")
    return getattr(module, "Dexcom"), getattr(const_module, "Region", None)


def _build_share_client(username: str, password: str, region: Optional[str]):
    dexcom_cls, region_enum = _load_pydexcom()
    normalized_region = _normalize_share_region(region)
    region_value = region_enum(normalized_region) if region_enum is not None else normalized_region
    return dexcom_cls(
        username=username.strip(),
        password=password,
        region=region_value,
    )


def _classify_dexcom_error(exc: Exception) -> Tuple[str, str]:
    text = str(exc or "").strip().lower()
    if "failed_authentication" in text or "invalid password" in text:
        return "error", "Identifiants Dexcom Share invalides."
    if "max_attempts" in text:
        return "error", "Trop de tentatives de connexion Dexcom. Réessaie plus tard."
    if "region_invalid" in text:
        return "error", "Région Dexcom Share invalide."
    if "username_invalid" in text or "user_id_required" in text:
        return "error", "Identifiant Dexcom invalide."
    if "password_invalid" in text:
        return "error", "Mot de passe Dexcom invalide."
    return "error", f"Impossible de contacter Dexcom Share : {exc}"


def test_dexcom_credentials(
    username: str,
    password: str,
    region: str = "ous",
    *,
    user_id: Optional[int] = None,
) -> Tuple[str, str]:
    try:
        client = _build_share_client(username=username, password=password, region=region)
        reading = client.get_current_glucose_reading()
    except Exception as exc:  # noqa: BLE001
        status, msg = _classify_dexcom_error(exc)
        set_dexcom_status_flag(user_id, status, msg)
        return status, msg

    if reading is None:
        msg = (
            "Connexion Dexcom Share vérifiée, mais aucune valeur récente n'est disponible. "
            "Vérifie qu'un capteur est actif et qu'au moins un follower est configuré."
        )
        set_dexcom_status_flag(user_id, "warn", msg)
        return "warn", msg

    value = getattr(reading, "value", None)
    trend = getattr(reading, "trend_direction", None) or getattr(reading, "trend_description", None)
    msg = "Connexion Dexcom Share vérifiée"
    if value is not None:
        msg += f" ({value} mg/dL"
        if trend:
            msg += f", tendance {trend}"
        msg += ")."
    else:
        msg += "."
    set_dexcom_status_flag(user_id, "ok", msg)
    return "ok", msg


class DexcomClient:
    def __init__(self, user_id: int, db: Session):
        self.user_id = user_id
        self.db = db

    def _get_token_record(self) -> Optional[DexcomToken]:
        return (
            self.db.query(DexcomToken)
            .filter(DexcomToken.user_id == self.user_id)
            .order_by(DexcomToken.id.desc())
            .first()
        )

    def _get_share_credentials(self) -> Optional[DexcomToken]:
        token = self._get_token_record()
        if not token or not has_dexcom_share_credentials(token):
            return None
        return token

    @staticmethod
    def _parse_timestamp(payload: Dict[str, Any], fallback: Any) -> Optional[dt.datetime]:
        for key in ("WT", "ST", "DT"):
            raw = payload.get(key)
            if isinstance(raw, str):
                match = _DEXCOM_DATE_RE.search(raw)
                if match:
                    return dt.datetime.fromtimestamp(int(match.group(1)) / 1000, tz=dt.timezone.utc)

        if isinstance(fallback, dt.datetime):
            return (
                fallback.astimezone(dt.timezone.utc)
                if fallback.tzinfo is not None
                else fallback.replace(tzinfo=dt.timezone.utc)
            )
        return None

    @classmethod
    def _reading_to_point(cls, reading: Any) -> Optional[Dict[str, Any]]:
        payload = getattr(reading, "json", None)
        if not isinstance(payload, dict):
            payload = {}

        ts = cls._parse_timestamp(payload, getattr(reading, "datetime", None))

        value = getattr(reading, "mg_dl", None)
        if value is None:
            value = getattr(reading, "value", None)
        if value is None:
            value = payload.get("Value")

        if ts is None or value is None:
            return None

        trend = getattr(reading, "trend_direction", None) or payload.get("Trend")
        return {"ts": ts, "mgdl": int(value), "trend": trend}

    def get_graph(self, start: dt.datetime, end: dt.datetime) -> List[Dict[str, Any]]:
        credentials = self._get_share_credentials()
        if not credentials:
            print(f"[Dexcom] Aucun identifiant Dexcom Share pour user_id={self.user_id}")
            return []

        start = start.replace(tzinfo=dt.timezone.utc) if start.tzinfo is None else start.astimezone(dt.timezone.utc)
        end = end.replace(tzinfo=dt.timezone.utc) if end.tzinfo is None else end.astimezone(dt.timezone.utc)
        if end <= start:
            return []

        minutes = max(1, min(1440, int(math.ceil((end - start).total_seconds() / 60)) + 5))
        max_count = max(1, min(288, int(math.ceil(minutes / 5)) + 12))

        try:
            client = _build_share_client(
                username=credentials.share_username or "",
                password=decrypt_secret(credentials.share_password) or "",
                region=credentials.share_region,
            )
            readings = client.get_glucose_readings(minutes=minutes, max_count=max_count)
        except Exception as exc:  # noqa: BLE001
            print(f"[Dexcom] Erreur Dexcom Share pour user_id={self.user_id}: {exc}")
            return []

        out: List[Dict[str, Any]] = []
        for reading in readings:
            point = self._reading_to_point(reading)
            if point is None:
                continue
            if start <= point["ts"] <= end:
                out.append(point)

        out.sort(key=lambda row: row["ts"])
        return out
