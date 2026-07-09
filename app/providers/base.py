from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Any, Iterable, Optional


CommonGlucosePoint = dict[str, Any]


class ProviderConnectionError(RuntimeError):
    pass


@dataclass
class ProviderConnectionResult:
    ok: bool
    status: str
    message: str
    provider: str
    last_sync_at: Optional[dt.datetime] = None
    details: Optional[dict[str, Any]] = None


def ensure_utc(ts: dt.datetime | str | None) -> dt.datetime | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        raw = ts.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            ts = dt.datetime.fromisoformat(raw)
        except ValueError:
            return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def common_point(
    *,
    timestamp: dt.datetime | str,
    glucose: int | float,
    source: str,
    unit: str = "mg/dL",
    trend: str | None = None,
    raw: Any = None,
) -> CommonGlucosePoint:
    ts = ensure_utc(timestamp)
    if ts is None:
        raise ValueError("timestamp invalide")
    return {
        "timestamp": ts,
        "glucose": float(glucose),
        "unit": unit,
        "trend": trend,
        "source": source,
        "raw": raw,
    }


def common_to_legacy_points(points: Iterable[CommonGlucosePoint]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for point in points:
        ts = ensure_utc(point.get("timestamp"))
        mgdl = point.get("glucose")
        if ts is None or mgdl is None:
            continue
        out.append(
            {
                "ts": ts,
                "mgdl": float(mgdl),
                "trend": point.get("trend"),
                "source": point.get("source"),
                "raw": point.get("raw"),
            }
        )
    out.sort(key=lambda row: row["ts"])
    return out


def legacy_to_common_points(
    points: Iterable[dict[str, Any]],
    *,
    source: str,
) -> list[CommonGlucosePoint]:
    out: list[CommonGlucosePoint] = []
    for point in points:
        ts = ensure_utc(point.get("ts"))
        mgdl = point.get("mgdl")
        if ts is None or mgdl is None:
            continue
        out.append(
            common_point(
                timestamp=ts,
                glucose=mgdl,
                trend=point.get("trend"),
                source=source,
                raw=point.get("raw"),
            )
        )
    out.sort(key=lambda row: row["timestamp"])
    return out
