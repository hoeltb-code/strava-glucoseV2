#!/usr/bin/env python
"""
Populate the runner_profile_monthly table with historical aggregates.

Current implementation focuses on the slope×zone cardio metrics that feed
the "Profil coureur" tabs (VAM / allure / cadence). The data is grouped
per user, sport and calendar month so that we can later aggregate any
time range (all-time, 12 derniers mois, etc.) without rescanning every
activity.
"""

from __future__ import annotations

import datetime as dt
from collections import defaultdict
from typing import Dict, Iterable, Tuple

from sqlalchemy import func

from app.database import SessionLocal
from app import models
from app.logic import compute_best_dplus_windows, build_fatigue_profile


def _month_start(ts: dt.datetime) -> dt.date:
    """Normalize a datetime into the first day of its month (UTC naïf)."""
    if ts.tzinfo is not None:
        ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return dt.date(ts.year, ts.month, 1)


def _next_month(month: dt.date) -> dt.date:
    if month.month == 12:
        return dt.date(month.year + 1, 1, 1)
    return dt.date(month.year, month.month + 1, 1)


def _month_bounds(month: dt.date) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime(month.year, month.month, 1)
    end_month = _next_month(month)
    end = dt.datetime(end_month.year, end_month.month, 1)
    return start, end


def _iter_user_sport_months(db: SessionLocal) -> Iterable[tuple[int, str, dt.date]]:
    rows = (
        db.query(
            models.Activity.user_id,
            models.Activity.sport,
            func.min(models.Activity.start_date),
            func.max(models.Activity.start_date),
        )
        .group_by(models.Activity.user_id, models.Activity.sport)
        .all()
    )

    for user_id, sport, start_at, end_at in rows:
        if not start_at or not end_at or not sport:
            continue
        month = _month_start(start_at)
        end_month = _month_start(end_at)
        while month <= end_month:
            yield user_id, sport, month
            month = _next_month(month)


def _isoformat(ts: dt.datetime | None) -> str | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.isoformat()


def _classify_activity_profile_local(zone_durations: dict[str, float]) -> str | None:
    total = sum(zone_durations.values())
    if total <= 0:
        return None

    z1 = zone_durations.get("Zone 1", 0.0)
    z2 = zone_durations.get("Zone 2", 0.0)
    z3 = zone_durations.get("Zone 3", 0.0)
    z4 = zone_durations.get("Zone 4", 0.0)
    z5 = zone_durations.get("Zone 5", 0.0)

    z5_ratio = z5 / total if total else 0.0
    if z5_ratio >= 0.12 or z5 >= 600:
        return "fractionne"

    threshold_ratio = (z3 + z4) / total if total else 0.0
    if threshold_ratio >= 0.5:
        return "seuil"

    endurance_ratio = (z1 + z2 + z3) / total if total else 0.0
    if endurance_ratio >= 0.45:
        return "endurance"

    return "seuil"


def _get_activity_zone_mix(db: SessionLocal, activity_id: int) -> dict[str, float]:
    rows = (
        db.query(
            models.ActivityZoneSlopeAgg.hr_zone,
            func.sum(models.ActivityZoneSlopeAgg.duration_sec),
        )
        .filter(models.ActivityZoneSlopeAgg.activity_id == activity_id)
        .group_by(models.ActivityZoneSlopeAgg.hr_zone)
        .all()
    )
    return {
        row[0]: float(row[1] or 0.0)
        for row in rows
        if row[0]
    }


def _get_activity_glucose_point(
    db: SessionLocal,
    activity_id: int,
    first: bool = True,
) -> float | None:
    order_clause = (
        models.ActivityStreamPoint.elapsed_time.asc()
        if first
        else models.ActivityStreamPoint.elapsed_time.desc()
    )
    row = (
        db.query(models.ActivityStreamPoint.glucose_mgdl)
        .filter(
            models.ActivityStreamPoint.activity_id == activity_id,
            models.ActivityStreamPoint.glucose_mgdl.isnot(None),
        )
        .order_by(order_clause)
        .first()
    )
    if not row:
        return None
    value = row[0] if isinstance(row, tuple) else row.glucose_mgdl
    return float(value) if value is not None else None


def rebuild_runner_profile_monthly(db: SessionLocal, wipe_existing: bool = False) -> None:
    """
    Recompute monthly aggregates for all activities currently stored.

    Remplit successivement :
      - slope_zone (pente × zone cardio)
      - ascent_window (fenêtres D+)
      - fatigue (allure par pente × durée)
    """
    if wipe_existing:
        deleted = db.query(models.RunnerProfileMonthly).delete()
        print(f"Deleted {deleted} existing monthly rows.")
        db.commit()

    total_inserted = 0
    total_inserted += _backfill_slope_zone(db)
    total_inserted += _backfill_ascent_windows(db)
    total_inserted += _backfill_fatigue(db)
    total_inserted += _backfill_glucose_activity(db)

    print(f"Total rows inserted: {total_inserted}")


def _backfill_slope_zone(db: SessionLocal) -> int:
    agg_map: Dict[
        Tuple[int, str, dt.date, str, str],
        Dict[str, float],
    ] = defaultdict(lambda: {
        "total_duration_sec": 0.0,
        "total_distance_m": 0.0,
        "total_elevation_gain_m": 0.0,
        "total_points": 0.0,
        "sum_pace_x_duration": 0.0,
        "pace_duration_sec": 0.0,
        "sum_vam_x_duration": 0.0,
        "vam_duration_sec": 0.0,
        "sum_cadence_x_duration": 0.0,
        "cadence_duration_sec": 0.0,
        "sum_velocity_x_duration": 0.0,
        "velocity_duration_sec": 0.0,
    })

    rows = (
        db.query(
            models.ActivityZoneSlopeAgg.user_id,
            models.ActivityZoneSlopeAgg.sport,
            models.ActivityZoneSlopeAgg.hr_zone,
            models.ActivityZoneSlopeAgg.slope_band,
            models.ActivityZoneSlopeAgg.duration_sec,
            models.ActivityZoneSlopeAgg.num_points,
            models.ActivityZoneSlopeAgg.distance_m,
            models.ActivityZoneSlopeAgg.elevation_gain_m,
            models.ActivityZoneSlopeAgg.avg_vam_m_per_h,
            models.ActivityZoneSlopeAgg.avg_cadence_spm,
            models.ActivityZoneSlopeAgg.avg_velocity_m_s,
            models.ActivityZoneSlopeAgg.avg_pace_s_per_km,
            models.Activity.start_date,
        )
        .join(models.Activity, models.ActivityZoneSlopeAgg.activity_id == models.Activity.id)
        .all()
    )

    print(f"Found {len(rows)} slope×zone rows to aggregate.")

    for row in rows:
        start_date = row.start_date
        if not start_date or not row.hr_zone or not row.slope_band:
            continue

        month = _month_start(start_date)
        key = (row.user_id, row.sport, month, row.hr_zone, row.slope_band)
        cell = agg_map[key]

        duration = float(row.duration_sec or 0.0)
        cell["total_duration_sec"] += duration
        cell["total_distance_m"] += float(row.distance_m or 0.0)
        cell["total_elevation_gain_m"] += float(row.elevation_gain_m or 0.0)
        cell["total_points"] += float(row.num_points or 0.0)

        if row.avg_pace_s_per_km is not None:
            cell["sum_pace_x_duration"] += float(row.avg_pace_s_per_km) * duration
            cell["pace_duration_sec"] += duration

        if row.avg_vam_m_per_h is not None:
            cell["sum_vam_x_duration"] += float(row.avg_vam_m_per_h) * duration
            cell["vam_duration_sec"] += duration

        if row.avg_cadence_spm is not None:
            cell["sum_cadence_x_duration"] += float(row.avg_cadence_spm) * duration
            cell["cadence_duration_sec"] += duration

        if row.avg_velocity_m_s is not None:
            cell["sum_velocity_x_duration"] += float(row.avg_velocity_m_s) * duration
            cell["velocity_duration_sec"] += duration

    created = 0
    for (user_id, sport, month, hr_zone, slope_band), data in agg_map.items():
        def _safe_avg(sum_value: float, weight: float) -> float | None:
            if weight <= 0:
                return None
            return sum_value / weight

        entry = models.RunnerProfileMonthly(
            user_id=user_id,
            sport=sport,
            year_month=month,
            metric_scope="slope_zone",
            hr_zone=hr_zone,
            slope_band=slope_band,
            total_duration_sec=data["total_duration_sec"],
            total_distance_m=data["total_distance_m"],
            total_elevation_gain_m=data["total_elevation_gain_m"],
            total_points=int(data["total_points"]),
            sum_pace_x_duration=data["sum_pace_x_duration"],
            pace_duration_sec=data["pace_duration_sec"],
            sum_vam_x_duration=data["sum_vam_x_duration"],
            vam_duration_sec=data["vam_duration_sec"],
            sum_cadence_x_duration=data["sum_cadence_x_duration"],
            cadence_duration_sec=data["cadence_duration_sec"],
            sum_velocity_x_duration=data["sum_velocity_x_duration"],
            velocity_duration_sec=data["velocity_duration_sec"],
            avg_pace_s_per_km=_safe_avg(
                data["sum_pace_x_duration"], data["pace_duration_sec"]
            ),
            avg_vam_m_per_h=_safe_avg(
                data["sum_vam_x_duration"], data["vam_duration_sec"]
            ),
            avg_cadence_spm=_safe_avg(
                data["sum_cadence_x_duration"], data["cadence_duration_sec"]
            ),
            avg_velocity_m_s=_safe_avg(
                data["sum_velocity_x_duration"], data["velocity_duration_sec"]
            ),
        )
        db.add(entry)
        created += 1

    db.commit()
    print(f"Inserted {created} monthly aggregates (scope=slope_zone).")
    return created


def _backfill_ascent_windows(db: SessionLocal) -> int:
    inserted = 0
    for user_id, sport, month in _iter_user_sport_months(db):
        month_start, month_end = _month_bounds(month)
        windows = compute_best_dplus_windows(
            db,
            user_id=user_id,
            sport=sport,
            date_from=month_start,
            date_to=month_end,
        )
        for win in windows:
            gain = win.get("gain_m") or 0.0
            if gain <= 0:
                continue
            entry = models.RunnerProfileMonthly(
                user_id=user_id,
                sport=sport,
                year_month=month,
                metric_scope="ascent_window",
                window_label=win.get("window_id"),
                total_duration_sec=float(win.get("duration_sec") or 0.0),
                total_distance_m=float(win.get("distance_km") or 0.0) * 1000.0,
                dplus_total_m=gain,
                dminus_total_m=win.get("loss_m") or 0.0,
                avg_vam_m_per_h=win.get("gain_per_hour"),
                extra={
                    "label": win.get("label"),
                    "activity_id": win.get("activity_id"),
                    "activity_name": win.get("activity_name"),
                    "start_datetime": _isoformat(win.get("start_datetime")),
                    "end_datetime": _isoformat(win.get("end_datetime")),
                    "start_offset_sec": win.get("start_offset_sec"),
                    "end_offset_sec": win.get("end_offset_sec"),
                },
            )
            db.add(entry)
            inserted += 1
    db.commit()
    print(f"Inserted {inserted} monthly aggregates (scope=ascent_window).")
    return inserted


def _backfill_fatigue(db: SessionLocal) -> int:
    inserted = 0
    for user_id, sport, month in _iter_user_sport_months(db):
        month_start, month_end = _month_bounds(month)
        profile = build_fatigue_profile(
            db,
            user_id=user_id,
            sport=sport,
            date_from=month_start,
            date_to=month_end,
            hr_zone=None,
        )
        by_slope = profile.get("by_slope") or {}
        for slope_band, buckets in by_slope.items():
            for bucket_id, stats in buckets.items():
                duration = float(stats.get("dur_for_pace") or 0.0)
                if duration <= 0 and not stats.get("avg_pace_s_per_km"):
                    continue
                entry = models.RunnerProfileMonthly(
                    user_id=user_id,
                    sport=sport,
                    year_month=month,
                    metric_scope="fatigue",
                    slope_band=slope_band,
                    fatigue_bucket=bucket_id,
                    total_duration_sec=duration,
                    sum_pace_x_duration=float(stats.get("sum_pace_x_dur") or 0.0),
                    pace_duration_sec=duration,
                    avg_pace_s_per_km=stats.get("avg_pace_s_per_km"),
                    extra={
                        "count": stats.get("count"),
                        "degradation_pct": stats.get("degradation_vs_short_pct"),
                    },
                )
                db.add(entry)
                inserted += 1
    db.commit()
    print(f"Inserted {inserted} monthly aggregates (scope=fatigue).")
    return inserted


def _backfill_glucose_activity(db: SessionLocal) -> int:
    inserted = 0
    for user_id, sport, month in _iter_user_sport_months(db):
        month_start, month_end = _month_bounds(month)
        activities = (
            db.query(models.Activity)
            .filter(
                models.Activity.user_id == user_id,
                models.Activity.sport == sport,
                models.Activity.start_date >= month_start,
                models.Activity.start_date < month_end,
                models.Activity.avg_glucose.isnot(None),
            )
            .order_by(models.Activity.start_date.asc())
            .all()
        )

        if not activities:
            continue

        activity_payload = []
        profile_sum = {"endurance": 0.0, "seuil": 0.0, "fractionne": 0.0}
        profile_count = {"endurance": 0, "seuil": 0, "fractionne": 0}
        total_duration = 0.0

        for act in activities:
            start_val = _get_activity_glucose_point(db, act.id, first=True)
            end_val = _get_activity_glucose_point(db, act.id, first=False)
            avg_val = float(act.avg_glucose) if act.avg_glucose is not None else None
            tir_pct = (
                float(act.time_in_range_percent)
                if act.time_in_range_percent is not None
                else None
            )

            zone_mix = _get_activity_zone_mix(db, act.id)
            profile_key = _classify_activity_profile_local(zone_mix) if zone_mix else None
            if profile_key and avg_val is not None:
                profile_sum[profile_key] += avg_val
                profile_count[profile_key] += 1

            activity_payload.append(
                {
                    "activity_id": act.id,
                    "start_mgdl": start_val,
                    "end_mgdl": end_val,
                    "avg_mgdl": avg_val,
                    "tir_percent": tir_pct,
                    "profile": profile_key,
                    "start_ts": _isoformat(act.start_date),
                    "distance_km": float(act.distance) / 1000.0 if act.distance else None,
                    "elevation_gain_m": float(act.total_elevation_gain)
                    if act.total_elevation_gain is not None
                    else None,
                    "duration_sec": act.elapsed_time,
                }
            )
            total_duration += float(act.elapsed_time or 0.0)

        if not activity_payload:
            continue

        entry = models.RunnerProfileMonthly(
            user_id=user_id,
            sport=sport,
            year_month=month,
            metric_scope="glucose_activity",
            total_duration_sec=total_duration,
            extra={
                "activities": activity_payload,
                "profile_stats": {
                    key: {
                        "sum_avg_mgdl": profile_sum[key],
                        "count": profile_count[key],
                    }
                    for key in profile_sum.keys()
                },
            },
        )
        db.add(entry)
        inserted += 1

    db.commit()
    print(f"Inserted {inserted} monthly aggregates (scope=glucose_activity).")
    return inserted




def main():
    db = SessionLocal()
    try:
        rebuild_runner_profile_monthly(db, wipe_existing=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
