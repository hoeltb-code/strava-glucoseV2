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
import statistics
from collections import defaultdict
from typing import Dict, Iterable, Tuple

from sqlalchemy import func

from app.database import SessionLocal
from app import models
from app.logic import (
    compute_best_dplus_windows,
    SERIES_DISTANCE_TARGETS_M,
    SERIES_REPETITION_COUNTS,
    SERIES_LONG_DISTANCE_MIN_M,
    SERIES_MIN_PACE_S,
    SERIES_MAX_PACE_S,
    SERIES_MAX_STABILITY_RATIO,
    VOLUME_WINDOW_DAYS,
    LONG_RUN_LOOKBACK_DAYS,
    LONG_RUN_THRESHOLD_KM,
    RUN_VOLUME_SPORTS,
)


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


def _load_distance_time_stream(db: SessionLocal, activity_id: int) -> list[tuple[float, float]]:
    rows = (
        db.query(
            models.ActivityStreamPoint.distance,
            models.ActivityStreamPoint.elapsed_time,
        )
        .filter(models.ActivityStreamPoint.activity_id == activity_id)
        .order_by(models.ActivityStreamPoint.idx.asc())
        .all()
    )
    stream: list[tuple[float, float]] = []
    last_dist = None
    for row in rows:
        if row.distance is None or row.elapsed_time is None:
            continue
        dist = float(row.distance)
        elapsed = float(row.elapsed_time)
        if last_dist is not None and dist < last_dist - 1.0:
            continue
        stream.append((dist, elapsed))
        last_dist = dist
    return stream



def rebuild_runner_profile_monthly(db: SessionLocal, wipe_existing: bool = False) -> None:
    if wipe_existing:
        deleted = db.query(models.RunnerProfileMonthly).delete()
        print(f"Deleted {deleted} existing monthly rows.")
        db.commit()

    total_inserted = 0
    total_inserted += _backfill_slope_zone(db)
    total_inserted += _backfill_ascent_windows(db)
    total_inserted += _backfill_glucose_activity(db)
    total_inserted += _backfill_series_splits(db)
    total_inserted += _backfill_volume_weekly(db)

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
                    "name": act.name,
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


def _build_distance_segments(stream: list[tuple[float, float]], target_distance_m: float) -> list[dict]:
    if not stream or len(stream) < 2:
        return []

    total_distance = stream[-1][0]
    max_segments = int(total_distance // target_distance_m)
    if max_segments == 0:
        return []

    segments: list[dict] = []
    targets = [i * target_distance_m for i in range(max_segments + 1)]
    target_times = [None] * (max_segments + 1)
    target_times[0] = stream[0][1] if stream[0][0] <= 1e-3 else 0.0

    idx_target = 0
    prev_dist, prev_time = stream[0]

    for dist, time in stream[1:]:
        if dist < prev_dist:
            prev_dist, prev_time = dist, time
            continue

        while idx_target + 1 < len(targets) and targets[idx_target + 1] <= dist:
            target = targets[idx_target + 1]
            if dist == prev_dist:
                interpolated_time = time
            else:
                ratio = (target - prev_dist) / (dist - prev_dist)
                interpolated_time = prev_time + ratio * (time - prev_time)

            target_times[idx_target + 1] = interpolated_time
            start_time = target_times[idx_target]
            if start_time is not None:
                duration = interpolated_time - start_time
                if duration > 1.0:
                    pace = duration / (target_distance_m / 1000.0)
                    if SERIES_MIN_PACE_S <= pace <= SERIES_MAX_PACE_S:
                        segments.append(
                            {
                                "index": idx_target,
                                "start_time": start_time,
                                "end_time": interpolated_time,
                                "duration_sec": duration,
                                "pace_s_per_km": pace,
                            }
                        )
            idx_target += 1

        prev_dist = dist
        prev_time = time

        if idx_target + 1 >= len(targets):
            break

    return segments


def _extract_series_from_segments(segments: list[dict], distance_m: float) -> dict[int, dict]:
    if not segments:
        return {}

    segments = sorted(segments, key=lambda s: s["index"])
    best_by_reps: dict[int, dict] = {}

    reps_list = [1] + list(SERIES_REPETITION_COUNTS)

    for reps in reps_list:
        if len(segments) < reps:
            continue

        best_entry: dict | None = None
        window: list[dict] = []
        prev_index = None

        for seg in segments:
            if prev_index is not None and seg["index"] != prev_index + 1:
                window = []
            window.append(seg)
            prev_index = seg["index"]

            if len(window) > reps:
                window.pop(0)

            if len(window) == reps:
                pace_values = [s["pace_s_per_km"] for s in window]
                avg_pace = sum(pace_values) / reps
                if avg_pace <= 0:
                    continue

                if reps > 1:
                    std_dev = statistics.pstdev(pace_values)
                    stability = std_dev / avg_pace if avg_pace > 0 else None
                else:
                    stability = 0.0

                if stability is not None and stability > SERIES_MAX_STABILITY_RATIO:
                    continue

                duration = sum(s["duration_sec"] for s in window)
                entry = {
                    "distance_m": distance_m,
                    "reps": reps,
                    "avg_pace_s_per_km": avg_pace,
                    "stability_ratio": stability,
                    "duration_sec": duration,
                }

                if best_entry is None or avg_pace < best_entry["avg_pace_s_per_km"]:
                    best_entry = entry

        if best_entry:
            best_by_reps[reps] = best_entry

    return best_by_reps


def _backfill_series_splits(db: SessionLocal) -> int:
    inserted = 0
    for user_id, sport, month in _iter_user_sport_months(db):
        if sport != "run":
            continue

        month_start, month_end = _month_bounds(month)
        activities = (
            db.query(models.Activity)
            .filter(
                models.Activity.user_id == user_id,
                models.Activity.sport == sport,
                models.Activity.start_date >= month_start,
                models.Activity.start_date < month_end,
            )
            .order_by(models.Activity.start_date.asc())
            .all()
        )

        if not activities:
            continue

        for act in activities:
            stream = _load_distance_time_stream(db, act.id)
            if not stream or stream[-1][0] < min(SERIES_DISTANCE_TARGETS_M):
                continue

            for distance_m in SERIES_DISTANCE_TARGETS_M:
                segments = _build_distance_segments(stream, distance_m)
                if not segments:
                    continue
                series_by_reps = _extract_series_from_segments(segments, distance_m)
                for reps, data in series_by_reps.items():
                    duration = float(data.get("duration_sec") or 0.0)
                    avg_pace = float(data.get("avg_pace_s_per_km") or 0.0)
                    if duration <= 0 or avg_pace <= 0:
                        continue
                    rpm = models.RunnerProfileMonthly(
                        user_id=user_id,
                        sport=sport,
                        year_month=month,
                        metric_scope="series_splits",
                        slope_band=f"D{int(distance_m)}",
                        hr_zone=f"R{int(reps)}",
                        total_duration_sec=duration,
                        total_distance_m=distance_m * reps,
                        total_points=reps,
                        sum_pace_x_duration=avg_pace * duration,
                        pace_duration_sec=duration,
                        avg_pace_s_per_km=avg_pace,
                        extra={
                            "distance_m": distance_m,
                            "reps": reps,
                            "series_count": 1,
                            "samples": [
                                {
                                    "activity_id": act.id,
                                    "avg_pace_s_per_km": avg_pace,
                                    "duration_sec": duration,
                                    "stability_ratio": data.get("stability_ratio"),
                                    "date": _isoformat(act.start_date),
                                }
                            ],
                            "best_series": {
                                "avg_pace_s_per_km": avg_pace,
                                "activity_id": act.id,
                                "duration_sec": duration,
                                "stability_ratio": data.get("stability_ratio"),
                                "date": _isoformat(act.start_date),
                            },
                        },
                    )
                    db.add(rpm)
                    inserted += 1

    db.commit()
    print(f"Inserted {inserted} monthly aggregates (scope=series_splits).")
    return inserted


def _compute_recent_volume_metrics_backfill(
    db: SessionLocal,
    user_id: int,
    month_end: dt.datetime,
) -> dict:
    window_start = month_end - dt.timedelta(days=VOLUME_WINDOW_DAYS)
    activities = (
        db.query(models.Activity)
        .filter(models.Activity.user_id == user_id)
        .filter(models.Activity.sport.in_(RUN_VOLUME_SPORTS))
        .filter(models.Activity.start_date >= window_start)
        .filter(models.Activity.start_date <= month_end)
        .all()
    )

    if not activities:
        return {
            "weekly_volume_km": 0.0,
            "activities_count": 0,
            "long_run": None,
            "window_days": VOLUME_WINDOW_DAYS,
        }

    total_km = 0.0
    activities_count = 0
    best_long_run = None

    for act in activities:
        if act.distance is None:
            continue
        km = float(act.distance) / 1000.0
        total_km += km
        activities_count += 1
        if best_long_run is None or km > best_long_run["distance_km"]:
            best_long_run = {
                "distance_km": km,
                "date": _isoformat(act.start_date),
            }

    weekly_volume_km = total_km / (VOLUME_WINDOW_DAYS / 7.0)

    if best_long_run and best_long_run["distance_km"] < LONG_RUN_THRESHOLD_KM:
        best_long_run = None

    return {
        "weekly_volume_km": weekly_volume_km,
        "activities_count": activities_count,
        "long_run": best_long_run,
        "window_days": VOLUME_WINDOW_DAYS,
    }


def _backfill_volume_weekly(db: SessionLocal) -> int:
    inserted = 0
    processed_months: set[tuple[int, dt.date]] = set()
    for user_id, sport, month in _iter_user_sport_months(db):
        if sport not in RUN_VOLUME_SPORTS:
            continue
        key = (user_id, month)
        if key in processed_months:
            continue
        processed_months.add(key)
        _, month_end = _month_bounds(month)
        metrics = _compute_recent_volume_metrics_backfill(
            db,
            user_id=user_id,
            month_end=month_end,
        )
        entry = models.RunnerProfileMonthly(
            user_id=user_id,
            sport="run",
            year_month=month,
            metric_scope="volume_weekly",
            total_distance_m=metrics.get("weekly_volume_km", 0.0) * 1000.0,
            total_points=metrics.get("activities_count") or 0,
            extra=metrics,
        )
        db.add(entry)
        inserted += 1

    db.commit()
    print(f"Inserted {inserted} monthly aggregates (scope=volume_weekly).")
    return inserted



def main():
    db = SessionLocal()
    try:
        rebuild_runner_profile_monthly(db, wipe_existing=True)
    finally:
        db.close()


if __name__ == "__main__":
    main()
