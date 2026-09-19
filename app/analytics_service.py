"""Local background maintenance only: no calls to Strava or CGM providers."""
import datetime as dt
import logging
import threading
from .database import SessionLocal
from .models import Activity, ActivityStreamPoint, GlucosePoint, User
from .activity_analytics import build_summary

logger = logging.getLogger(__name__)
_maintenance_lock = threading.Lock()


def refresh_activity_summary(db, activity, user=None):
    user = user or db.get(User, activity.user_id)
    points = db.query(ActivityStreamPoint).filter(ActivityStreamPoint.activity_id == activity.id).order_by(ActivityStreamPoint.idx).all()
    start = activity.start_date
    end = start + dt.timedelta(seconds=activity.elapsed_time or 0)
    raw = db.query(GlucosePoint.ts, GlucosePoint.mgdl).filter(
        GlucosePoint.user_id == activity.user_id, GlucosePoint.ts >= start,
        GlucosePoint.ts <= end).order_by(GlucosePoint.ts).all()
    samples = [{"ts":p.ts,"mgdl":p.mgdl} for p in raw] if raw else None
    activity.analytics_summary = build_summary(activity, user, points, samples)
    metrics=activity.analytics_summary["glycemia"]
    activity.avg_glucose=metrics["avg"]
    activity.min_glucose=metrics["min"]
    activity.max_glucose=metrics["max"]
    activity.time_in_range_percent=metrics["pct_in_range"]
    db.add(activity)
    return activity.analytics_summary


def maintain_batch(batch_size=10):
    if not _maintenance_lock.acquire(blocking=False):
        return 0
    from .logic import (backfill_signed_vertical_speed_for_activity, compute_and_store_zone_slope_aggs,
                        update_runner_profile_monthly_from_activity)
    try:
        with SessionLocal() as db:
            ids=[row[0] for row in db.query(Activity.id).filter(Activity.analytics_summary.is_(None)).order_by(Activity.id).limit(batch_size)]
        # Late CGM arrivals: refresh recent summaries at most hourly, outside GET.
        if len(ids)<batch_size:
            with SessionLocal() as db:
                recent=db.query(Activity.id,Activity.analytics_summary).filter(Activity.start_date>=dt.datetime.utcnow()-dt.timedelta(days=2)).order_by(Activity.id.desc()).limit(100).all()
                for row in recent:
                    generated=(row.analytics_summary or {}).get("generated_at")
                    try:
                        stale=dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(generated)>dt.timedelta(hours=1)
                    except (ValueError,TypeError):
                        stale=True
                    if stale and row.id not in ids:
                        ids.append(row.id)
                    if len(ids)>=batch_size:break
        completed=0
        for activity_id in ids:
            with SessionLocal() as db:
                try:
                    activity=db.get(Activity,activity_id)
                    if activity is None:
                        continue
                    if backfill_signed_vertical_speed_for_activity(db,activity):
                        compute_and_store_zone_slope_aggs(db,activity,activity.user_id)
                    summary=refresh_activity_summary(db,activity)
                    update_runner_profile_monthly_from_activity(db=db,activity=activity,stats=summary["glycemia"])
                    db.commit()
                    completed+=1
                except Exception:
                    db.rollback()
                    logger.exception("Analytics maintenance failed activity=%s",activity_id)
        return completed
    finally:
        _maintenance_lock.release()


def maintenance_loop():
    stop=threading.Event()
    while not stop.wait(20):
        try:
            maintain_batch()
        except Exception:
            logger.exception("Analytics maintenance batch failed")
