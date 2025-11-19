# scripts/rederive_activity.py
import argparse
from typing import Optional, List

from app.database import SessionLocal
from app import models
from app.logic import (
    compute_slope_series,
    compute_vertical_speed_series,
    get_hr_zone_label,
    compute_user_fc_max,
)
from app.logic import (
    compute_and_store_vam_peaks as logic_compute_and_store_vam_peaks,
    compute_and_store_zone_slope_aggs,
)

def rederive_for_activity(activity_id: int, user_id: int, default_sport: Optional[str] = "run") -> None:
    """
    Recalcule (en base) les champs dérivés manquants d'une activité EXISTANTE :
      - slope_percent
      - vertical_speed_m_per_h
      - hr_zone

    Puis calcule :
      - VAM peaks 5/15/30 (met à jour activities.max_vam_* + PR)
      - Aggs zone × pente

    Le tout SANS appeler Strava (uniquement à partir de activity_stream_points).
    """
    db = SessionLocal()
    try:
        activity = db.query(models.Activity).get(activity_id)
        if not activity:
            print(f"❌ Activity {activity_id} introuvable.")
            return

        # 0) sport manquant → on définit un défaut (run) pour activer les PR
        if not activity.sport and default_sport:
            activity.sport = default_sport
            db.add(activity)
            db.commit()
            print(f"ℹ️ sport était NULL → défini à '{default_sport}' pour activity_id={activity_id}")

        # 1) charger tous les points (ordre idx)
        pts = (
            db.query(models.ActivityStreamPoint)
              .filter(models.ActivityStreamPoint.activity_id == activity_id)
              .order_by(models.ActivityStreamPoint.idx.asc())
              .all()
        )
        if not pts or len(pts) < 2:
            print("❌ Pas assez de points dans activity_stream_points.")
            return

        # 2) préparer les séries brutes
        elapsed = [p.elapsed_time for p in pts]
        dist    = [p.distance     for p in pts]
        alt     = [p.altitude     for p in pts]
        hr      = [p.heartrate    for p in pts]

        # 3) calculer slope et VAM localement
        # slope nécessite distance+altitude, VAM nécessite elapsed+altitude
        slope = compute_slope_series(
            dist_stream=[float(x) if x is not None else None for x in dist],
            alt_stream =[float(x) if x is not None else None for x in alt],
            window=4,
            min_distance=10.0,
        )
        vam = compute_vertical_speed_series(
            elapsed_stream=[float(x) if x is not None else None for x in elapsed],
            alt_stream    =[float(x) if x is not None else None for x in alt],
            window_pts=5,
            min_dt=10.0,
            only_ascent=True,
            clamp_abs_mph=4000.0,
        )

        # 4) zones FC
        user = db.query(models.User).get(activity.user_id)
        fc_max = compute_user_fc_max(user)
        zones = [get_hr_zone_label(h, fc_max) for h in hr]

        # 5) mise à jour en base des champs dérivés
        updated = 0
        for i, p in enumerate(pts):
            changed = False
            new_slope = slope[i] if i < len(slope) else None
            new_vam   = vam[i]   if i < len(vam)   else None
            new_zone  = zones[i] if i < len(zones) else None

            if p.slope_percent != new_slope:
                p.slope_percent = new_slope
                changed = True
            if p.vertical_speed_m_per_h != new_vam:
                p.vertical_speed_m_per_h = new_vam
                changed = True
            if p.hr_zone != new_zone:
                p.hr_zone = new_zone
                changed = True

            if changed:
                db.add(p)
                updated += 1

        if updated:
            db.commit()
        print(f"🛠️ Dérivés mis à jour pour activity_id={activity_id} (rows touchées ≈ {updated}).")

        # 6) VAM peaks + aggs (implémentation robuste de logic.py)
        logic_compute_and_store_vam_peaks(db=db, activity=activity, user_id=user_id)
        compute_and_store_zone_slope_aggs(db=db, activity=activity, user_id=user_id)
        print("✅ VAM peaks + aggs pente×zone recalculés.")

    finally:
        db.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--activity", type=int, required=True)
    ap.add_argument("--user", type=int, default=1)
    ap.add_argument("--default-sport", type=str, default="run")
    args = ap.parse_args()
    rederive_for_activity(activity_id=args.activity, user_id=args.user, default_sport=args.default_sport)

if __name__ == "__main__":
    main()
