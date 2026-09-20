"""Choose documented personal model cells, otherwise a privacy-safe cohort median."""
import math
from .pace_trend import fit, evaluate

MIN_PERSONAL_SECONDS = 300
MIN_PERSONAL_POINTS = 20
MIN_COHORT_RUNNERS = 8


def reference_model(profile, personal_model, cohort, slopes, zones, slope_centers=None):
    lookup, sources = {}, {}
    counts = {"personal": 0, "median": 0, "missing": 0}
    for slope in slopes:
        for zone in zones:
            cell = ((profile or {}).get("zones", {}).get(zone) or {}).get(slope) or {}
            pace = (personal_model.get(slope) or {}).get(zone)
            personal_ok = (isinstance(pace, (float,int)) and math.isfinite(pace) and pace > 0
                           and (cell.get("pace_duration_sec", cell.get("duration_sec")) or 0) >= MIN_PERSONAL_SECONDS
                           and (cell.get("num_points") or 0) >= MIN_PERSONAL_POINTS)
            median_cell = (((cohort or {}).get("zones") or {}).get(zone) or {}).get(slope) or {}
            median = median_cell.get("p50")
            if personal_ok:
                value, source = pace, "personal"
            elif (median_cell.get("count", 0) >= MIN_COHORT_RUNNERS and isinstance(median, (float,int))
                  and math.isfinite(median) and median > 0):
                value, source = median, "median"
            else:
                value, source = None, "missing"
            counts[source] += 1
            sources.setdefault(slope, {})[zone] = source
            if value is not None:
                lookup.setdefault(slope, {})[zone] = float(value)
    curves = {}
    if slope_centers:
        for zone in zones:
            points = []
            for slope in slopes:
                value = (lookup.get(slope) or {}).get(zone)
                if value is None or slope not in slope_centers:
                    continue
                cell = ((profile or {}).get("zones", {}).get(zone) or {}).get(slope) or {}
                seconds = cell.get("pace_duration_sec", cell.get("duration_sec")) or 0
                weight = min(3., math.sqrt(seconds / MIN_PERSONAL_SECONDS)) if sources[slope][zone] == "personal" else 1.
                points.append({"x": max(-45., min(45., slope_centers[slope])), "pace": value, "weight": weight})
            model = fit(points)
            if model:
                curves[zone] = model
                for slope in slopes:
                    if zone in lookup.get(slope, {}):
                        lookup[slope][zone] = evaluate(model, max(-45., min(45., slope_centers[slope])))
    return {"lookup": lookup, "curves": curves, "sources": sources, "counts": counts,
            "minimum_personal_seconds": MIN_PERSONAL_SECONDS, "minimum_personal_points": MIN_PERSONAL_POINTS,
            "minimum_runners": MIN_COHORT_RUNNERS, "version": "personal-or-cohort-trend-v2"}
