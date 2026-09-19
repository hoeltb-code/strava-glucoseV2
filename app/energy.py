"""Transparent locomotion estimates, independent of any nutrition prescription.

Minetti 2002: net J/kg/metre on slopes -45..45%, DOI 10.1152/japplphysiol.01177.2001.
Keytel 2005: separate HR estimate (kJ/min), DOI 10.1080/02640410470001730089.
The two estimates must never be added or silently blended.
"""
import math

MODEL_VERSION = "minetti-2002-v1"


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def terrain_energy(weight_kg, grade_percent, pace_seconds_km, distance_km=1, gait="run"):
    weight, grade, pace, distance = map(number, (weight_kg, grade_percent, pace_seconds_km, distance_km))
    if (weight is None or not 20 <= weight <= 300 or grade is None or not -45 <= grade <= 45
            or pace is None or pace <= 0 or distance is None or distance < 0 or gait not in {"run", "walk"}):
        return None
    g = grade / 100
    # Walking polynomial represents the minimum walking cost (not all walking speeds).
    cost = (280.5*g**5 - 58.7*g**4 - 76.8*g**3 + 51.9*g**2 + 19.6*g + 2.5) if gait == "walk" else (
        155.4*g**5 - 30.4*g**4 - 43.3*g**3 + 46.3*g**2 + 19.5*g + 3.6)
    per_km = cost * weight * 1000 / 4184
    return {"kcal": per_km * distance, "kcal_km": per_km, "kcal_hour": per_km * 3600 / pace,
            "flat_ratio": cost / (2.5 if gait == "walk" else 3.6), "model": MODEL_VERSION, "gait": gait}


def heart_rate_energy(weight_kg, heart_rate, age, sex, max_hr=None):
    """Separate adult submaximal HR estimate; refuse outside study population/range."""
    weight, hr, years, maximum = map(number, (weight_kg, heart_rate, age, max_hr))
    if (weight is None or not 47 <= weight <= 120 or years is None or not 18 <= years <= 45
            or hr is None or maximum is None or maximum <= 0 or not .57 <= hr / maximum <= .90):
        return None
    if str(sex).lower() in {"m", "male", "homme"}:
        kj_min = -55.0969 + .6309*hr + .1988*weight + .2017*years
    elif str(sex).lower() in {"f", "female", "femme"}:
        kj_min = -20.4022 + .4472*hr - .1263*weight + .074*years
    else:
        return None
    return kj_min / 4.184 * 60 if kj_min > 0 else None


def activity_energy(points, weight_kg, *, sport="run", max_hr=None, duration_seconds=None, terrain_threshold=None):
    empty = {"available": False, "kcal": None, "coverage_percent": None, "terrain": [], "model": MODEL_VERSION}
    if sport not in {"run", "hike", "walk"} or number(weight_kg) is None:
        return empty
    def get(p, name):
        return p.get(name) if isinstance(p, dict) else getattr(p, name, None)
    totals = {key: {"key": key, "label": label, "kcal": 0., "seconds": 0., "distance_km": 0.,
                   "hr_sum": 0., "hr_seconds": 0., "grade_sum": 0.}
              for key, label in [("descent", "Descente"), ("rolling", "Roulant"), ("climb", "Montée")]}
    valid_seconds = 0.
    ordered = sorted([p for p in points if number(get(p, "elapsed_time")) is not None], key=lambda p: float(get(p, "elapsed_time")))
    duration = float(get(ordered[-1], "elapsed_time")) - float(get(ordered[0], "elapsed_time")) if len(ordered) > 1 else 0
    if number(duration_seconds) is not None:
        duration = max(duration, float(duration_seconds))
    for a, b in zip(ordered, ordered[1:]):
        seconds = float(get(b, "elapsed_time")) - float(get(a, "elapsed_time"))
        da, db = number(get(a, "distance")), number(get(b, "distance"))
        grade = number(get(b, "slope_percent"))
        if grade is None:
            grade = number(get(b, "grade"))
        if (not 0 < seconds <= 30 or da is None or db is None or not 0 < (db-da)/seconds <= 12
                or grade is None or get(a, "moving") is False or get(b, "moving") is False):
            continue
        distance = (db-da)/1000
        estimate = terrain_energy(weight_kg, grade, seconds/distance, distance, "run" if sport == "run" else "walk")
        if not estimate:
            continue
        uphill = grade >= terrain_threshold if terrain_threshold is not None else grade > 3
        downhill = grade <= -terrain_threshold if terrain_threshold is not None else grade < -3
        key = "climb" if uphill else "descent" if downhill else "rolling"
        row = totals[key]
        row["kcal"] += estimate["kcal"]
        row["seconds"] += seconds
        row["distance_km"] += distance
        row["grade_sum"] += grade * distance
        valid_seconds += seconds
        hr = number(get(b, "heartrate"))
        if hr is not None and 30 <= hr <= 240:
            row["hr_sum"] += hr * seconds
            row["hr_seconds"] += seconds
    total = sum(row["kcal"] for row in totals.values())
    for row in totals.values():
        row["percent"] = 100 * row["kcal"] / total if total else 0
        row["pace"] = row["seconds"] / row["distance_km"] if row["distance_km"] else None
        row["grade"] = row.pop("grade_sum") / row["distance_km"] if row["distance_km"] else None
        row["heart_rate"] = row.pop("hr_sum") / row["hr_seconds"] if row["hr_seconds"] else None
        row["hr_percent"] = 100 * row["heart_rate"] / max_hr if row["heart_rate"] and max_hr else None
    return {"available": valid_seconds > 0, "kcal": total if valid_seconds else None,
            "coverage_percent": 100 * valid_seconds / duration if duration else None,
            "observed_seconds": valid_seconds, "kcal_hour": total / valid_seconds * 3600 if valid_seconds else None,
            "terrain": list(totals.values()), "model": MODEL_VERSION, "weight_kg": weight_kg}


def strava_energy_block(summary, style="compact"):
    if not summary.get("available"):
        return ""
    coverage = summary.get("coverage_percent") or 0
    title = f"🔥 Énergie estimée : {round(summary['kcal'])} kcal · terrain couvert {round(coverage)} %"
    if style == "compact":
        return title
    lines = [title]
    for row in summary.get("terrain", []):
        if not row["distance_km"]:
            continue
        pace = round(row["pace"])
        hr = f" · FC {round(row['heart_rate'])} bpm" if row.get("heart_rate") else ""
        if row.get("hr_percent"):
            hr += f" ({round(row['hr_percent'])} % max)"
        lines.append(f"{'↗' if row['key']=='climb' else '↘' if row['key']=='descent' else '→'} {row['label']} {row['grade']:+.0f} % · {pace//60}:{pace%60:02d}/km{hr} · ≈ {round(row['kcal'])} kcal")
    lines.append("Dépense de locomotion estimée, pas un objectif d’apport alimentaire.")
    return "\n".join(lines)
