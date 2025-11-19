# app/indicators/slope_cadence.py

import json
from typing import List
from app.models import ActivityStreamPoint


def build_slope_cadence_data(points: List[ActivityStreamPoint]):
    """
    Prépare les données pour le nuage de points pente (%) vs cadence,
    couleur = progression dans la distance (0 -> 1).

    Retourne:
      has_slope_scatter: bool
      slope_js: str (JSON)
      cadence_js: str (JSON)
      progress_js: str (JSON)
    """
    if not points:
        return False, "[]", "[]", "[]"

    slope_values = []
    cadence_values = []
    dist_values = []

    for p in points:
        slope_values.append(p.slope_percent if p.slope_percent is not None else None)

        # Strava renvoie la cadence par jambe -> on convertit en pas/min
        if p.cadence is not None:
            cadence_values.append(p.cadence * 2)
        else:
            cadence_values.append(None)

        dist_values.append(p.distance if p.distance is not None else None)


    max_dist = max((d for d in dist_values if d is not None), default=0.0)
    if max_dist <= 0:
        return False, "[]", "[]", "[]"

    valid_slope = []
    valid_cadence = []
    valid_progress = []

    for s, c, d in zip(slope_values, cadence_values, dist_values):
        if s is None or c is None or d is None:
            continue
        valid_slope.append(s)
        valid_cadence.append(c)
        valid_progress.append(d / max_dist)

    # on demande au moins quelques points pour que le scatter ait du sens
    if len(valid_slope) < 5:
        return False, "[]", "[]", "[]"

    return (
        True,
        json.dumps(valid_slope),
        json.dumps(valid_cadence),
        json.dumps(valid_progress),
    )
