# app/logic.py
# -----------------------------------------------------------------------------
# Ce module gère la logique de traitement et d’analyse des données issues de Strava
# et des capteurs CGM (LibreLinkUp, Dexcom, etc.), transmises par `main.py`.
#
# Il ne récupère aucune donnée directement : il reçoit les activités Strava
# et les courbes de glycémie déjà chargées dans `main.py`, puis :
#
# 🔹 Filtre les points de glycémie dans la fenêtre temporelle d’une activité.
# 🔹 Calcule les statistiques glycémiques (moyenne, min, max, temps dans la cible, hypos/hypers)
#    et génère une barre visuelle (🟥🟩🟨).
# 🔹 Calcule les zones de fréquence cardiaque et associe la glycémie moyenne à chaque zone.
# 🔹 Fusionne ces informations dans la description Strava, sans dépasser la limite de texte.
# 🔹 Met à jour ou crée les enregistrements en base (`Activity`, `ActivityStreamPoint`).
# 🔹 Aligne les points de glycémie sur la timeline Strava pour les stocker
#    définitivement dans `activity_stream_points`.
#
# En résumé : `logic.py` reçoit les données préparées par `main.py`, effectue
# tous les calculs et met en forme les résultats avant leur enregistrement et
# leur publication sur Strava.
# -----------------------------------------------------------------------------
# app/logic.py
# -----------------------------------------------------------------------------
# Ce module gère la logique de traitement et d’analyse des données issues de Strava
# et des capteurs CGM (LibreLinkUp, Dexcom, etc.), transmises par `main.py`.
# -----------------------------------------------------------------------------
import datetime as dt
import hashlib
import bisect
from typing import Optional, List

from sqlalchemy.orm import Session
from app import models

from collections import defaultdict

# --- helper pour timestamps naïfs -> UTC aware
def _safe_dt(ts):
    return ts if (ts is None or ts.tzinfo is not None) else ts.replace(tzinfo=dt.timezone.utc)


TARGET_MIN = 70
TARGET_MAX = 180

# Zones cardio en % de FC max
HR_ZONES = [
    ("Zone 1", 0.10, 0.60),
    ("Zone 2", 0.60, 0.70),
    ("Zone 3", 0.70, 0.80),
    ("Zone 4", 0.80, 0.90),
    ("Zone 5", 0.90, 1.00),
]

# FC max par défaut si on n'a aucune info utilisateur
DEFAULT_FC_MAX = 180


def normalize_activity_type(strava_type: Optional[str]) -> str:
    """
    Normalise le type d’activité Strava pour nos stats:
    - Regroupe TrailRun avec Run -> 'run'
    - Quelques regroupements utiles côté vélo/ski
    """
    if not strava_type:
        return "other"
    t = strava_type.strip().lower()

    # course à pied (trail inclus)
    if t in {"run", "trailrun", "trail run"}:
        return "run"

    # vélo
    if t in {"ride", "ebikeride", "gravelride", "virtualride"}:
        return "ride"
    if t in {"mountainbike", "mtb"}:
        return "ride"  # ou 'mtb' si tu veux distinguer

    # marche / rando
    if t in {"hike", "walk"}:
        return "hike"

    # ski
    if t in {"alpineski", "backcountryski"}:
        return "ski_alpine"
    if t in {"nordicski", "rollerski"}:
        return "ski_nordic"

    return t

# ---------------------------------------------------------------------------
# Buckets de durée pour le "profil fatigue"
# ---------------------------------------------------------------------------

# (id, label, min_sec, max_sec)
DURATION_BUCKETS = [
    ("short",  "< 2 h",       0,          2 * 3600),   # 0 à 2h
    ("medium", "2–5 h",       2 * 3600,   5 * 3600),   # 2 à 5h
    ("long",   "5–10 h",      5 * 3600,   10 * 3600),  # 5 à 10h
    ("ultra",  "10 h+",       10 * 3600,  None),       # 10h et plus
]


def _classify_duration_bucket(elapsed_sec: int | None) -> str | None:
    """
    Retourne l'identifiant du bucket de durée correspondant (short/medium/long/ultra)
    ou None si la durée est invalide.
    """
    if elapsed_sec is None or elapsed_sec <= 0:
        return None

    for bucket_id, _label, mn, mx in DURATION_BUCKETS:
        if mx is None:
            # dernier bucket = [mn, +inf)
            if elapsed_sec >= mn:
                return bucket_id
        else:
            if mn <= elapsed_sec < mx:
                return bucket_id

    return None


# ---------------------------------------------------------------------------
#- Calcul de la FC max à utiliser pour les zones cardio
#---------------------------------------------------------------------------
def compute_user_fc_max(user: Optional[models.User]) -> int:
    """
    Détermine la FC max à utiliser pour les calculs de zones :

    1) Si user.max_heartrate est renseigné -> on l'utilise.
    2) Sinon, si user.birthdate est connue -> FC max = âge - 20.
    3) Sinon -> 180 par défaut.
    """
    if user is None:
        return DEFAULT_FC_MAX

    if getattr(user, "max_heartrate", None):
        try:
            return int(user.max_heartrate)
        except (TypeError, ValueError):
            pass

    birthdate = getattr(user, "birthdate", None)
    if isinstance(birthdate, dt.date):
        today = dt.date.today()
        age = (
            today.year
            - birthdate.year
            - ((today.month, today.day) < (birthdate.month, birthdate.day))
        )
        if age > 0:
            return max(100, age - 20)

    return DEFAULT_FC_MAX

# ---------------------------------------------------------------------------
# Difficulté & niveau d'une activité (distance + D+)
# ---------------------------------------------------------------------------

def compute_difficulty_and_level(
    distance_m: float | None,
    elevation_m: float | None,
    sport: str | None,
) -> tuple[float | None, int | None]:
    """
    Calcule un score de difficulté et un niveau 1-5 (Vert, Bleu, Rouge, Noir, Or)
    pour les activités de course à pied ("run").

    score = distance_km + D+ / 100

    Niveaux :
      1 (Vert)  : score < 20
      2 (Bleu)  : 20 ≤ score < 40
      3 (Rouge) : 40 ≤ score < 80
      4 (Noir)  : 80 ≤ score < 180
      5 (Or)    : score ≥ 180
    """
    if sport != "run":
        return None, None

    if not distance_m and not elevation_m:
        return None, None

    km = (distance_m or 0.0) / 1000.0
    dplus = (elevation_m or 0.0)

    score = km + dplus / 100.0

    if score < 20:
        level = 1
    elif score < 40:
        level = 2
    elif score < 80:
        level = 3
    elif score < 180:
        level = 4
    else:
        level = 5

    return score, level

#------------------------------------------------------------------------------
# Sélectionne les points dans une fenêtre temporelle avec buffer
#------------------------------------------------------------------------------

def select_window(points, start: dt.datetime, end: dt.datetime, buffer_min: int = 5):
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=dt.timezone.utc)
    buf = dt.timedelta(minutes=buffer_min)
    lo, hi = start - buf, end + buf
    return [p for p in points if lo <= p["ts"] <= hi]

def _build_bar(pct_hypo: float, pct_in_range: float, pct_hyper: float) -> str:
    blocks_hypo = round(pct_hypo / 10)
    blocks_range = round(pct_in_range / 10)
    blocks_hyper = max(0, 10 - blocks_hypo - blocks_range)
    bar = "🟥" * blocks_hypo + "🟩" * blocks_range + "🟨" * blocks_hyper
    bar = bar[:10]
    if len(bar) < 10:
        bar = bar.ljust(10, "⬜")
    return bar

#----------------------------------------------------------------------------
# Calcul des stats glycémiques
#----------------------------------------------------------------------------
def compute_stats(samples):
    if not samples:
        return None

    values = [s["mgdl"] for s in samples if s.get("mgdl") is not None]
    if len(values) < 2:
        return None

    n = len(values)
    vmin = min(values)
    vmax = max(values)
    avg = sum(values) / n

    nb_hypo = sum(1 for v in values if v < TARGET_MIN)
    nb_in_range = sum(1 for v in values if TARGET_MIN <= v <= TARGET_MAX)
    nb_hyper = sum(1 for v in values if v > TARGET_MAX)

    pct_hypo = (nb_hypo / n) * 100
    pct_in_range = (nb_in_range / n) * 100
    pct_hyper = (nb_hyper / n) * 100

    bar = _build_bar(pct_hypo, pct_in_range, pct_hyper)

    avg_r = round(avg)
    pct_zone_r = round(pct_in_range)

    lines = []
    lines.append(f"🔬Glycémie : Moy : {avg_r} mg/dL | {TARGET_MIN}-{TARGET_MAX} : {pct_zone_r}%")
    lines.append(bar)
    lines.append(f"Max : {vmax} mg/dL | Min : {vmin} mg/dL")
    block = "\n".join(lines)

    h = hashlib.sha1(block.encode()).hexdigest()

    return {
        "avg": avg,
        "min": vmin,
        "max": vmax,
        "pct_in_range": pct_in_range,
        "pct_hypo": pct_hypo,
        "pct_hyper": pct_hyper,
        "n": n,
        "nb_hypo": nb_hypo,
        "nb_in_range": nb_in_range,
        "nb_hyper": nb_hyper,
        "bar": bar,
        "block": block,
        "hash": h,
    }

#---------------------------------------------------------------------------
# Calcul des zones cardio + stats glycémiques par zone
#---------------------------------------------------------------------------
def _format_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:01d}:{m:02d}:{s:02d}"
    else:
        return f"{m:02d}:{s:02d}"

def compute_hr_zones(
    samples,
    activity_start: dt.datetime,
    time_stream: list[int],
    hr_stream: list[float],
    fc_max: int,
):
    if not time_stream or not hr_stream:
        return None

    n = min(len(time_stream), len(hr_stream))
    if n < 2:
        return None

    if fc_max is None or fc_max <= 0:
        fc_max = 180

    zones_data = []
    for name, lo_r, hi_r in HR_ZONES:
        lo = lo_r * fc_max
        hi = hi_r * fc_max
        zones_data.append(
            {
                "name": name,
                "lo": lo,
                "hi": hi,
                "duration_sec": 0.0,
                "gly_sum": 0.0,
                "gly_count": 0,
            }
        )

    samples_sorted = sorted(samples, key=lambda s: s["ts"])
    j = 0

    def get_zone_index(hr: float) -> int | None:
        for idx, z in enumerate(zones_data):
            if z["lo"] <= hr < z["hi"]:
                return idx
        if hr >= zones_data[-1]["lo"]:
            return len(zones_data) - 1
        return None

    for i in range(n - 1):
        t0_rel = time_stream[i]
        t1_rel = time_stream[i + 1]
        hr = hr_stream[i]
        if hr is None:
            continue

        dt_sec = t1_rel - t0_rel
        if dt_sec <= 0:
            continue

        zone_idx = get_zone_index(hr)
        if zone_idx is None:
            continue

        t0_abs = activity_start + dt.timedelta(seconds=int(t0_rel))

        while j + 1 < len(samples_sorted) and samples_sorted[j + 1]["ts"] <= t0_abs:
            j += 1

        mg = None
        if samples_sorted[j]["ts"] <= t0_abs:
            mg = samples_sorted[j]["mgdl"]

        z = zones_data[zone_idx]
        z["duration_sec"] += dt_sec
        if mg is not None:
            z["gly_sum"] += mg
            z["gly_count"] += 1

    for z in zones_data:
        if z["gly_count"] > 0:
            z["gly_avg"] = z["gly_sum"] / z["gly_count"]
        else:
            z["gly_avg"] = None

    return zones_data

#---------------------------------------------------------------------------
# Fusion de la description Strava avec le bloc glycémie
#---------------------------------------------------------------------------

def merge_desc(existing: str, block: str) -> str:
    """
    Remplace proprement tout ancien bloc auto (glycémie/VAM/allure/cadence/cardio/zones)
    sans utiliser de balises visibles. Si rien à nettoyer, on ajoute à la fin.
    """
    existing = (existing or "").strip()

    # Marqueurs possibles d'un ancien bloc auto
    markers = [
        "🔬Glycémie :",                 # ancien entête gly
        "🔬 Glycémie (LibreLinkUp)",    # tout premier format historique
        "⛰️ VAM :",                     # entête VAM
        "🏃 Allure",                    # entête allure
        "🔁 Cadence",                   # entête cadence
        "Cardio moy",                   # bloc cardio
        "Zone 1 :", "Zone 2 :", "Zone 3 :", "Zone 4 :", "Zone 5 :",  # zones FC
        "—> Made with ❤️ by Benoit",    # signature historique
    ]

    # Cherche le premier marqueur (le plus tôt dans le texte)
    cut_positions = []
    for m in markers:
        pos = existing.find(m)
        if pos != -1:
            cut_positions.append(pos)

    if cut_positions:
        cut = min(cut_positions)
        base_clean = existing[:cut].rstrip()
    else:
        base_clean = existing

    # Fusion finale (sans balises)
    if base_clean:
        merged = f"{base_clean}\n\n{block}"
    else:
        merged = block

    # Sécurité Strava (~2000). On garde 1800 pour marge.
    return merged[:1800]

#---------------------------------------------------------------------------
# Enregistrement / mise à jour d’une activité Strava en base
#---------------------------------------------------------------------------

def upsert_activity_record(
    db: Session,
    *,
    user_id: int,
    athlete_id: int,
    strava_activity: dict,
    glucose_stats: dict | None,
    summary_block: str | None = None,
):
    strava_activity_id = strava_activity["id"]
    activity_name = strava_activity.get("name")

    # NEW: type brut Strava (selon l’API, 'type' ou 'sport_type')
    raw_type = strava_activity.get("type") or strava_activity.get("sport_type")
    sport = normalize_activity_type(raw_type)

    start_date_raw = strava_activity.get("start_date")
    if start_date_raw:
        start_date = dt.datetime.fromisoformat(start_date_raw.replace("Z", "+00:00"))
    else:
        start_date = dt.datetime.now(dt.timezone.utc)

    elapsed_time = strava_activity.get("elapsed_time")
    average_heartrate = strava_activity.get("average_heartrate")
    distance = strava_activity.get("distance")
    total_elevation_gain = strava_activity.get("total_elevation_gain")
    # Calcul difficulté & niveau (uniquement pour la course à pied)
    difficulty_score, level = compute_difficulty_and_level(
        distance_m=distance,
        elevation_m=total_elevation_gain,
        sport=sport,
    )

    activity = (
        db.query(models.Activity)
        .filter(
            models.Activity.user_id == user_id,
            models.Activity.strava_activity_id == strava_activity_id,
        )
        .one_or_none()
    )

    now_utc = dt.datetime.now(dt.timezone.utc)

    if activity is None:
        activity = models.Activity(
            user_id=user_id,
            athlete_id=athlete_id,
            strava_activity_id=strava_activity_id,
            name=activity_name,
            activity_type=raw_type,
            sport=sport,
            start_date=start_date,
            elapsed_time=elapsed_time,
            average_heartrate=average_heartrate,
            distance=distance,
            total_elevation_gain=total_elevation_gain,
            # 💡 nouveaux champs
            difficulty_score=difficulty_score,
            level=level,
            created_at=now_utc,
        )
        db.add(activity)
    else:
        activity.name = activity_name
        activity.activity_type = raw_type
        activity.sport = sport
        activity.start_date = start_date
        activity.elapsed_time = elapsed_time
        activity.average_heartrate = average_heartrate
        activity.distance = distance
        activity.total_elevation_gain = total_elevation_gain
        # 💡 mise à jour difficulté + niveau
        activity.difficulty_score = difficulty_score
        activity.level = level

    if glucose_stats:
        activity.avg_glucose = glucose_stats.get("avg")
        activity.min_glucose = glucose_stats.get("min")
        activity.max_glucose = glucose_stats.get("max")
        activity.time_in_range_percent = glucose_stats.get("pct_in_range")
        activity.hypo_count = glucose_stats.get("nb_hypo")
        activity.hyper_count = glucose_stats.get("nb_hyper")

    if summary_block:
        activity.glucose_summary_block = summary_block

    activity.last_synced_at = now_utc

    db.commit()
    db.refresh(activity)

    return activity

# ---------------------------------------------------------------------------
# Helper pente : calcul d'une pente lissée à partir distance / altitude
# ---------------------------------------------------------------------------

def compute_slope_series(
    dist_stream: list[Optional[float]],
    alt_stream: list[Optional[float]],
    window: int = 4,
    min_distance: float = 10.0,
) -> list[Optional[float]]:
    n = len(dist_stream)
    slopes: list[Optional[float]] = [None] * n
    if n == 0:
        return slopes

    for i in range(n):
        left = max(0, i - window)
        right = min(n - 1, i + window)

        dist_left = dist_stream[left]
        dist_right = dist_stream[right]
        alt_left = alt_stream[left]
        alt_right = alt_stream[right]

        if (
            dist_left is None or dist_right is None or
            alt_left is None or alt_right is None
        ):
            slopes[i] = None
            continue

        delta_dist = dist_right - dist_left
        delta_alt = alt_right - alt_left

        if delta_dist <= min_distance:
            slopes[i] = None
            continue

        slope_percent = (delta_alt / delta_dist) * 100.0

        if slope_percent < -60 or slope_percent > 60:
            slopes[i] = None
        else:
            slopes[i] = slope_percent

    return slopes

# ---------------------------------------------------------------------------
# Calcul de la vitesse ascentionnelle (VAM) sur une série de points
# ---------------------------------------------------------------------------

def compute_vertical_speed_series(
    elapsed_stream: List[Optional[float]],  # secondes depuis start
    alt_stream: List[Optional[float]],      # mètres
    window_pts: int = 4,                    # même logique que pour slope
    min_dt: float = 10.0,                   # sec min pour un ratio fiable
    only_ascent: bool = True,               # VAM uniquement en montée
    clamp_abs_mph: float = 4000.0           # coupe les valeurs absurdes
) -> List[Optional[float]]:
    n = len(elapsed_stream)
    out: List[Optional[float]] = [None] * n
    if n == 0:
        return out

    for i in range(n):
        left = max(0, i - window_pts)
        right = min(n - 1, i + window_pts)

        t0 = elapsed_stream[left]
        t1 = elapsed_stream[right]
        a0 = alt_stream[left]
        a1 = alt_stream[right]

        if t0 is None or t1 is None or a0 is None or a1 is None:
            out[i] = None
            continue

        dt_sec = float(t1 - t0)
        if dt_sec < min_dt:
            out[i] = None
            continue

        d_alt = float(a1 - a0)
        if only_ascent and d_alt <= 0:
            out[i] = None
            continue

        vam_m_per_h = (d_alt / dt_sec) * 3600.0

        if abs(vam_m_per_h) > clamp_abs_mph:
            out[i] = None
        else:
            out[i] = vam_m_per_h

    return out

# ---------------------------------------------------------------------------
# Matching glycémie ↔ timeline Strava
# ---------------------------------------------------------------------------

def match_glucose_to_time_stream(
    graph: list,
    start: dt.datetime,
    time_stream: list,
    max_delta_sec: int = 600,
):
    if not graph or not time_stream:
        n = len(time_stream) if time_stream else 0
        return [None] * n, [None] * n, [None] * n

    valid = [
        p for p in graph
        if p.get("mgdl") is not None and p.get("ts") is not None
    ]
    if not valid:
        n = len(time_stream)
        return [None] * n, [None] * n, [None] * n

    valid.sort(key=lambda p: p["ts"])
    ts_list = [p["ts"] for p in valid]

    glucoses = []
    trends = []
    sources = []

    for t_sec in time_stream:
        t_abs = start + dt.timedelta(seconds=int(t_sec))
        pos = bisect.bisect_left(ts_list, t_abs)

    # candidates = immediate neighbors
        candidates = []
        if pos > 0:
            candidates.append(valid[pos - 1])
        if pos < len(valid):
            candidates.append(valid[pos])

        best = None
        best_delta = None
        for c in candidates:
            delta = abs((c["ts"] - t_abs).total_seconds())
            if best is None or delta < best_delta:
                best = c
                best_delta = delta

        if best is not None and best_delta <= max_delta_sec:
            glucoses.append(float(best["mgdl"]))
            trends.append(best.get("trend"))
            sources.append(best.get("source", "archive"))
        else:
            glucoses.append(None)
            trends.append(None)
            sources.append(None)

    return glucoses, trends, sources

def get_hr_zone_label(hr: Optional[float], fc_max: float) -> Optional[str]:
    if hr is None or fc_max <= 0:
        return None

    last_name = None
    last_lo = None
    for name, lo_r, hi_r in HR_ZONES:
        lo = lo_r * fc_max
        hi = hi_r * fc_max
        last_name = name
        last_lo = lo
        if lo <= hr < hi:
            return name

    if last_name is not None and last_lo is not None and hr >= last_lo:
        return last_name

    return None


#---------------------------------------------------------------------------
# Enregistrement des points de stream d’une activité
#---------------------------------------------------------------------------
def save_activity_stream_points(
    db: Session,
    *,
    activity: models.Activity,
    streams: dict,
) -> int:
    """
    Enregistre tous les points de stream Strava (temps, FC, altitude, etc.)
    pour une activité donnée dans la table ActivityStreamPoint.

    Ajoute aussi:
      - slope_percent (%)
      - vertical_speed_m_per_h (m/h)
      - hr_zone ("Zone 1"...)
    """
    user = db.query(models.User).get(activity.user_id)
    fc_max = compute_user_fc_max(user)

    def get_data(key: str):
        s = streams.get(key) or {}
        if isinstance(s, dict):
            return s.get("data") or []
        return []

    time_stream = get_data("time")
    if not time_stream:
        return 0

    dist_stream = get_data("distance")
    alt_stream = get_data("altitude")
    hr_stream = get_data("heartrate")
    cad_stream = get_data("cadence")
    vel_stream = get_data("velocity_smooth")
    watts_stream = get_data("watts")
    grade_stream = get_data("grade_smooth")
    temp_stream = get_data("temp")
    moving_stream = get_data("moving")
    latlng_stream = get_data("latlng")

    # glycémie (optionnel)
    glu_stream = get_data("glucose_mgdl")
    glu_trend_stream = get_data("glucose_trend")
    glu_source_stream = get_data("glucose_source")

    # ✅ pente lissée
    slope_stream: list[Optional[float]] = []
    if dist_stream and alt_stream:
        m = min(len(time_stream), len(dist_stream), len(alt_stream))
        dist_for_slope = [float(d) if d is not None else None for d in dist_stream[:m]]
        alt_for_slope  = [float(a) if a is not None else None for a in alt_stream[:m]]
        slope_stream = compute_slope_series(
            dist_for_slope,
            alt_for_slope,
            window=4,
            min_distance=10.0,
        )
    else:
        slope_stream = []

    # ✅ VAM lissée (m/h)
    vertical_speed_stream: list[Optional[float]] = []
    if time_stream and alt_stream:
        m_vam = min(len(time_stream), len(alt_stream))
        elapsed_for_vam = [float(t) if t is not None else None for t in time_stream[:m_vam]]
        alt_for_vam     = [float(a) if a is not None else None for a in alt_stream[:m_vam]]
        vertical_speed_stream = compute_vertical_speed_series(
            elapsed_stream=elapsed_for_vam,
            alt_stream=alt_for_vam,
            window_pts=5,       # ≈ 10 s à 1 Hz
            min_dt=10.0,        # accepté (test "<" dans la fonction)
            only_ascent=True,   # VAM uniquement en montée
            clamp_abs_mph=4000.0,
        )
    else:
        vertical_speed_stream = []

    # Longueur commune minimale
    candidates = [len(time_stream)]
    for arr in [
        dist_stream,
        alt_stream,
        hr_stream,
        cad_stream,
        vel_stream,
        watts_stream,
        grade_stream,
        temp_stream,
        moving_stream,
        latlng_stream,
        glu_stream,
        glu_trend_stream,
        glu_source_stream,
        slope_stream,
        vertical_speed_stream,
    ]:
        if arr:
            candidates.append(len(arr))

    n = min(candidates) if candidates else 0
    if n == 0:
        return 0

    # Purge des anciens points
    db.query(models.ActivityStreamPoint).filter(
        models.ActivityStreamPoint.activity_id == activity.id
    ).delete()
    db.flush()

    start_date = activity.start_date

    for i in range(n):
        t = time_stream[i]

        dist  = dist_stream[i] if i < len(dist_stream) else None
        alt   = alt_stream[i] if i < len(alt_stream) else None
        hr    = hr_stream[i] if i < len(hr_stream) else None
        cad   = cad_stream[i] if i < len(cad_stream) else None
        vel   = vel_stream[i] if i < len(vel_stream) else None
        watts = watts_stream[i] if i < len(watts_stream) else None
        grade = grade_stream[i] if i < len(grade_stream) else None
        temp  = temp_stream[i] if i < len(temp_stream) else None
        moving = moving_stream[i] if i < len(moving_stream) else None

        lat = lon = None
        if i < len(latlng_stream):
            point = latlng_stream[i]
            if isinstance(point, (list, tuple)) and len(point) >= 2:
                lat, lon = point[0], point[1]

        glu        = glu_stream[i] if i < len(glu_stream) else None
        glu_trend  = glu_trend_stream[i] if i < len(glu_trend_stream) else None
        glu_source = glu_source_stream[i] if i < len(glu_source_stream) else None

        slope = slope_stream[i] if i < len(slope_stream) else None
        vs_mh = vertical_speed_stream[i] if i < len(vertical_speed_stream) else None

        hr_zone_label = get_hr_zone_label(hr, fc_max)

        timestamp = None
        if start_date is not None and t is not None:
            try:
                timestamp = start_date + dt.timedelta(seconds=int(t))
            except Exception:
                timestamp = None

        p = models.ActivityStreamPoint(
            activity_id=activity.id,
            idx=i,
            elapsed_time=int(t) if t is not None else None,
            distance=dist,
            altitude=alt,
            heartrate=hr,
            cadence=cad,
            velocity=vel,
            watts=watts,
            grade=grade,
            temp=temp,
            moving=bool(moving) if moving is not None else None,
            lat=lat,
            lon=lon,
            timestamp=timestamp,
            # glucose
            glucose_mgdl=glu,
            glucose_trend=glu_trend,
            glucose_source=glu_source,
            # dérivés
            slope_percent=slope,
            hr_zone=hr_zone_label,
            vertical_speed_m_per_h=vs_mh,
        )
        db.add(p)

    db.commit()
    return n


# ---------------------------------------------------------------------------
# VAM peaks 5/15/30 min + MAJ caches activity + PR utilisateur
# ---------------------------------------------------------------------------

def compute_and_store_vam_peaks(db: Session, activity: models.Activity, user_id: int) -> None:
    """
    Calcule les meilleurs segments VAM sur 5/15/30 minutes pour une activité,
    contrainte : perte <= 5% du gain sur le segment.
    - Met à jour Activity.max_vam_5m/15m/30m (+ *_start_ts)
    - Upsert une ligne par fenêtre dans ActivityVamPeak
    - Met à jour les PR utilisateur (UserVamPR) par sport
    """
    # 1) charger le stream altitude/temps (ordonné)
    rows = (
        db.query(models.ActivityStreamPoint.elapsed_time,
                 models.ActivityStreamPoint.altitude,
                 models.ActivityStreamPoint.timestamp)
        .filter(models.ActivityStreamPoint.activity_id == activity.id)
        .order_by(models.ActivityStreamPoint.idx.asc())
        .all()
    )
    if not rows or len(rows) < 2:
        return

    # séries propres
    t = []
    a = []
    ts = []
    for et, alt, ts_abs in rows:
        if et is None or alt is None:
            continue
        t.append(float(et))
        a.append(float(alt))
        ts.append(_safe_dt(ts_abs))

    n = len(t)
    if n < 2:
        return

    # fenêtre glissante par durée
    def best_vam_for_window(window_min: int):
        goal = window_min * 60.0
        i = 0
        gain_best = 0.0
        vam_best = None
        start_i_best = None
        end_j_best = None
        start_ts_best = None
        end_ts_best = None
        loss_best = 0.0

        j = 0
        while i < n - 1:
            # avancer j pour couvrir >= goal
            while j < n and (t[j] - t[i] < goal):
                j += 1
            if j >= n:
                break

            # gain/loss sur [i, j]
            gain = 0.0
            loss = 0.0
            for k in range(i, j):
                da = a[k + 1] - a[k]
                if da > 0:
                    gain += da
                elif da < 0:
                    loss += -da

            if gain > 0:
                if loss <= 0.05 * gain:
                    dur = t[j] - t[i]
                    vam = (gain / dur) * 3600.0  # m/h
                    if (vam_best is None) or (vam > vam_best):
                        vam_best = vam
                        gain_best = gain
                        loss_best = loss
                        start_i_best = i
                        end_j_best = j
                        start_ts_best = ts[i] if ts[i] is not None else (activity.start_date + dt.timedelta(seconds=int(t[i]))) if activity.start_date else None
                        end_ts_best = ts[j] if ts[j] is not None else (activity.start_date + dt.timedelta(seconds=int(t[j]))) if activity.start_date else None

            i += 1

        if vam_best is None:
            return None
        return {
            "vam": vam_best,
            "gain": gain_best,
            "loss": loss_best,
            "start_idx": start_i_best,
            "end_idx": end_j_best,
            "start_ts": start_ts_best,
            "end_ts": end_ts_best,
            "duration_sec": (t[end_j_best] - t[start_i_best]) if (start_i_best is not None and end_j_best is not None) else None,
            "distance_m": None,
        }

    sport = activity.sport or "other"
    user_id = int(user_id)

    windows = [5, 15, 30]
    bests = {w: best_vam_for_window(w) for w in windows}

    # 2) Upserts dans ActivityVamPeak + caches Activity
    field_map = {
        5: ("max_vam_5m", "max_vam_5m_start_ts"),
        15: ("max_vam_15m", "max_vam_15m_start_ts"),
        30: ("max_vam_30m", "max_vam_30m_start_ts"),
    }

    for w in windows:
        res = bests[w]
        if not res:
            continue

        # a) caches dans Activity
        vam_field, ts_field = field_map[w]
        setattr(activity, vam_field, float(res["vam"]))
        setattr(activity, ts_field, _safe_dt(res["start_ts"]))

        # b) upsert ActivityVamPeak (1 ligne/fenêtre)
        peak = (
            db.query(models.ActivityVamPeak)
            .filter(models.ActivityVamPeak.activity_id == activity.id,
                    models.ActivityVamPeak.window_min == w)
            .one_or_none()
        )
        if peak is None:
            peak = models.ActivityVamPeak(
                user_id=user_id,
                activity_id=activity.id,
                sport=sport,
                window_min=w,
                max_vam_m_per_h=res["vam"],
                start_idx=res["start_idx"],
                end_idx=res["end_idx"],
                start_ts=_safe_dt(res["start_ts"]),
                end_ts=_safe_dt(res["end_ts"]),
                gain_m=res["gain"],
                loss_m=res["loss"],
                loss_pct_vs_gain=(100.0 * res["loss"] / res["gain"]) if res["gain"] else 0.0,
                distance_m=res["distance_m"],
                method="window_by_duration_v1",
            )
            db.add(peak)
        else:
            peak.user_id = user_id
            peak.sport = sport
            peak.max_vam_m_per_h = res["vam"]
            peak.start_idx = res["start_idx"]
            peak.end_idx = res["end_idx"]
            peak.start_ts = _safe_dt(res["start_ts"])
            peak.end_ts = _safe_dt(res["end_ts"])
            peak.gain_m = res["gain"]
            peak.loss_m = res["loss"]
            peak.loss_pct_vs_gain = (100.0 * res["loss"] / res["gain"]) if res["gain"] else 0.0
            peak.distance_m = res["distance_m"]
            peak.method = "window_by_duration_v1"

        # c) Records utilisateur (PR) par sport
        _update_user_vam_pr(db, user_id=user_id, sport=sport, window_min=w,
                            vam_value=float(res["vam"]), activity_id=activity.id,
                            start_ts=_safe_dt(res["start_ts"]))

    db.add(activity)
    db.commit()

def compute_and_store_zone_slope_aggs(db: Session, activity: models.Activity, user_id: int) -> None:
    """
    Calcule et enregistre les agrégats par zone cardio × pente :
      - durée totale
      - distance
      - dénivelé positif
      - VAM moyenne
      - cadence moyenne
      - vitesse moyenne
      - allure moyenne (s/km)
    """
    sport = getattr(activity, "sport", "unknown")

    points = (
        db.query(models.ActivityStreamPoint)
        .filter(models.ActivityStreamPoint.activity_id == activity.id)
        .order_by(models.ActivityStreamPoint.idx.asc())
        .all()
    )
    if not points:
        return

    # 🎯 NOUVELLES BANDES : NEGATIVES + POSITIVES (symétriques)
    slope_bands = [
        (-999, -40, "Sneg40p"),
        (-40, -30, "Sneg30_40"),
        (-30, -25, "Sneg25_30"),
        (-25, -20, "Sneg20_25"),
        (-20, -15, "Sneg15_20"),
        (-15, -10, "Sneg10_15"),
        (-10, -5,  "Sneg5_10"),
        (-5, 0,    "Sneg0_5"),

        (0, 5, "S0_5"),
        (5, 10, "S5_10"),
        (10, 15, "S10_15"),
        (15, 20, "S15_20"),
        (20, 25, "S20_25"),
        (25, 30, "S25_30"),
        (30, 40, "S30_40"),
        (40, 999, "S40p"),
    ]

    aggs = {}  # (hr_zone, slope_band) → stats

    for idx, p in enumerate(points):
        if not p.hr_zone or p.slope_percent is None:
            continue

        hr_zone = p.hr_zone
        slope = p.slope_percent  # GARDER SIGNÉ !

        # Trouver la bande correspondante
        slope_band = None
        for lo, hi, label in slope_bands:
            if lo <= slope < hi:
                slope_band = label
                break
        if slope_band is None:
            continue

        key = (hr_zone, slope_band)

        if key not in aggs:
            aggs[key] = {
                "duration_sec": 0,
                "num_points": 0,
                "distance_m": 0.0,
                "elevation_gain_m": 0.0,
                "sum_vam": 0.0,
                "sum_cad": 0.0,
                "sum_vel": 0.0,
            }

        # Durée réelle basée sur l'intervalle temporel avec le point suivant (fallback=1s)
        dt_sec = 1.0
        if idx + 1 < len(points):
            next_pt = points[idx + 1]
            if p.elapsed_time is not None and next_pt.elapsed_time is not None:
                delta = float(next_pt.elapsed_time) - float(p.elapsed_time)
                if delta > 0:
                    dt_sec = delta
        aggs[key]["duration_sec"] += dt_sec
        aggs[key]["num_points"] += 1

        if p.distance:
            aggs[key]["distance_m"] += float(p.distance)

        # D+ seulement si montée
        if p.altitude:
            aggs[key]["elevation_gain_m"] += max(0, p.altitude)

        if p.vertical_speed_m_per_h:
            aggs[key]["sum_vam"] += p.vertical_speed_m_per_h
        if p.cadence:
            aggs[key]["sum_cad"] += p.cadence
        if p.velocity:
            aggs[key]["sum_vel"] += p.velocity

    # Purge anciens
    db.query(models.ActivityZoneSlopeAgg).filter(
        models.ActivityZoneSlopeAgg.activity_id == activity.id
    ).delete()
    db.flush()

    # Enregistrement
    for (hr_zone, slope_band), d in aggs.items():
        n = max(1, d["num_points"])

        avg_vel = d["sum_vel"] / n
        avg_pace_s_per_km = (1000.0 / avg_vel) if avg_vel > 0 else None

        rec = models.ActivityZoneSlopeAgg(
            user_id=user_id,
            activity_id=activity.id,
            sport=sport,
            hr_zone=hr_zone,
            slope_band=slope_band,
            duration_sec=d["duration_sec"],
            num_points=d["num_points"],
            distance_m=d["distance_m"],
            elevation_gain_m=d["elevation_gain_m"],
            avg_vam_m_per_h=d["sum_vam"] / n,
            avg_cadence_spm=d["sum_cad"] / n,
            avg_velocity_m_s=avg_vel,
            avg_pace_s_per_km=avg_pace_s_per_km,
        )
        db.add(rec)

    db.commit()
    print(f"📊 Agrégats zone×pente calculés (pentes + et -) pour activité {activity.id}")


#----------------------------------------------------------------------------
#-------------- Construction du profil coureur ------------------------------------
#----------------------------------------------------------------------------   
def _update_user_vam_pr(
    db: Session,
    *,
    user_id: int,
    sport: str,
    window_min: int,
    vam_value: float,
    activity_id: int,
    start_ts: dt.datetime | None,
) -> None:
    pr = (
        db.query(models.UserVamPR)
        .filter(models.UserVamPR.user_id == user_id,
                models.UserVamPR.sport == sport,
                models.UserVamPR.window_min == window_min)
        .one_or_none()
    )
    if pr is None:
        pr = models.UserVamPR(
            user_id=user_id,
            sport=sport,
            window_min=window_min,
            vam_m_per_h=vam_value,
            activity_id=activity_id,
            start_ts=_safe_dt(start_ts),
            updated_at=dt.datetime.utcnow(),
        )
        db.add(pr)
    else:
        if vam_value > (pr.vam_m_per_h or 0.0):
            pr.vam_m_per_h = vam_value
            pr.activity_id = activity_id
            pr.start_ts = _safe_dt(start_ts)
            pr.updated_at = dt.datetime.utcnow()
    # pas de commit ici (l'appelant gère)

#---------------------------------------------------------------------------
# Construction du profil coureur de performance à partir des agrégats zone×pente
#---------------------------------------------------------------------------
def build_runner_profile(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
) -> dict:
    """
    Construit un "profil coureur" à partir des agrégats ActivityZoneSlopeAgg.

    - Agrège toutes les lignes pour un user + sport donnés
    - Optionnellement filtré par période (date_from / date_to sur Activity.start_date)
    - Retourne un dict prêt à être utilisé dans une route ou un template.

    Structure de retour (exemple) :

    {
      "user_id": 1,
      "sport": "run",
      "period": {
        "from": datetime | None,
        "to": datetime | None,
      },
      "zones": {
        "Zone 1": {
          "S0_5": {
            "duration_sec": 1234,
            "distance_m": 5678.0,
            "elevation_gain_m": 321.0,
            "avg_vam_m_per_h": 900.0,
            "avg_cadence_spm": 160.0,
            "avg_velocity_m_s": 2.8,
            "avg_pace_s_per_km": 357,   # en secondes / km
            "num_points": 456,
          },
          "S5_10": { ... },
          ...
        },
        "Zone 2": {
          ...
        },
        ...
      }
    }
    """

    # 1) Charger toutes les lignes d'agrégats pour ce user + sport (+ filtre dates)
    q = (
        db.query(models.ActivityZoneSlopeAgg, models.Activity.start_date)
        .join(models.Activity, models.ActivityZoneSlopeAgg.activity_id == models.Activity.id)
        .filter(
            models.ActivityZoneSlopeAgg.user_id == user_id,
            models.ActivityZoneSlopeAgg.sport == sport,
        )
    )

    if date_from is not None:
        # on force en UTC naïf pour être cohérent
        if date_from.tzinfo is not None:
            date_from = date_from.astimezone(dt.timezone.utc).replace(tzinfo=None)
        q = q.filter(models.Activity.start_date >= date_from)

    if date_to is not None:
        if date_to.tzinfo is not None:
            date_to = date_to.astimezone(dt.timezone.utc).replace(tzinfo=None)
        q = q.filter(models.Activity.start_date < date_to)

    rows = q.all()
    if not rows:
        return {
            "user_id": user_id,
            "sport": sport,
            "period": {"from": date_from, "to": date_to},
            "zones": {},
        }

    # 2) Agrégateurs en mémoire
    # key = (hr_zone, slope_band)
    agg = {}

    for row, start_date in rows:
        hr_zone = row.hr_zone
        slope_band = row.slope_band

        if hr_zone is None or slope_band is None:
            continue

        key = (hr_zone, slope_band)
        if key not in agg:
            agg[key] = {
                "duration_sec": 0,
                "num_points": 0,
                "distance_m": 0.0,
                "elevation_gain_m": 0.0,
                # sommes pondérées par la durée (pour moyennes globales)
                "sum_vam_x_dur": 0.0,
                "sum_cad_x_dur": 0.0,
                "sum_vel_x_dur": 0.0,
                "sum_pace_x_dur": 0.0,
                # compteurs pour chacune de ces moyennes
                "dur_for_vam": 0.0,
                "dur_for_cad": 0.0,
                "dur_for_vel": 0.0,
                "dur_for_pace": 0.0,
            }

        a = agg[key]

        dur = float(row.duration_sec or 0)
        a["duration_sec"] += dur
        a["num_points"] += int(row.num_points or 0)
        a["distance_m"] += float(row.distance_m or 0.0)
        a["elevation_gain_m"] += float(row.elevation_gain_m or 0.0)

        # Moyennes pondérées par la durée (si dispo)
        if row.avg_vam_m_per_h is not None:
            a["sum_vam_x_dur"] += float(row.avg_vam_m_per_h) * dur
            a["dur_for_vam"] += dur

        if row.avg_cadence_spm is not None:
            a["sum_cad_x_dur"] += float(row.avg_cadence_spm) * dur
            a["dur_for_cad"] += dur

        if row.avg_velocity_m_s is not None:
            a["sum_vel_x_dur"] += float(row.avg_velocity_m_s) * dur
            a["dur_for_vel"] += dur

        if row.avg_pace_s_per_km is not None:
            a["sum_pace_x_dur"] += float(row.avg_pace_s_per_km) * dur
            a["dur_for_pace"] += dur

    # 3) Conversion en structure finale imbriquée zones[hr_zone][slope_band]
    zones: dict[str, dict[str, dict]] = {}

    for (hr_zone, slope_band), a in agg.items():
        hr_dict = zones.setdefault(hr_zone, {})

        def _safe_avg(sum_x_dur: float, dur: float) -> float | None:
            if dur <= 0:
                return None
            return sum_x_dur / dur

        hr_dict[slope_band] = {
            "duration_sec": int(a["duration_sec"]),
            "num_points": int(a["num_points"]),
            "distance_m": a["distance_m"],
            "elevation_gain_m": a["elevation_gain_m"],
            "avg_vam_m_per_h": _safe_avg(a["sum_vam_x_dur"], a["dur_for_vam"]),
            "avg_cadence_spm": _safe_avg(a["sum_cad_x_dur"], a["dur_for_cad"]),
            "avg_velocity_m_s": _safe_avg(a["sum_vel_x_dur"], a["dur_for_vel"]),
            "avg_pace_s_per_km": _safe_avg(a["sum_pace_x_dur"], a["dur_for_pace"]),
        }

    # 4 bis) Calcul de la "dégradation d'allure" par pente
    #    - pour chaque slope_band : meilleure allure (toutes zones confondues)
    #    - pour chaque zone : % plus lent que cette meilleure allure

    def _format_pace(sec: float | None) -> str | None:
        if sec is None or sec <= 0:
            return None
        total = int(round(sec))
        m, s = divmod(total, 60)
        return f"{m:02d}:{s:02d}"

    # a) meilleure allure (en s/km) par slope_band
    best_pace_by_slope: dict[str, float] = {}

    for hr_zone, slopes in zones.items():
        for slope_band, cell in slopes.items():
            pace = cell.get("avg_pace_s_per_km")
            if pace is None or pace <= 0:
                continue
            current_best = best_pace_by_slope.get(slope_band)
            if current_best is None or pace < current_best:
                best_pace_by_slope[slope_band] = pace

    # b) pour chaque zone×pente : calcul du % de dégradation + string d'allure
    for hr_zone, slopes in zones.items():
        for slope_band, cell in slopes.items():
            pace = cell.get("avg_pace_s_per_km")
            best = best_pace_by_slope.get(slope_band)

            rel_pct: float | None = None
            if pace is not None and pace > 0 and best is not None and best > 0:
                rel_pct = (pace - best) / best * 100.0

            cell["rel_pace_pct"] = rel_pct         # % plus lent que le meilleur de cette pente
            cell["pace_str"] = _format_pace(pace)  # allure formatée mm:ss


    # 4) On peut éventuellement trier les zones (Zone 1..5) et slopes si besoin plus tard dans le template
    profile = {
        "user_id": user_id,
        "sport": sport,
        "period": {"from": date_from, "to": date_to},
        "zones": zones,
    }

    return profile


#---------------------------------------------------------------------------
def compute_best_dplus_windows(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
) -> list[dict]:
    """Retourne le meilleur D+ cumulé sur des fenêtres glissantes (1h,2h,5h,10h,24h)."""

    window_defs = [
        {"id": "1m", "label": "1 min", "seconds": 60},
        {"id": "5m", "label": "5 min", "seconds": 5 * 60},
        {"id": "15m", "label": "15 min", "seconds": 15 * 60},
        {"id": "30m", "label": "30 min", "seconds": 30 * 60},
        {"id": "1h", "label": "1 h", "seconds": 3600},
        {"id": "2h", "label": "2 h", "seconds": 7200},
        {"id": "5h", "label": "5 h", "seconds": 5 * 3600},
        {"id": "10h", "label": "10 h", "seconds": 10 * 3600},
        {"id": "24h", "label": "24 h", "seconds": 24 * 3600},
    ]

    # Prépare les placeholders de résultat
    results = {
        w["id"]: {
            "window_id": w["id"],
            "label": w["label"],
            "seconds": w["seconds"],
            "gain_m": 0.0,
            "activity": None,
            "start_offset_sec": None,
            "end_offset_sec": None,
            "duration_sec": None,
            "loss_m": 0.0,
            "distance_m": 0.0,
        }
        for w in window_defs
    }

    activities_q = db.query(models.Activity).filter(
        models.Activity.user_id == user_id,
        models.Activity.sport == sport,
    )

    if date_from is not None:
        if date_from.tzinfo is not None:
            date_from = date_from.astimezone(dt.timezone.utc).replace(tzinfo=None)
        activities_q = activities_q.filter(models.Activity.start_date >= date_from)

    if date_to is not None:
        if date_to.tzinfo is not None:
            date_to = date_to.astimezone(dt.timezone.utc).replace(tzinfo=None)
        activities_q = activities_q.filter(models.Activity.start_date < date_to)

    activities = activities_q.order_by(models.Activity.start_date.desc()).all()
    if not activities:
        return list(results.values())

    for activity in activities:
        points = (
            db.query(
                models.ActivityStreamPoint.elapsed_time,
                models.ActivityStreamPoint.altitude,
                models.ActivityStreamPoint.distance,
            )
            .filter(models.ActivityStreamPoint.activity_id == activity.id)
            .order_by(models.ActivityStreamPoint.idx.asc())
            .all()
        )

        if not points or len(points) < 2:
            continue

        times = []
        cum_dplus = []
        cum_dminus = []
        cum_distance = []
        cumulative = 0.0
        cumulative_loss = 0.0
        current_distance = 0.0
        prev_alt = None
        last_distance_val = None

        for p in points:
            if p.elapsed_time is None:
                continue

            alt = float(p.altitude) if p.altitude is not None else None
            if alt is not None and prev_alt is not None:
                delta = alt - prev_alt
                if delta > 0:
                    cumulative += delta
                elif delta < 0:
                    cumulative_loss += -delta
            if alt is not None:
                prev_alt = alt

            if p.distance is not None:
                current_distance = float(p.distance)
                last_distance_val = current_distance
            elif last_distance_val is not None:
                current_distance = last_distance_val

            times.append(float(p.elapsed_time))
            cum_dplus.append(cumulative)
            cum_dminus.append(cumulative_loss)
            cum_distance.append(current_distance)

        if len(times) < 2:
            continue

        for win in window_defs:
            win_res = results[win["id"]]
            win_sec = win["seconds"]
            start_idx = 0
            for idx, t in enumerate(times):
                while start_idx < idx and (t - times[start_idx]) > win_sec:
                    start_idx += 1
                gain = cum_dplus[idx] - cum_dplus[start_idx]
                loss = cum_dminus[idx] - cum_dminus[start_idx]
                dist = cum_distance[idx] - cum_distance[start_idx]
                if gain > win_res["gain_m"]:
                    start_time = times[start_idx]
                    end_time = t
                    duration = max(end_time - start_time, 1e-6)
                    win_res.update(
                        {
                            "gain_m": gain,
                            "loss_m": max(loss, 0.0),
                            "distance_m": max(dist, 0.0),
                            "activity": activity,
                            "start_offset_sec": start_time,
                            "end_offset_sec": end_time,
                            "duration_sec": duration,
                        }
                    )

    formatted = []
    for win in window_defs:
        res = results[win["id"]]
        activity = res["activity"]
        if activity and res["duration_sec"]:
            start_dt = activity.start_date
            if start_dt is not None:
                start_dt = _safe_dt(start_dt)
            start_point = start_dt + dt.timedelta(seconds=res["start_offset_sec"]) if (start_dt and res["start_offset_sec"] is not None) else None
            end_point = start_dt + dt.timedelta(seconds=res["end_offset_sec"]) if (start_dt and res["end_offset_sec"] is not None) else None
            gain_per_hour = res["gain_m"] * 3600.0 / max(res["duration_sec"], 1.0)
            formatted.append(
                {
                    "window_id": res["window_id"],
                    "label": res["label"],
                    "seconds": res["seconds"],
                    "gain_m": res["gain_m"],
                    "loss_m": res.get("loss_m") or 0.0,
                    "distance_km": (res.get("distance_m") or 0.0) / 1000.0,
                    "activity_id": activity.id,
                    "activity_name": activity.name,
                    "activity_date": activity.start_date,
                    "start_datetime": start_point,
                    "end_datetime": end_point,
                    "duration_sec": res["duration_sec"],
                    "gain_per_hour": gain_per_hour,
                }
            )
        else:
            formatted.append(
                {
                    "window_id": res["window_id"],
                    "label": res["label"],
                    "seconds": res["seconds"],
                    "gain_m": 0.0,
                    "loss_m": 0.0,
                    "distance_km": 0.0,
                    "activity_id": None,
                    "activity_name": None,
                    "activity_date": None,
                    "start_datetime": None,
                    "end_datetime": None,
                    "duration_sec": None,
                    "gain_per_hour": None,
                }
            )

    return formatted


#---------------------------------------------------------------------------
# Construction du profil de fatigue à partir des agrégats zone×pente
#---------------------------------------------------------------------------
def build_fatigue_profile(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    hr_zone: str | None = None,           # filtre optionnel zone cardio
    date_from: dt.datetime | None = None, # 👈 AJOUT
    date_to: dt.datetime | None = None,   # 👈 AJOUT
) -> dict:
    """
    Profil de fatigue : pour chaque pente (slope_band), on regarde l'allure moyenne
    sur différentes durées de sortie, et on calcule la dégradation vs sorties < 2h.

    - Si hr_zone est renseignée (ex: "Zone 3"), on ne prend en compte que cette zone.
    - Si date_from / date_to sont renseignées, on filtre sur Activity.start_date.
    """

    # Définition des "buckets" de durée (en heures)
    buckets = [
        {"id": "short",  "label": "< 2h",     "min_h": 0.0,  "max_h": 2.0},
        {"id": "mid",    "label": "2–5h",     "min_h": 2.0,  "max_h": 5.0},
        {"id": "long",   "label": "5–10h",    "min_h": 5.0,  "max_h": 10.0},
        {"id": "ultra",  "label": "≥ 10h",    "min_h": 10.0, "max_h": None},
    ]

    # 1) Charger toutes les lignes agrégées + la durée de l’activité (pour filtres période)
    q = (
        db.query(
            models.ActivityZoneSlopeAgg,
            models.Activity.elapsed_time,
            models.Activity.start_date,
        )
        .join(models.Activity, models.ActivityZoneSlopeAgg.activity_id == models.Activity.id)
        .filter(
            models.ActivityZoneSlopeAgg.user_id == user_id,
            models.ActivityZoneSlopeAgg.sport == sport,
        )
    )

    # Filtre optionnel sur la zone cardio
    if hr_zone is not None:
        q = q.filter(models.ActivityZoneSlopeAgg.hr_zone == hr_zone)

    # 🔎 Filtres période sur Activity.start_date (on ne change pas les colonnes retournées)
    if date_from is not None:
        if date_from.tzinfo is not None:
            date_from = date_from.astimezone(dt.timezone.utc).replace(tzinfo=None)
        q = q.filter(models.Activity.start_date >= date_from)

    if date_to is not None:
        if date_to.tzinfo is not None:
            date_to = date_to.astimezone(dt.timezone.utc).replace(tzinfo=None)
        q = q.filter(models.Activity.start_date < date_to)

    rows = q.all()
    if not rows:
        return {
            "user_id": user_id,
            "sport": sport,
            "hr_zone": hr_zone,
            "buckets": buckets,
            "by_slope": {},
        }

    activity_dur_sum: dict[int, float] = defaultdict(float)
    activity_elapsed_map: dict[int, float] = {}
    for row, elapsed_time, _start_date in rows:
        if row.activity_id is None:
            continue
        activity_dur_sum[row.activity_id] += float(row.duration_sec or 0)
        if elapsed_time:
            activity_elapsed_map[row.activity_id] = float(elapsed_time)

    # 2) Agrégation en mémoire
    # Structure : by_slope[slope_band][bucket_id] = {...}
    by_slope: dict[str, dict[str, dict]] = {}

    def _find_bucket_id(hours: float) -> str | None:
        for b in buckets:
            if hours < b["min_h"]:
                continue
            if b["max_h"] is None or hours < b["max_h"]:
                return b["id"]
        return None

    combined_by_activity: dict[tuple[int, str], dict[str, float]] = {}

    for row, elapsed_time, _start_date in rows:
        slope_band = row.slope_band
        if slope_band is None:
            continue

        zone_duration_sec = float(row.duration_sec or 0)
        if row.activity_id is not None:
            total_dur = activity_dur_sum.get(row.activity_id)
            activity_elapsed = activity_elapsed_map.get(row.activity_id)
            if total_dur and activity_elapsed and total_dur > 0:
                scale = activity_elapsed / total_dur
                if scale > 1.2:  # corrige les anciens agrégats 1s/point
                    zone_duration_sec *= scale
        if zone_duration_sec <= 0:
            continue

        if hr_zone is None:
            if row.activity_id is None:
                continue
            key = (row.activity_id, slope_band)
            stats = combined_by_activity.setdefault(
                key,
                {
                    "duration_sec": 0.0,
                    "sum_pace_x_dur": 0.0,
                    "dur_for_pace": 0.0,
                    "elapsed_time": float(elapsed_time or 0.0),
                },
            )
            stats["duration_sec"] += zone_duration_sec
            pace = row.avg_pace_s_per_km
            if pace is not None:
                stats["sum_pace_x_dur"] += float(pace) * zone_duration_sec
                stats["dur_for_pace"] += zone_duration_sec
            if elapsed_time:
                stats["elapsed_time"] = float(elapsed_time)
            continue

        hours = zone_duration_sec / 3600.0
        bucket_id = _find_bucket_id(hours)
        if bucket_id is None:
            continue

        slope_dict = by_slope.setdefault(slope_band, {})
        b_dict = slope_dict.setdefault(
            bucket_id,
            {
                "sum_pace_x_dur": 0.0,
                "dur_for_pace": 0.0,
                "count": 0,
            },
        )

        dur_sec = zone_duration_sec
        pace = row.avg_pace_s_per_km
        if pace is not None and dur_sec > 0:
            b_dict["sum_pace_x_dur"] += float(pace) * dur_sec
            b_dict["dur_for_pace"] += dur_sec
            b_dict["count"] += 1

    if hr_zone is None:
        for (activity_id, slope_band), stats in combined_by_activity.items():
            elapsed_sec = stats.get("elapsed_time", 0.0)
            if elapsed_sec <= 0:
                continue
            hours_total = elapsed_sec / 3600.0
            bucket_id = _find_bucket_id(hours_total)
            if bucket_id is None:
                continue

            slope_dict = by_slope.setdefault(slope_band, {})
            b_dict = slope_dict.setdefault(
                bucket_id,
                {
                    "sum_pace_x_dur": 0.0,
                    "dur_for_pace": 0.0,
                    "count": 0,
                },
            )

            if stats["dur_for_pace"] > 0:
                b_dict["sum_pace_x_dur"] += stats["sum_pace_x_dur"]
                b_dict["dur_for_pace"] += stats["dur_for_pace"]
            b_dict["count"] += 1

    # 3) Calcul des moyennes et de la dégradation vs "short"
    def _safe_avg(sum_x_dur: float, dur: float) -> float | None:
        if dur <= 0:
            return None
        return sum_x_dur / dur

    for slope_band, slope_dict in by_slope.items():
        # allure de référence sur sorties "short"
        short_stats = slope_dict.get("short")
        short_pace = None
        if short_stats and short_stats["dur_for_pace"] > 0:
            short_pace = _safe_avg(
                short_stats["sum_pace_x_dur"],
                short_stats["dur_for_pace"],
            )

        for bucket_id, b_dict in slope_dict.items():
            avg_pace = _safe_avg(b_dict["sum_pace_x_dur"], b_dict["dur_for_pace"])
            b_dict["avg_pace_s_per_km"] = avg_pace

            # Dégradation en % vs sorties < 2h
            if short_pace is not None and avg_pace is not None and short_pace > 0:
                b_dict["degradation_vs_short_pct"] = ((avg_pace / short_pace) - 1.0) * 100.0
            else:
                b_dict["degradation_vs_short_pct"] = None

    return {
        "user_id": user_id,
        "sport": sport,
        "hr_zone": hr_zone,
        "buckets": buckets,
        "by_slope": by_slope,
    }
