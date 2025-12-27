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
import math
import bisect
import statistics
import logging
from typing import Optional, List

from sqlalchemy import func
from sqlalchemy.orm import Session
from app import models

from collections import defaultdict

# --- helper pour timestamps naïfs -> UTC aware
def _safe_dt(ts):
    return ts if (ts is None or ts.tzinfo is not None) else ts.replace(tzinfo=dt.timezone.utc)


TARGET_MIN = 70
TARGET_MAX = 180

logger = logging.getLogger(__name__)

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

SERIES_DISTANCE_TARGETS_M = [
    200,
    400,
    800,
    1000,
    2000,
    3000,
    5000,
    10000,
    15000,
]

SERIES_DISTANCE_LABELS = {
    200: "200 m",
    400: "400 m",
    800: "800 m",
    1000: "1 km",
    2000: "2 km",
    3000: "3 km",
    5000: "5 km",
    10000: "10 km",
    15000: "15 km",
}

SERIES_REPETITION_COUNTS = [2, 3, 6, 8, 10, 15, 20, 30, 40]
SERIES_REPETITION_COLUMNS = [1] + SERIES_REPETITION_COUNTS
SERIES_LONG_DISTANCE_MIN_M = 5000
SERIES_MAX_STABILITY_RATIO = 0.08  # 8 % max variance vs moyenne
SERIES_MIN_PACE_S = 150            # 2:30 / km
SERIES_MAX_PACE_S = 720            # 12:00 / km

VOLUME_WINDOW_DAYS = 28
LONG_RUN_LOOKBACK_DAYS = 35
LONG_RUN_THRESHOLD_KM = 20.0
RUN_SPORT_ALIASES = {"run", "hike"}
RUN_VOLUME_SPORTS = set(RUN_SPORT_ALIASES)
SPORT_ALIAS_MAP = {
    "run": RUN_SPORT_ALIASES,
}


def canonicalize_sport_label(sport: Optional[str]) -> str:
    if not sport:
        return "other"
    normalized = sport.strip().lower()
    for canonical, aliases in SPORT_ALIAS_MAP.items():
        if normalized in aliases:
            return canonical
    return normalized


def sport_aliases(sport: str) -> set[str]:
    canonical = canonicalize_sport_label(sport)
    return set(SPORT_ALIAS_MAP.get(canonical, {canonical}))


def sport_column_condition(column, sport: str):
    aliases = sport_aliases(sport)
    if len(aliases) == 1:
        value = next(iter(aliases))
        return column == value
    return column.in_(tuple(aliases))


def normalize_activity_type(strava_type: Optional[str]) -> str:
    """
    Normalise le type d’activité Strava pour nos stats:
    - Regroupe TrailRun avec Run -> 'run'
    - Quelques regroupements utiles côté vélo/ski
    """
    if not strava_type:
        return "other"
    t = strava_type.strip().lower()

    # course à pied (trail inclus + marche/rando assimilées)
    if t in {"run", "trailrun", "trail run", "hike", "walk"}:
        return "run"

    # vélo
    if t in {"ride", "ebikeride", "gravelride", "virtualride"}:
        return "ride"
    if t in {"mountainbike", "mtb"}:
        return "ride"  # ou 'mtb' si tu veux distinguer

    # ski (ordre important)
    if t in {"nordicski", "rollerski"}:
        return "ski_nordic"
    if t in {"alpineski"}:
        return "ski_alpine"
    if t in {"backcountryski", "skitouring", "skimo"}:
        return "ski_rando"
    if t == "ski":
        return "ski_rando"
    if "ski" in t:
        # heuristique : mots-clés rando
        if any(k in t for k in ["backcountry", "tour", "rando", "skimo", "mountain", "alpin"]):
            return "ski_rando"
        # sinon par défaut ski alpin
        return "ski_alpine"

    return t



def _month_floor_date(ts: dt.datetime | None) -> dt.date | None:
    if ts is None:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return dt.date(ts.year, ts.month, 1)


def _next_month_date(month: dt.date) -> dt.date:
    if month.month == 12:
        return dt.date(month.year + 1, 1, 1)
    return dt.date(month.year, month.month + 1, 1)


def _parse_iso_datetime(ts_str: str | None) -> dt.datetime | None:
    if not ts_str:
        return None
    try:
        return dt.datetime.fromisoformat(ts_str)
    except ValueError:
        return None


def _isoformat(ts: dt.datetime | None) -> str | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.isoformat()


def _format_pace_label(pace_s: float | None) -> str | None:
    if not pace_s or pace_s <= 0:
        return None
    total = int(round(pace_s))
    minutes, seconds = divmod(total, 60)
    return f"{minutes}:{seconds:02d}"


def _format_time_label(seconds: float | None) -> str | None:
    if seconds is None or seconds <= 0:
        return None
    total = int(round(seconds))
    h = total // 3600
    m = (total % 3600) // 60
    s = total % 60
    if h > 0:
        return f"{h}h{m:02d}m{s:02d}s"
    return f"{m}:{s:02d}"


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
            # Formule Tanaka: FCmax = 208 - 0.7 * âge
            tanaka = round(208 - 0.7 * age)
            return max(150, tanaka)

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
def compute_stats(
    samples,
    activity_start: dt.datetime | None = None,
    activity_end: dt.datetime | None = None,
    start_value_hint: float | None = None,
    end_value_hint: float | None = None,
):
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

    pairs_with_ts = []
    for s in samples:
        val = s.get("mgdl")
        ts = s.get("ts")
        if val is None or ts is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        pairs_with_ts.append((ts, val))
    pairs_with_ts.sort(key=lambda x: x[0])

    def _value_at_start(pairs, target):
        if not pairs:
            return None
        if target is None:
            return pairs[0][1]
        for ts, val in pairs:
            if ts >= target:
                return val
        return pairs[-1][1]

    def _value_at_end(pairs, target):
        if not pairs:
            return None
        if target is None:
            return pairs[-1][1]
        for ts, val in reversed(pairs):
            if ts <= target:
                return val
        return pairs[0][1]

    start_val = _value_at_start(pairs_with_ts, activity_start)
    end_val = _value_at_end(pairs_with_ts, activity_end)

    if start_value_hint is not None:
        start_val = start_value_hint
    if end_value_hint is not None:
        end_val = end_value_hint

    lines = []
    lines.append(f"🔬Glycémie : Moy : {avg_r} mg/dL | {TARGET_MIN}-{TARGET_MAX} : {pct_zone_r}%")
    lines.append(bar)
    if start_val is not None or end_val is not None:
        start_str = f"{round(start_val)} mg/dL" if start_val is not None else "n/a"
        end_str = f"{round(end_val)} mg/dL" if end_val is not None else "n/a"
        lines.append(f"Départ : {start_str} | Arrivée : {end_str}")
    def _fmt(v):
        if v is None:
            return "n/a"
        if isinstance(v, (int, float)):
            return str(int(round(v)))
        return str(v)
    lines.append(f"Max : {_fmt(vmax)} mg/dL | Min : {_fmt(vmin)} mg/dL")
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
        "start_mgdl": start_val,
        "end_mgdl": end_val,
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
        "⚡ Allures max",               # nouveau bloc allures
        "⚡ Vitesse moy",               # bloc vitesses pour ski
        "⚡ Puissance moy",             # bloc puissance vélo
        "🎿 D+ max",                    # bloc ski D+
        "⛰️ D+ max",                    # bloc run D+
        "⛰️ VAM max",                  # nouveau bloc VAM
        "🔁 Cadence vélo",             # bloc cadence vélo
        "Cardio moy",                   # bloc cardio
        "Zone 1 :", "Zone 2 :", "Zone 3 :", "Zone 4 :", "Zone 5 :",  # zones FC
        "—> Made with ❤️ by Benoit",    # signature historique
        "—> Join us : https://strava-glucosev2.onrender.com/",
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
    sport = canonicalize_sport_label(sport)
    aliases = sport_aliases(sport)
    pr = (
        db.query(models.UserVamPR)
        .filter(
            models.UserVamPR.user_id == user_id,
            models.UserVamPR.window_min == window_min,
        )
    )
    if len(aliases) == 1:
        pr = pr.filter(models.UserVamPR.sport == sport)
    else:
        pr = pr.filter(models.UserVamPR.sport.in_(tuple(aliases)))
    pr = pr.one_or_none()
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
        if pr.sport != sport:
            pr.sport = sport
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
    sport = canonicalize_sport_label(sport)
    q = (
        db.query(models.ActivityZoneSlopeAgg, models.Activity.start_date)
        .join(models.Activity, models.ActivityZoneSlopeAgg.activity_id == models.Activity.id)
        .filter(
            models.ActivityZoneSlopeAgg.user_id == user_id,
            sport_column_condition(models.ActivityZoneSlopeAgg.sport, sport),
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
def get_cached_dplus_windows(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
) -> list[dict]:
    sport = canonicalize_sport_label(sport)
    q = (
        db.query(models.RunnerProfileMonthly)
        .filter(
            models.RunnerProfileMonthly.user_id == user_id,
            sport_column_condition(models.RunnerProfileMonthly.sport, sport),
            models.RunnerProfileMonthly.metric_scope == "ascent_window",
        )
    )

    month_from = _month_floor_date(date_from)
    if month_from is not None:
        q = q.filter(models.RunnerProfileMonthly.year_month >= month_from)

    if date_to is not None:
        month_to = _month_floor_date(date_to)
        if month_to is not None:
            q = q.filter(models.RunnerProfileMonthly.year_month < _next_month_date(month_to))

    rows = q.all()
    if not rows:
        return []

    best_by_window: dict[str, models.RunnerProfileMonthly] = {}
    for row in rows:
        window_label = row.window_label
        if not window_label:
            continue
        current = best_by_window.get(window_label)
        current_gain = current.dplus_total_m if current else None
        if current is None or (row.dplus_total_m or 0.0) > (current_gain or 0.0):
            best_by_window[window_label] = row

    if not best_by_window:
        return []

    window_defs = [
        {"id": "1m", "label": "1 min"},
        {"id": "5m", "label": "5 min"},
        {"id": "15m", "label": "15 min"},
        {"id": "30m", "label": "30 min"},
        {"id": "1h", "label": "1 h"},
        {"id": "2h", "label": "2 h"},
        {"id": "5h", "label": "5 h"},
        {"id": "10h", "label": "10 h"},
        {"id": "24h", "label": "24 h"},
    ]

    formatted = []
    for win in window_defs:
        row = best_by_window.get(win["id"])
        if not row:
            formatted.append(
                {
                    "window_id": win["id"],
                    "label": win["label"],
                    "seconds": None,
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
            continue

        extra = row.extra or {}
        start_dt = _parse_iso_datetime(extra.get("start_datetime") if isinstance(extra, dict) else None)
        end_dt = _parse_iso_datetime(extra.get("end_datetime") if isinstance(extra, dict) else None)

        formatted.append(
            {
                "window_id": row.window_label,
                "label": (extra.get("label") if isinstance(extra, dict) else None) or win["label"],
                "seconds": row.total_duration_sec,
                "gain_m": row.dplus_total_m or 0.0,
                "loss_m": row.dminus_total_m or 0.0,
                "distance_km": (row.total_distance_m or 0.0) / 1000.0,
                "activity_id": extra.get("activity_id") if isinstance(extra, dict) else None,
                "activity_name": extra.get("activity_name") if isinstance(extra, dict) else None,
                "activity_date": start_dt,
                "start_datetime": start_dt,
                "end_datetime": end_dt,
                "duration_sec": row.total_duration_sec,
                "gain_per_hour": row.avg_vam_m_per_h,
            }
        )

    return formatted


def get_series_splits_matrix(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
) -> dict:
    sport = canonicalize_sport_label(sport)
    q = (
        db.query(models.RunnerProfileMonthly)
        .filter(
            models.RunnerProfileMonthly.user_id == user_id,
            sport_column_condition(models.RunnerProfileMonthly.sport, sport),
            models.RunnerProfileMonthly.metric_scope == "series_splits",
        )
    )

    month_from = _month_floor_date(date_from)
    if month_from is not None:
        q = q.filter(models.RunnerProfileMonthly.year_month >= month_from)

    if date_to is not None:
        month_to = _month_floor_date(date_to)
        if month_to is not None:
            q = q.filter(models.RunnerProfileMonthly.year_month < _next_month_date(month_to))

    rows = q.all()

    aggregates: dict[tuple[int, int], dict] = {}

    for row in rows:
        distance_m = None
        if row.slope_band and row.slope_band.startswith("D"):
            try:
                distance_m = int(row.slope_band[1:])
            except ValueError:
                distance_m = None
        if not distance_m:
            extra = row.extra if isinstance(row.extra, dict) else {}
            try:
                distance_m = int(extra.get("distance_m") or 0)
            except (TypeError, ValueError):
                distance_m = None

        reps = None
        if row.hr_zone and row.hr_zone.startswith("R"):
            try:
                reps = int(row.hr_zone[1:])
            except ValueError:
                reps = None
        if not reps:
            extra = row.extra if isinstance(row.extra, dict) else {}
            try:
                reps = int(extra.get("reps") or 0)
            except (TypeError, ValueError):
                reps = None

        if not distance_m or not reps:
            continue

        key = (distance_m, reps)
        cell = aggregates.setdefault(
            key,
            {
                "series_count": 0,
                "stability_sum": 0.0,
                "stability_count": 0,
                "best_entry": None,
            },
        )

        extra = row.extra if isinstance(row.extra, dict) else {}
        cell["series_count"] += int(extra.get("series_count") or row.total_points or 0)

        stability_samples = extra.get("stability_samples")
        if isinstance(stability_samples, list):
            numeric = [float(s) for s in stability_samples if isinstance(s, (int, float))]
            cell["stability_sum"] += sum(numeric)
            cell["stability_count"] += len(numeric)

        best_series = extra.get("best_series")
        if isinstance(best_series, dict):
            best_pace = best_series.get("avg_pace_s_per_km")
            if best_pace and best_pace > 0:
                current = cell["best_entry"]
                if current is None or best_pace < current.get("avg_pace_s_per_km", float("inf")):
                    cell["best_entry"] = {
                        "avg_pace_s_per_km": best_pace,
                        "activity_id": best_series.get("activity_id"),
                        "date": _parse_iso_datetime(best_series.get("date")),
                        "duration_sec": best_series.get("duration_sec"),
                    }

    rows_out = []
    has_data = False

    for distance in SERIES_DISTANCE_TARGETS_M:
        cells = []
        for reps in SERIES_REPETITION_COLUMNS:
            bucket = aggregates.get((distance, reps))
            best_entry = bucket.get("best_entry") if bucket else None
            if best_entry and (best_entry.get("avg_pace_s_per_km") or 0) > 0:
                pace_val = best_entry["avg_pace_s_per_km"]
                stability_pct = None
                if bucket["stability_count"] > 0:
                    stability_pct = (bucket["stability_sum"] / bucket["stability_count"]) * 100.0

                cells.append(
                    {
                        "distance_m": distance,
                        "reps": reps,
                        "avg_pace_s_per_km": pace_val,
                        "pace_str": _format_pace_label(pace_val),
                        "series_count": bucket["series_count"],
                        "stability_pct": stability_pct,
                        "best_activity_id": best_entry.get("activity_id"),
                        "best_activity_date": best_entry.get("date"),
                    }
                )
                has_data = True
            else:
                cells.append(None)

        rows_out.append(
            {
                "distance_m": distance,
                "distance_label": SERIES_DISTANCE_LABELS.get(distance, f"{distance} m"),
                "cells": cells,
            }
        )

    repetition_headers = []
    for reps in SERIES_REPETITION_COLUMNS:
        repetition_headers.append(
            {
                "value": reps,
                "label": "1×" if reps == 1 else f"{reps}×",
                "description": "1 répétition" if reps == 1 else f"{reps} répétitions",
            }
        )

    return {
        "rows": rows_out,
        "repetitions": repetition_headers,
        "has_data": has_data,
    }


def _build_series_lookup(series_matrix: dict | None) -> dict[int, dict[int, dict]]:
    lookup: dict[int, dict[int, dict]] = {}
    if not series_matrix:
        return lookup
    for row in series_matrix.get("rows", []):
        distance_m = row.get("distance_m")
        if not distance_m:
            continue
        dist_cells = lookup.setdefault(distance_m, {})
        for cell in row.get("cells", []):
            if not cell:
                continue
            reps = cell.get("reps")
            pace = cell.get("avg_pace_s_per_km")
            if not reps or not pace:
                continue
            existing = dist_cells.get(reps)
            if not existing or pace < existing.get("avg_pace_s_per_km", float("inf")):
                dist_cells[reps] = cell
    return lookup


def _format_series_source(distance_m: int, reps: int) -> str:
    base = SERIES_DISTANCE_LABELS.get(distance_m, f"{distance_m} m")
    return f"{reps}×{base}" if reps > 1 else base


def _apply_direct_projection(
    lookup: dict[int, dict[int, dict]],
    combos: list[tuple[int, int]],
    distance_km: float,
    multiplier: float,
) -> tuple[float, float, str] | None:
    for distance_m, reps in combos:
        cell = lookup.get(distance_m, {}).get(reps)
        if not cell or not cell.get("avg_pace_s_per_km"):
            continue
        pace = cell["avg_pace_s_per_km"] * multiplier
        chrono = pace * distance_km
        return pace, chrono, _format_series_source(distance_m, reps)
    return None


def _ensure_projection_entry(
    results: dict, key: str, label: str, combo_labels: list[str] | None = None
) -> dict:
    entry = results.setdefault(
        key,
        {
            "target": key,
            "label": label,
            "available": False,
            "reason": "Données insuffisantes",
            "validation_keys": combo_labels or [],
        },
    )
    if combo_labels and not entry.get("validation_keys"):
        entry["validation_keys"] = combo_labels
    return entry


def _record_projection(
    results: dict,
    key: str,
    label: str,
    pace: float | None,
    chrono: float | None,
    source: str | None,
    mode: str,
):
    if pace is None or chrono is None:
        return False
    entry = _ensure_projection_entry(results, key, label)
    entry.update(
        {
            "available": True,
            "pace_s_per_km": pace,
            "pace_str": _format_pace_label(pace),
            "chrono_seconds": chrono,
            "chrono_str": _format_time_label(chrono),
            "source": source,
            "mode": mode,
        }
    )
    return True


def _projection_available(entry: dict | None) -> bool:
    return bool(entry and entry.get("available"))


def compute_distance_projections(series_matrix: dict | None) -> list[dict]:
    lookup = _build_series_lookup(series_matrix)
    target_cfg = {
        "10k": {
            "label": "10 km",
            "distance": 10.0,
            "combos": [(1000, 8), (2000, 3), (3000, 2)],
            "multiplier": 1.015,
        },
        "half": {
            "label": "Semi-marathon",
            "distance": 21.0975,
            "combos": [(3000, 3), (5000, 2), (10000, 1)],
            "multiplier": 1.005,
        },
        "5k": {
            "label": "5 km",
            "distance": 5.0,
            "combos": [(1000, 5), (800, 10), (400, 20)],
            "multiplier": 1.01,
        },
        "marathon": {
            "label": "Marathon",
            "distance": 42.195,
            "combos": [(15000, 1), (10000, 1), (5000, 2)],
            "multiplier": 1.10,
        },
    }
    for cfg in target_cfg.values():
        cfg["combo_labels"] = [
            _format_series_source(distance_m, reps) for distance_m, reps in cfg["combos"]
        ]
    results: dict[str, dict] = {}

    order = ["10k", "half", "5k", "marathon"]
    for key in order:
        cfg = target_cfg[key]
        entry = _ensure_projection_entry(results, key, cfg["label"], cfg["combo_labels"])
        direct = _apply_direct_projection(lookup, cfg["combos"], cfg["distance"], cfg["multiplier"])
        if direct:
            pace, chrono, source = direct
            _record_projection(results, key, cfg["label"], pace, chrono, source, "direct")

    def get_time(key: str) -> float | None:
        entry = results.get(key)
        if entry and entry.get("available"):
            return entry.get("chrono_seconds")
        return None

    def fallback_10k() -> bool:
        entry = results.get("10k")
        if _projection_available(entry):
            return False
        time_5k = get_time("5k")
        time_half = get_time("half")
        target = target_cfg["10k"]
        chrono = pace = None
        source = None
        if time_5k and time_half:
            exponent = math.log(time_half / time_5k) / math.log(21.0975 / 5.0)
            chrono = time_5k * (target["distance"] / 5.0) ** exponent
            source = "Interpolation 5 km / semi"
        elif time_5k:
            chrono = time_5k * (target["distance"] / 5.0) ** 1.06
            source = "Projection depuis 5 km"
        elif time_half:
            chrono = time_half * (target["distance"] / 21.0975) ** 1.06
            source = "Projection depuis semi"
        if chrono:
            pace = chrono / target["distance"]
            return _record_projection(results, "10k", target["label"], pace, chrono, source, "fallback")
        return False

    def fallback_half() -> bool:
        entry = results.get("half")
        if _projection_available(entry):
            return False
        time_10k = get_time("10k")
        time_marathon = get_time("marathon")
        target = target_cfg["half"]
        chrono = pace = None
        source = None
        if time_10k and time_marathon:
            exponent = math.log(time_marathon / time_10k) / math.log(42.195 / 10.0)
            chrono = time_10k * (target["distance"] / 10.0) ** exponent
            source = "Interpolation 10 km / marathon"
        elif time_10k:
            chrono = time_10k * (target["distance"] / 10.0) ** 1.06
            source = "Projection depuis 10 km"
        elif time_marathon:
            chrono = time_marathon * (target["distance"] / 42.195) ** 1.06
            source = "Projection depuis marathon"
        if chrono:
            pace = chrono / target["distance"]
            return _record_projection(results, "half", target["label"], pace, chrono, source, "fallback")
        return False

    def fallback_5k() -> bool:
        entry = results.get("5k")
        if _projection_available(entry):
            return False
        time_10k = get_time("10k")
        if not time_10k:
            return False
        target = target_cfg["5k"]
        chrono = time_10k * (target["distance"] / 10.0) ** 1.06
        pace = chrono / target["distance"]
        return _record_projection(results, "5k", target["label"], pace, chrono, "Projection depuis 10 km", "fallback")

    def fallback_marathon() -> bool:
        entry = results.get("marathon")
        if _projection_available(entry):
            return False
        time_half = get_time("half")
        if not time_half:
            return False
        target = target_cfg["marathon"]
        chrono = time_half * (target["distance"] / 21.0975) ** 1.06
        pace = chrono / target["distance"]
        return _record_projection(results, "marathon", target["label"], pace, chrono, "Projection depuis semi", "fallback")

    changed = True
    while changed:
        changed = False
        if fallback_10k():
            changed = True
        if fallback_half():
            changed = True
        if fallback_5k():
            changed = True
        if fallback_marathon():
            changed = True

    output = []
    for key in ["5k", "10k", "half", "marathon"]:
        if key in results:
            output.append(results[key])
        else:
            output.append(
                {
                    "target": key,
                    "label": target_cfg[key]["label"],
                    "available": False,
                    "reason": "Données insuffisantes",
                    "validation_keys": target_cfg[key]["combo_labels"],
                }
            )
    return output


def get_cached_runner_profile(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
) -> dict | None:
    sport = canonicalize_sport_label(sport)
    q = (
        db.query(models.RunnerProfileMonthly)
        .filter(
            models.RunnerProfileMonthly.user_id == user_id,
            sport_column_condition(models.RunnerProfileMonthly.sport, sport),
            models.RunnerProfileMonthly.metric_scope == "slope_zone",
        )
    )

    month_from = _month_floor_date(date_from)
    if month_from is not None:
        q = q.filter(models.RunnerProfileMonthly.year_month >= month_from)

    if date_to is not None:
        month_to = _month_floor_date(date_to)
        if month_to is not None:
            q = q.filter(models.RunnerProfileMonthly.year_month < _next_month_date(month_to))

    rows = q.all()
    if not rows:
        return None

    agg: dict[tuple[str, str], dict[str, float]] = {}

    for row in rows:
        hr_zone = row.hr_zone
        slope_band = row.slope_band
        if not hr_zone or not slope_band:
            continue

        cell = agg.setdefault(
            (hr_zone, slope_band),
            {
                "duration_sec": 0.0,
                "num_points": 0.0,
                "distance_m": 0.0,
                "elevation_gain_m": 0.0,
                "sum_vam_x_dur": 0.0,
                "dur_for_vam": 0.0,
                "sum_cad_x_dur": 0.0,
                "dur_for_cad": 0.0,
                "sum_vel_x_dur": 0.0,
                "dur_for_vel": 0.0,
                "sum_pace_x_dur": 0.0,
                "dur_for_pace": 0.0,
            },
        )

        cell["duration_sec"] += float(row.total_duration_sec or 0.0)
        cell["num_points"] += float(row.total_points or 0.0)
        cell["distance_m"] += float(row.total_distance_m or 0.0)
        cell["elevation_gain_m"] += float(row.total_elevation_gain_m or 0.0)

        if row.sum_vam_x_duration is not None:
            cell["sum_vam_x_dur"] += float(row.sum_vam_x_duration)
            cell["dur_for_vam"] += float(row.vam_duration_sec or 0.0)

        if row.sum_cadence_x_duration is not None:
            cell["sum_cad_x_dur"] += float(row.sum_cadence_x_duration)
            cell["dur_for_cad"] += float(row.cadence_duration_sec or 0.0)

        if row.sum_velocity_x_duration is not None:
            cell["sum_vel_x_dur"] += float(row.sum_velocity_x_duration)
            cell["dur_for_vel"] += float(row.velocity_duration_sec or 0.0)

        if row.sum_pace_x_duration is not None:
            cell["sum_pace_x_dur"] += float(row.sum_pace_x_duration)
            cell["dur_for_pace"] += float(row.pace_duration_sec or 0.0)

    if not agg:
        return None

    zones: dict[str, dict[str, dict]] = {}

    def _safe_avg(sum_val: float, dur: float) -> float | None:
        if dur <= 0:
            return None
        return sum_val / dur

    for (hr_zone, slope_band), vals in agg.items():
        hr_dict = zones.setdefault(hr_zone, {})
        hr_dict[slope_band] = {
            "duration_sec": int(vals["duration_sec"]),
            "num_points": int(vals["num_points"]),
            "distance_m": vals["distance_m"],
            "elevation_gain_m": vals["elevation_gain_m"],
            "avg_vam_m_per_h": _safe_avg(vals["sum_vam_x_dur"], vals["dur_for_vam"]),
            "avg_cadence_spm": _safe_avg(vals["sum_cad_x_dur"], vals["dur_for_cad"]),
            "avg_velocity_m_s": _safe_avg(vals["sum_vel_x_dur"], vals["dur_for_vel"]),
            "pace_duration_sec": int(vals["dur_for_pace"]),
            "avg_pace_s_per_km": _safe_avg(vals["sum_pace_x_dur"], vals["dur_for_pace"]),
        }

    best_pace_by_slope: dict[str, float] = {}
    for hr_zone, slopes in zones.items():
        for slope_band, cell in slopes.items():
            pace = cell.get("avg_pace_s_per_km")
            if pace is None or pace <= 0:
                continue
            current_best = best_pace_by_slope.get(slope_band)
            if current_best is None or pace < current_best:
                best_pace_by_slope[slope_band] = pace

    def _format_pace(sec: float | None) -> str | None:
        if sec is None or sec <= 0:
            return None
        total = int(round(sec))
        m, s = divmod(total, 60)
        return f"{m:02d}:{s:02d}"

    for hr_zone, slopes in zones.items():
        for slope_band, cell in slopes.items():
            pace = cell.get("avg_pace_s_per_km")
            best = best_pace_by_slope.get(slope_band)
            rel_pct = None
            if pace is not None and pace > 0 and best is not None and best > 0:
                rel_pct = (pace - best) / best * 100.0
            cell["rel_pace_pct"] = rel_pct
            cell["pace_str"] = _format_pace(pace)

    return {
        "user_id": user_id,
        "sport": sport,
        "period": {"from": date_from, "to": date_to},
        "zones": zones,
    }


def get_cached_volume_weekly_summary(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
) -> dict | None:
    sport = canonicalize_sport_label(sport)
    q = (
        db.query(models.RunnerProfileMonthly)
        .filter(
            models.RunnerProfileMonthly.user_id == user_id,
            sport_column_condition(models.RunnerProfileMonthly.sport, sport),
            models.RunnerProfileMonthly.metric_scope == "volume_weekly",
        )
    )

    month_from = _month_floor_date(date_from)
    if month_from is not None:
        q = q.filter(models.RunnerProfileMonthly.year_month >= month_from)

    if date_to is not None:
        month_to = _month_floor_date(date_to)
        if month_to is not None:
            q = q.filter(models.RunnerProfileMonthly.year_month < _next_month_date(month_to))

    rows = q.order_by(models.RunnerProfileMonthly.year_month.asc()).all()
    if not rows:
        return None

    history = []
    weekly_values: list[float] = []
    session_values: list[float] = []
    divisor = VOLUME_WINDOW_DAYS / 7.0 if VOLUME_WINDOW_DAYS else 4.0

    for row in rows:
        extra = row.extra if isinstance(row.extra, dict) else {}
        weekly_km = extra.get("weekly_volume_km")
        if weekly_km is None:
            weekly_km = (row.total_distance_m or 0.0) / 1000.0

        activities_count = extra.get("activities_count")
        if activities_count is None:
            activities_count = row.total_points or 0

        sessions_per_week = None
        if activities_count is not None:
            sessions_per_week = activities_count / divisor if divisor else activities_count

        long_run = extra.get("long_run") if isinstance(extra, dict) else None
        long_run_km = None
        long_run_date = None
        if isinstance(long_run, dict):
            long_run_km = long_run.get("distance_km")
            long_run_date = _parse_iso_datetime(long_run.get("date"))

        history.append(
            {
                "month": row.year_month,
                "weekly_km": weekly_km,
                "sessions_per_week": sessions_per_week,
                "activities_count": activities_count,
                "long_run_km": long_run_km,
                "long_run_date": long_run_date,
            }
        )

        if weekly_km is not None:
            weekly_values.append(float(weekly_km))
        if sessions_per_week is not None:
            session_values.append(float(sessions_per_week))

    def _avg(values: list[float]) -> float | None:
        return sum(values) / len(values) if values else None

    avg_weekly_km = _avg(weekly_values)
    avg_sessions = _avg(session_values)
    latest = history[-1]
    recent_long_run = next((item for item in reversed(history) if item["long_run_km"]), None)

    return {
        "history": history,
        "avg_weekly_km": avg_weekly_km,
        "avg_sessions_per_week": avg_sessions,
        "latest_weekly_km": latest.get("weekly_km"),
        "latest_sessions_per_week": latest.get("sessions_per_week"),
        "recent_long_run": recent_long_run,
        "window_days": VOLUME_WINDOW_DAYS,
    }


def _get_or_create_runner_profile_row(
    db: Session,
    *,
    user_id: int,
    sport: str,
    month: dt.date,
    metric_scope: str,
    slope_band: str | None = None,
    hr_zone: str | None = None,
    window_label: str | None = None,
) -> models.RunnerProfileMonthly:
    sport = canonicalize_sport_label(sport)
    rows = (
        db.query(models.RunnerProfileMonthly)
        .filter(
            models.RunnerProfileMonthly.user_id == user_id,
            sport_column_condition(models.RunnerProfileMonthly.sport, sport),
            models.RunnerProfileMonthly.year_month == month,
            models.RunnerProfileMonthly.metric_scope == metric_scope,
            models.RunnerProfileMonthly.slope_band == slope_band,
            models.RunnerProfileMonthly.hr_zone == hr_zone,
            models.RunnerProfileMonthly.window_label == window_label,
        )
        .order_by(models.RunnerProfileMonthly.updated_at.desc().nulls_last(), models.RunnerProfileMonthly.id.desc())
        .all()
    )

    row = rows[0] if rows else None

    # Si des doublons existent (absence de contrainte unique côté DB), on garde le plus récent et on purge les autres.
    if rows and len(rows) > 1:
        logger.warning(
            "[RPM] Purge doublons user_id=%s sport=%s month=%s scope=%s slope=%s hr_zone=%s window=%s (n=%s)",
            user_id, sport, month, metric_scope, slope_band, hr_zone, window_label, len(rows),
        )
        for dup in rows[1:]:
            db.delete(dup)

    if row is None:
        row = models.RunnerProfileMonthly(
            user_id=user_id,
            sport=sport,
            year_month=month,
            metric_scope=metric_scope,
            slope_band=slope_band,
            hr_zone=hr_zone,
            window_label=window_label,
            total_duration_sec=0.0,
            total_distance_m=0.0,
            total_elevation_gain_m=0.0,
            total_points=0,
            sum_pace_x_duration=0.0,
            pace_duration_sec=0.0,
            sum_vam_x_duration=0.0,
            vam_duration_sec=0.0,
            sum_cadence_x_duration=0.0,
            cadence_duration_sec=0.0,
            sum_velocity_x_duration=0.0,
            velocity_duration_sec=0.0,
            dplus_total_m=0.0,
            dminus_total_m=0.0,
            extra={},
        )
        db.add(row)
    return row


def _update_runner_profile_slope_zone(
    db: Session,
    *,
    activity: models.Activity,
    sport: str,
    month: dt.date,
):
    rows = (
        db.query(models.ActivityZoneSlopeAgg)
        .filter(models.ActivityZoneSlopeAgg.activity_id == activity.id)
        .all()
    )
    if not rows:
        return

    def _safe_avg(sum_val: float, dur: float) -> float | None:
        if dur <= 0:
            return None
        return sum_val / dur

    for agg in rows:
        hr_zone = agg.hr_zone
        slope_band = agg.slope_band
        if not hr_zone or not slope_band:
            continue

        rpm = _get_or_create_runner_profile_row(
            db,
            user_id=activity.user_id,
            sport=sport,
            month=month,
            metric_scope="slope_zone",
            hr_zone=hr_zone,
            slope_band=slope_band,
        )

        dur = float(agg.duration_sec or 0.0)
        rpm.total_duration_sec = (rpm.total_duration_sec or 0.0) + dur
        rpm.total_distance_m = (rpm.total_distance_m or 0.0) + float(agg.distance_m or 0.0)
        rpm.total_elevation_gain_m = (rpm.total_elevation_gain_m or 0.0) + float(agg.elevation_gain_m or 0.0)
        rpm.total_points = int((rpm.total_points or 0) + int(agg.num_points or 0))

        if agg.avg_vam_m_per_h is not None:
            rpm.sum_vam_x_duration = (rpm.sum_vam_x_duration or 0.0) + float(agg.avg_vam_m_per_h) * dur
            rpm.vam_duration_sec = (rpm.vam_duration_sec or 0.0) + dur

        if agg.avg_cadence_spm is not None:
            rpm.sum_cadence_x_duration = (rpm.sum_cadence_x_duration or 0.0) + float(agg.avg_cadence_spm) * dur
            rpm.cadence_duration_sec = (rpm.cadence_duration_sec or 0.0) + dur

        if agg.avg_velocity_m_s is not None:
            rpm.sum_velocity_x_duration = (rpm.sum_velocity_x_duration or 0.0) + float(agg.avg_velocity_m_s) * dur
            rpm.velocity_duration_sec = (rpm.velocity_duration_sec or 0.0) + dur

        if agg.avg_pace_s_per_km is not None:
            rpm.sum_pace_x_duration = (rpm.sum_pace_x_duration or 0.0) + float(agg.avg_pace_s_per_km) * dur
            rpm.pace_duration_sec = (rpm.pace_duration_sec or 0.0) + dur

        rpm.avg_vam_m_per_h = _safe_avg(rpm.sum_vam_x_duration or 0.0, rpm.vam_duration_sec or 0.0)
        rpm.avg_cadence_spm = _safe_avg(rpm.sum_cadence_x_duration or 0.0, rpm.cadence_duration_sec or 0.0)
        rpm.avg_velocity_m_s = _safe_avg(rpm.sum_velocity_x_duration or 0.0, rpm.velocity_duration_sec or 0.0)
        rpm.avg_pace_s_per_km = _safe_avg(rpm.sum_pace_x_duration or 0.0, rpm.pace_duration_sec or 0.0)


def _load_distance_time_stream(db: Session, activity_id: int) -> list[tuple[float, float]]:
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
            # Distance reset / GPS glitch -> on ignore ce point
            continue
        stream.append((dist, elapsed))
        last_dist = dist
    return stream


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


def _select_best_segments_no_overlap(segments: list[dict], reps: int) -> list[dict] | None:
    if reps <= 0 or len(segments) < reps:
        return None

    segments_sorted = sorted(segments, key=lambda s: s["end_time"])
    end_times = [seg["end_time"] for seg in segments_sorted]
    prev_idx: list[int] = []

    for i, seg in enumerate(segments_sorted):
        start = seg["start_time"]
        j = bisect.bisect_right(end_times, start, hi=i) - 1
        prev_idx.append(j)

    n = len(segments_sorted)
    INF = float("inf")

    dp = [[INF] * (reps + 1) for _ in range(n + 1)]
    choice = [[False] * (reps + 1) for _ in range(n + 1)]
    jump = [[0] * (reps + 1) for _ in range(n + 1)]

    for i in range(n + 1):
        dp[i][0] = 0.0

    for i in range(1, n + 1):
        seg = segments_sorted[i - 1]
        for c in range(1, reps + 1):
            # Option 1 : hériter du meilleur coût précédent (ne pas prendre ce segment)
            dp[i][c] = dp[i - 1][c]
            choice[i][c] = False

            prev_row = prev_idx[i - 1] + 1
            prev_cost = dp[prev_row][c - 1]
            if prev_cost == INF:
                continue

            include_cost = seg["pace_s_per_km"] + prev_cost
            if include_cost < dp[i][c]:
                dp[i][c] = include_cost
                choice[i][c] = True
                jump[i][c] = prev_row

    if dp[n][reps] == INF:
        return None

    selected_indices: list[int] = []
    i, c = n, reps
    while i > 0 and c > 0:
        if choice[i][c]:
            selected_indices.append(i - 1)
            i = jump[i][c]
            c -= 1
        else:
            i -= 1

    if c != 0:
        return None

    selected_indices.sort()
    return [segments_sorted[idx] for idx in selected_indices]


def _extract_series_from_segments(
    segments: list[dict],
    distance_m: float,
) -> dict[int, dict]:
    if not segments:
        return {}

    best_by_reps: dict[int, dict] = {}

    reps_list = [1] + list(SERIES_REPETITION_COUNTS)

    for reps in reps_list:
        selection = _select_best_segments_no_overlap(segments, reps)
        if not selection:
            continue

        pace_values = [s["pace_s_per_km"] for s in selection]
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

        duration = sum(s["duration_sec"] for s in selection)
        start_offset = min(s["start_time"] for s in selection)
        entry = {
            "distance_m": distance_m,
            "reps": reps,
            "avg_pace_s_per_km": avg_pace,
            "stability_ratio": stability,
            "duration_sec": duration,
            "start_offset_sec": start_offset,
        }
        best_by_reps[reps] = entry

    return best_by_reps


def _extract_activity_series_splits(db: Session, activity: models.Activity) -> list[dict]:
    stream = _load_distance_time_stream(db, activity.id)
    if not stream:
        return []

    total_distance = stream[-1][0]
    if total_distance < min(SERIES_DISTANCE_TARGETS_M):
        return []

    series_entries: list[dict] = []

    for distance_m in SERIES_DISTANCE_TARGETS_M:
        segments = _build_distance_segments(stream, distance_m)
        if not segments:
            continue

        series_by_reps = _extract_series_from_segments(segments, distance_m)
        for reps, data in series_by_reps.items():
            entry = data.copy()
            entry["activity_id"] = activity.id
            series_entries.append(entry)

    return series_entries


def _update_runner_profile_series_splits(
    db: Session,
    *,
    activity: models.Activity,
    sport: str,
    month: dt.date,
):
    if sport != "run":
        return

    series_entries = _extract_activity_series_splits(db, activity)
    if not series_entries:
        return

    activity_date_iso = _isoformat(_safe_dt(activity.start_date))

    for entry in series_entries:
        duration = float(entry.get("duration_sec") or 0.0)
        avg_pace = float(entry.get("avg_pace_s_per_km") or 0.0)
        if duration <= 0 or avg_pace <= 0:
            continue

        distance_m = int(entry["distance_m"])
        reps = int(entry["reps"])

        distance_key = f"D{distance_m}"
        reps_key = f"R{reps}"

        rpm = _get_or_create_runner_profile_row(
            db,
            user_id=activity.user_id,
            sport=sport,
            month=month,
            metric_scope="series_splits",
            slope_band=distance_key,
            hr_zone=reps_key,
        )

        rpm.total_duration_sec = (rpm.total_duration_sec or 0.0) + duration
        rpm.total_distance_m = (rpm.total_distance_m or 0.0) + (distance_m * reps)
        rpm.total_points = int((rpm.total_points or 0) + reps)
        rpm.sum_pace_x_duration = (rpm.sum_pace_x_duration or 0.0) + (avg_pace * duration)
        rpm.pace_duration_sec = (rpm.pace_duration_sec or 0.0) + duration
        rpm.avg_pace_s_per_km = _safe_avg(rpm.sum_pace_x_duration or 0.0, rpm.pace_duration_sec or 0.0)

        extra = rpm.extra or {}
        extra["distance_m"] = distance_m
        extra["reps"] = reps
        extra["series_count"] = int(extra.get("series_count", 0)) + 1

        stability_samples = extra.get("stability_samples") or []
        if entry.get("stability_ratio") is not None:
            stability_samples.append(entry["stability_ratio"])
            stability_samples = stability_samples[-50:]
        extra["stability_samples"] = stability_samples

        samples = extra.get("samples") or []
        samples.append(
            {
                "activity_id": entry["activity_id"],
                "avg_pace_s_per_km": avg_pace,
                "duration_sec": duration,
                "start_offset_sec": entry.get("start_offset_sec"),
                "stability_ratio": entry.get("stability_ratio"),
                "date": activity_date_iso,
            }
        )
        samples = samples[-20:]
        extra["samples"] = samples

        best_series = extra.get("best_series")
        if not best_series or avg_pace < best_series.get("avg_pace_s_per_km", float("inf")):
            extra["best_series"] = {
                "avg_pace_s_per_km": avg_pace,
                "activity_id": entry["activity_id"],
                "duration_sec": duration,
                "start_offset_sec": entry.get("start_offset_sec"),
                "stability_ratio": entry.get("stability_ratio"),
                "date": activity_date_iso,
            }

        rpm.extra = extra


def _compute_recent_volume_metrics(
    db: Session,
    *,
    user_id: int,
    reference_date: dt.datetime,
) -> dict:
    ref = _safe_dt(reference_date)
    window_start = ref - dt.timedelta(days=VOLUME_WINDOW_DAYS)

    activities = (
        db.query(models.Activity)
        .filter(models.Activity.user_id == user_id)
        .filter(models.Activity.sport.in_(RUN_VOLUME_SPORTS))
        .filter(models.Activity.start_date >= window_start)
        .filter(models.Activity.start_date <= ref)
        .all()
    )

    if not activities:
        return {
            "weekly_volume_km": 0.0,
            "activities_count": 0,
            "long_run_km": None,
            "long_run_date": None,
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
                "date": _isoformat(_safe_dt(act.start_date)),
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


def _update_runner_profile_volume_weekly(
    db: Session,
    *,
    activity: models.Activity,
    sport: str,
    month: dt.date,
):
    if sport not in RUN_VOLUME_SPORTS:
        return

    metrics = _compute_recent_volume_metrics(
        db,
        user_id=activity.user_id,
        reference_date=_safe_dt(activity.start_date),
    )

    target_sport = "run"
    rpm = _get_or_create_runner_profile_row(
        db,
        user_id=activity.user_id,
        sport=target_sport,
        month=month,
        metric_scope="volume_weekly",
    )

    weekly_km = metrics.get("weekly_volume_km") or 0.0
    rpm.total_distance_m = weekly_km * 1000.0
    rpm.total_points = metrics.get("activities_count") or 0

    extra = rpm.extra or {}
    extra.update(metrics)
    rpm.extra = extra


def _update_runner_profile_ascent_windows(
    db: Session,
    *,
    activity: models.Activity,
    sport: str,
    month: dt.date,
):
    windows = compute_best_dplus_windows(
        db,
        user_id=activity.user_id,
        sport=sport,
        activity_ids=[activity.id],
    )
    if not windows:
        return

    for win in windows:
        gain = win.get("gain_m") or 0.0
        window_id = win.get("window_id")
        if not window_id or gain <= 0:
            continue

        rpm = _get_or_create_runner_profile_row(
            db,
            user_id=activity.user_id,
            sport=sport,
            month=month,
            metric_scope="ascent_window",
            window_label=window_id,
        )

        current_gain = rpm.dplus_total_m or 0.0
        if gain <= current_gain:
            continue

        rpm.dplus_total_m = gain
        rpm.dminus_total_m = win.get("loss_m") or 0.0
        rpm.total_duration_sec = win.get("duration_sec") or 0.0
        rpm.total_distance_m = (win.get("distance_km") or 0.0) * 1000.0
        rpm.avg_vam_m_per_h = win.get("gain_per_hour")

        extra = rpm.extra or {}
        extra.update(
            {
                "label": win.get("label"),
                "activity_id": win.get("activity_id") or activity.id,
                "activity_name": win.get("activity_name") or activity.name,
                "start_datetime": _isoformat(win.get("start_datetime")),
                "end_datetime": _isoformat(win.get("end_datetime")),
                "start_offset_sec": win.get("start_offset_sec"),
                "end_offset_sec": win.get("end_offset_sec"),
            }
        )
        rpm.extra = extra


def _get_activity_zone_mix(db: Session, activity_id: int) -> dict[str, float]:
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


def _classify_activity_profile_cache(zone_durations: dict[str, float]) -> str | None:
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


def _update_runner_profile_glucose_activity(
    db: Session,
    *,
    activity: models.Activity,
    sport: str,
    month: dt.date,
    stats: dict | None,
):
    if not stats:
        return

    avg = stats.get("avg")
    if avg is None:
        return

    rpm = _get_or_create_runner_profile_row(
        db,
        user_id=activity.user_id,
        sport=sport,
        month=month,
        metric_scope="glucose_activity",
    )

    zone_mix = _get_activity_zone_mix(db, activity.id)
    profile_key = _classify_activity_profile_cache(zone_mix)

    entry = {
        "activity_id": activity.id,
        "name": activity.name,
        "start_mgdl": stats.get("start_mgdl"),
        "end_mgdl": stats.get("end_mgdl"),
        "avg_mgdl": avg,
        "tir_percent": stats.get("pct_in_range"),
        "profile": profile_key,
        "start_ts": _isoformat(_safe_dt(activity.start_date)),
        "distance_km": (activity.distance or 0.0) / 1000.0 if activity.distance else None,
        "elevation_gain_m": float(activity.total_elevation_gain) if activity.total_elevation_gain is not None else None,
        "duration_sec": activity.elapsed_time,
    }

    extra = rpm.extra or {}
    activities = extra.get("activities") or []
    activities.append(entry)
    if len(activities) > 100:
        activities = activities[-100:]
    extra["activities"] = activities

    profile_stats = extra.get("profile_stats") or {
        "endurance": {"sum_avg_mgdl": 0.0, "count": 0},
        "seuil": {"sum_avg_mgdl": 0.0, "count": 0},
        "fractionne": {"sum_avg_mgdl": 0.0, "count": 0},
    }
    if profile_key in profile_stats and avg is not None:
        profile_stats[profile_key]["sum_avg_mgdl"] += float(avg)
        profile_stats[profile_key]["count"] += 1
    extra["profile_stats"] = profile_stats

    rpm.extra = extra
    rpm.total_duration_sec = (rpm.total_duration_sec or 0.0) + float(activity.elapsed_time or 0.0)


def update_runner_profile_monthly_from_activity(
    db: Session,
    *,
    activity: models.Activity,
    stats: dict | None = None,
) -> None:
    if not activity or not activity.start_date:
        return

    month = _month_floor_date(_safe_dt(activity.start_date))
    if month is None:
        return

    sport = activity.sport or normalize_activity_type(activity.activity_type)
    if not sport:
        sport = "run"
    sport = canonicalize_sport_label(sport)

    logger.info(
        "[RPM] Maj profils mensuels user_id=%s activity_id=%s sport=%s month=%s",
        activity.user_id, activity.id, sport, month,
    )

    _update_runner_profile_slope_zone(db, activity=activity, sport=sport, month=month)
    _update_runner_profile_series_splits(db, activity=activity, sport=sport, month=month)
    _update_runner_profile_volume_weekly(db, activity=activity, sport=sport, month=month)
    _update_runner_profile_ascent_windows(db, activity=activity, sport=sport, month=month)
    _update_runner_profile_glucose_activity(db, activity=activity, sport=sport, month=month, stats=stats)
    # En cas de purge de doublons pendant les mises à jour, on persiste.
    db.commit()
    logger.info(
        "[RPM] Maj profils mensuels OK user_id=%s activity_id=%s sport=%s month=%s",
        activity.user_id, activity.id, sport, month,
    )


def get_cached_glucose_activity_summary(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    limit: int = 20,
) -> dict | None:
    sport = canonicalize_sport_label(sport)
    q = (
        db.query(models.RunnerProfileMonthly)
        .filter(
            models.RunnerProfileMonthly.user_id == user_id,
            sport_column_condition(models.RunnerProfileMonthly.sport, sport),
            models.RunnerProfileMonthly.metric_scope == "glucose_activity",
        )
    )

    rows = q.all()
    if not rows:
        return None

    activities: list[dict] = []
    profile_stats = {
        "endurance": {"sum_avg_mgdl": 0.0, "count": 0},
        "seuil": {"sum_avg_mgdl": 0.0, "count": 0},
        "fractionne": {"sum_avg_mgdl": 0.0, "count": 0},
    }

    for row in rows:
        extra = row.extra or {}
        for entry in extra.get("activities") or []:
            start_ts = _parse_iso_datetime(entry.get("start_ts"))
            activities.append(
                {
                    "activity_id": entry.get("activity_id"),
                    "start_mgdl": entry.get("start_mgdl"),
                    "end_mgdl": entry.get("end_mgdl"),
                    "avg_mgdl": entry.get("avg_mgdl"),
                    "tir_percent": entry.get("tir_percent"),
                    "profile": entry.get("profile"),
                    "start_ts": start_ts,
                    "distance_km": entry.get("distance_km"),
                    "elevation_gain_m": entry.get("elevation_gain_m"),
                    "duration_sec": entry.get("duration_sec"),
                }
            )

        stats = extra.get("profile_stats") or {}
        for key in profile_stats.keys():
            data = stats.get(key) or {}
            profile_stats[key]["sum_avg_mgdl"] += float(data.get("sum_avg_mgdl") or 0.0)
            profile_stats[key]["count"] += int(data.get("count") or 0)

    if not activities:
        return {"activities": [], "profile_stats": profile_stats}

    activities.sort(key=lambda x: x.get("start_ts") or dt.datetime.min, reverse=True)
    limited = activities[:limit]

    return {
        "activities": limited,
        "profile_stats": profile_stats,
    }


#---------------------------------------------------------------------------
def compute_best_dplus_windows(
    db: Session,
    *,
    user_id: int,
    sport: str = "run",
    date_from: dt.datetime | None = None,
    date_to: dt.datetime | None = None,
    activity_ids: list[int] | None = None,
) -> list[dict]:
    """Retourne le meilleur D+ cumulé sur des fenêtres glissantes (1h,2h,5h,10h,24h)."""
    sport = canonicalize_sport_label(sport)

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
        sport_column_condition(models.Activity.sport, sport),
    )

    if activity_ids:
        activities_q = activities_q.filter(models.Activity.id.in_(activity_ids))
    else:
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
