# app/main.py
# -----------------------------------------------------------------------------
# Ce module constitue le cœur de l’application FastAPI :
# il assemble l’API, la logique métier et la mini interface web utilisateur.
#
# Il orchestre désormais :
#
# 🔗 Les connexions externes
#   - Strava (OAuth, webhooks, récupération des activités + streams).
#   - LibreLinkUp & Dexcom (lecture continue des données de glycémie).
#
# 🧠 La logique métier (via `logic.py` & services associés)
#   - Lecture des activités Strava et de leurs streams (temps, FC, altitude, GPS…).
#   - Lecture des courbes de glycémie (Libre / Dexcom) + stockage en base
#     dans `glucose_points` avec normalisation en UTC et gestion des doublons.
#   - Appel aux fonctions de `logic.py` pour :
#       • sélectionner la fenêtre temporelle d’analyse,
#       • calculer les stats glycémie (moyenne, min, max, TIR, hypos/hypers…),
#       • calculer les zones de fréquence cardiaque personnalisées
#         (en fonction du profil utilisateur et de sa FC max),
#       • fusionner proprement la description Strava existante avec
#         un bloc de résumé auto (glycémie, cardio, zones…),
#       • enregistrer en base l’activité + les points de stream détaillés.
#
# ⚙️ L’orchestration CGM & Strava
#   - Endpoint webhook Strava (`/webhooks/strava`) pour traiter automatiquement
#     les créations / mises à jour d’activités.
#   - Fonction centrale `enrich_activity(...)` qui :
#       • récupère l’activité Strava,
#       • récupère la courbe CGM en respectant la préférence utilisateur
#         (`cgm_source` = Auto / Libre / Dexcom avec mécanisme de fallback),
#       • projette la glycémie sur la timeline Strava,
#       • calcule stats + zones,
#       • met à jour la description sur Strava,
#       • persiste activités + streams en base.
#   - Démarrage au `startup` d’un thread de polling CGM continu
#     (via `run_polling_loop`) après initialisation de la base.
#
# 🧩 Authentification & gestion des utilisateurs
#   - Auth “générale” (signup/login, mots de passe hashés, JWT ou autre)
#     via `app.auth`.
#   - Routes dédiées d’auth Strava et Dexcom (routers `auth_strava` / `auth_dexcom`).
#   - Gestion des identifiants LibreLinkUp par utilisateur (API + UI),
#     avec test rapide des credentials.
#
# 🖥️ Mini interface web (HTML / Jinja2, sous `/ui`)
#   - Page d’accueil / listing des utilisateurs.
#   - Signup / login UI.
#   - Écran de bienvenue après inscription, avec proposition de connexion Strava.
#   - Profil utilisateur :
#       • infos de base (nom, email, localisation),
#       • paramètres physiologiques (date de naissance, sexe, FC max, taille, poids),
#       • statut d’abonnement (flag `is_pro`),
#       • choix de la source CGM (Auto / Libre / Dexcom),
#       • gestion des connexions / déconnexions Strava, Libre, Dexcom,
#       • upload de photo de profil (avatars dans `static/avatars`).
#   - Dashboard utilisateur :
#       • liste des activités enregistrées,
#       • enrichissement de la dernière activité en un clic.
#   - Détail d’une activité :
#       • résumé global (distance, D+, FC, etc.),
#       • résumé glycémie,
#       • graphique FC / altitude vs temps (à partir de `ActivityStreamPoint`),
#       • tracé GPS sur carte,
#       • nuage de points pente (%) vs cadence
#         via `app.indicators.slope_cadence.build_slope_cadence_data`.
#   - Suppression d’une activité (et de ses streams) côté base.
#
# 🛠️ Divers
#   - Routes de debug (healthcheck, dernière activité Strava, dump des activités en base…).
#   - Gestion des fichiers statiques (`/static`) et des templates (`templates/`).
#
# En résumé : `main.py` est l’orchestrateur principal de l’API, du polling CGM,
# de la synchronisation Strava et de la petite UI web de visualisation.
# -----------------------------------------------------------------------------

import os
import asyncio
import datetime as dt
import subprocess
import secrets
import hashlib
import struct
import zlib
import smtplib
from email.message import EmailMessage
from io import BytesIO
import json
from html import escape
from typing import Optional
import statistics
import threading
import shutil
import logging
import time
import tempfile
import uuid
from datetime import datetime, timedelta
from collections import Counter, defaultdict, deque
import re
from urllib.parse import quote_plus, urlencode, urlsplit
from bisect import bisect_left
from functools import lru_cache
from sqlalchemy import desc, func, and_
from sqlalchemy.orm import Session, selectinload

try:
    from fitparse import FitFile
except ImportError:  # pragma: no cover - couvert par les dépendances de production
    FitFile = None

from dotenv import load_dotenv
load_dotenv()

from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    Query,
    Body,
    Form,
    UploadFile,
    File,
    Depends,
    BackgroundTasks,
)
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from .logic import (
    select_window,
    compute_stats,
    merge_desc,
    normalize_summary_block_layout,
    compute_hr_zones,
    compute_user_fc_max,
    _format_duration,
    upsert_activity_record,
    save_activity_stream_points,
    match_glucose_to_time_stream,
    compute_and_store_zone_slope_aggs,
    compute_difficulty_and_level,   # 👈 AJOUT
    build_runner_profile,
    get_cached_runner_profile,
    compute_best_dplus_windows,
    get_cached_dplus_windows,
    get_series_splits_matrix,
    compute_distance_projections,
    get_cached_volume_weekly_summary,
    update_runner_profile_monthly_from_activity,
    get_cached_glucose_activity_summary,
    get_cached_distance_efforts,
    rebuild_runner_profile_range_from_contributions,
    sport_column_condition,
    canonicalize_sport_label,
    normalize_activity_type,
    compute_km_highlights_from_streams,
    compute_terrain_adjusted_cardiac_drift,
    backfill_signed_vertical_speed_for_activity,
    is_valid_activity_stream_interval,
    ensure_activity_meta_contribution,
    get_archived_training_summary,
    purge_old_user_activities,
    purge_old_activities_for_all_users,
    delete_activity_live_data,
    HR_ZONES,
)
from .settings import settings
from .strava_client import StravaClient
from .clubs import build_club_payload, get_available_clubs, get_club_by_slug
from .libre_client import (
    get_last_libre_status,
    clear_libre_disabled_state,
    is_libre_auth_error_message,
    set_libre_status_flag,
)
from .dexcom_client import (
    get_last_dexcom_status,
    has_dexcom_share_credentials,
    test_dexcom_credentials,
)
from app.providers.medtronic_carelink import test_connection as test_carelink_connection
from app.providers.registry import (
    get_active_glucose_source,
    get_glucose_source_label,
    set_active_glucose_source,
    test_provider_connection,
)
from app.providers.nightscout import normalize_base_url
from app.database import SessionLocal, init_db, get_db
from app.models import (
    StravaToken,
    LibreCredentials,
    User,
    Activity,
    ActivityStreamPoint,
    GlucosePoint,
    DexcomToken,
    CareLinkCredential,
    NightscoutCredential,
    UserSettings,
    ActivityEnrichmentJob,
    ActivityVamPeak,
    ActivityZoneSlopeAgg,
    CoursePlanDownload,
    CoursePlan,
    PlanPaymentAttempt,
    PlanCreditWallet,
    UserLoginEvent,
)
from app.secrets import encrypt_secret
from app.seo_content import SEO_GUIDES

SLOPE_BANDS_DEF = [
    (-999, -40, "Sneg40p", "<-40%"),
    (-40, -30, "Sneg30_40", "-40 à -30%"),
    (-30, -25, "Sneg25_30", "-30 à -25%"),
    (-25, -20, "Sneg20_25", "-25 à -20%"),
    (-20, -15, "Sneg15_20", "-20 à -15%"),
    (-15, -10, "Sneg10_15", "-15 à -10%"),
    (-10, -5, "Sneg5_10", "-10 à -5%"),
    (-5, 0, "Sneg0_5", "-5 à 0%"),
    (0, 5, "S0_5", "0–5%"),
    (5, 10, "S5_10", "5–10%"),
    (10, 15, "S10_15", "10–15%"),
    (15, 20, "S15_20", "15–20%"),
    (20, 25, "S20_25", "20–25%"),
    (25, 30, "S25_30", "25–30%"),
    (30, 40, "S30_40", "30–40%"),
    (40, 999, "S40p", ">40%"),
]
SLOPE_LABELS = {band_id: label for _min, _max, band_id, label in SLOPE_BANDS_DEF}
SLOPE_ORDER = [(band_id, label) for _min, _max, band_id, label in SLOPE_BANDS_DEF]
SLOPE_ORDER_INDEX = {band_id: index for index, (band_id, _label) in enumerate(SLOPE_ORDER)}

OFFICIAL_COURSES_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "data", "official_courses")
)


def _load_official_course_catalog() -> list[dict]:
    """Lit les fiches de courses locales sans exposer les chemins de fichiers."""
    if not os.path.isdir(OFFICIAL_COURSES_DIR):
        return []

    courses: list[dict] = []
    for filename in sorted(os.listdir(OFFICIAL_COURSES_DIR)):
        if not filename.endswith(".json"):
            continue
        json_path = os.path.join(OFFICIAL_COURSES_DIR, filename)
        try:
            with open(json_path, "r", encoding="utf-8") as handle:
                course = json.load(handle)
            course_id = str(course.get("id") or "").strip()
            route_file = os.path.basename(str(course.get("route_file") or ""))
            if not course_id or not route_file.lower().endswith(".gpx"):
                continue
            reference_route_file = os.path.basename(str(course.get("reference_route_file") or ""))
            official_route_available = os.path.isfile(os.path.join(OFFICIAL_COURSES_DIR, route_file))
            reference_route_available = bool(reference_route_file.lower().endswith(".gpx") and os.path.isfile(os.path.join(OFFICIAL_COURSES_DIR, reference_route_file)))
            event_name = str(course.get("event_name") or _course_event_name(course_id))
            courses.append(
                {
                    "id": course_id,
                    "name": course.get("name") or course_id,
                    "short_name": course.get("short_name") or course.get("name") or course_id,
                    "distance_km": course.get("distance_km"),
                    "distance_label": course.get("distance_label"),
                    "elevation_gain_m": course.get("elevation_gain_m"),
                    "elevation_loss_m": course.get("elevation_loss_m"),
                    "course_coefficient_percent": course.get("course_coefficient_percent", 100),
                    "course_pace_category": course.get("course_pace_category", ""),
                    "event_name": event_name,
                    "search_terms": event_name,
                    "route_available": official_route_available or reference_route_available,
                    "route_status": "official" if official_route_available else "previous_edition" if reference_route_available else "pending",
                    "route_edition_year": course.get("route_edition_year") or (course.get("edition_year") if official_route_available else None),
                    "points": course.get("points") or [],
                }
            )
        except (OSError, ValueError, TypeError):
            logger.warning("[COURSES] Fiche officielle ignorée: %s", filename)
    return courses


def _load_official_course(course_id: str, *, require_route: bool = True) -> dict | None:
    normalized_id = (course_id or "").strip()
    if not normalized_id or os.path.basename(normalized_id) != normalized_id:
        return None
    json_path = os.path.join(OFFICIAL_COURSES_DIR, f"{normalized_id}.json")
    if not os.path.isfile(json_path):
        return None
    try:
        with open(json_path, "r", encoding="utf-8") as handle:
            course = json.load(handle)
    except (OSError, ValueError, TypeError):
        return None
    if course.get("id") != normalized_id:
        return None
    route_file = os.path.basename(str(course.get("route_file") or ""))
    if not route_file.lower().endswith(".gpx"):
        return None
    route_path = os.path.join(OFFICIAL_COURSES_DIR, route_file)
    route_status = "official" if os.path.isfile(route_path) else "pending"
    if route_status == "pending":
        reference_route_file = os.path.basename(str(course.get("reference_route_file") or ""))
        reference_path = os.path.join(OFFICIAL_COURSES_DIR, reference_route_file)
        if reference_route_file.lower().endswith(".gpx") and os.path.isfile(reference_path):
            route_path = reference_path
            route_status = "previous_edition"
    if require_route and route_status == "pending":
        return None
    return {"course": course, "route_path": route_path if route_status != "pending" else None, "route_status": route_status}


def _seo_course_slug(course_id: str) -> str:
    return re.sub(r"-\d{4}$", "", str(course_id or "").strip().lower())


def _seo_elevation_svg(profile: list[dict]) -> str:
    """Small server-rendered elevation profile for public pages and crawlers."""
    points = [
        point for point in profile or []
        if isinstance(point, dict)
        and isinstance(point.get("distance_km"), (int, float))
        and isinstance(point.get("elevation_m"), (int, float))
    ]
    if len(points) < 2:
        return ""
    if len(points) > 220:
        step = max(1, len(points) // 220)
        points = points[::step]
        if points[-1] != profile[-1]:
            points.append(profile[-1])
    width, height, pad_x, pad_top, pad_bottom = 960, 260, 34, 26, 34
    xs = [float(point["distance_km"]) for point in points]
    ys = [float(point["elevation_m"]) for point in points]
    min_x, max_x, min_y, max_y = min(xs), max(xs), min(ys), max(ys)
    span_x, span_y = max(.01, max_x - min_x), max(1.0, max_y - min_y)
    coordinates = [
        (
            pad_x + ((x - min_x) / span_x) * (width - 2 * pad_x),
            pad_top + ((max_y - y) / span_y) * (height - pad_top - pad_bottom),
        )
        for x, y in zip(xs, ys)
    ]
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in coordinates)
    area = f"{pad_x},{height - pad_bottom} {line} {width - pad_x},{height - pad_bottom}"
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Profil altimétrique de {max_x:.1f} kilomètres">'
        '<defs><linearGradient id="seo-profile-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#ff6a49" stop-opacity=".72"/><stop offset="1" stop-color="#ffdbc9" stop-opacity=".4"/></linearGradient></defs>'
        f'<line x1="{pad_x}" y1="{height - pad_bottom}" x2="{width - pad_x}" y2="{height - pad_bottom}" stroke="#cfc9bf"/>'
        f'<polygon points="{area}" fill="url(#seo-profile-fill)"/><polyline points="{line}" fill="none" stroke="#b73d25" stroke-width="3" stroke-linejoin="round"/>'
        f'<text x="{pad_x}" y="{height - 10}" fill="#68645d" font-size="18">0 km</text><text x="{width - pad_x - 78}" y="{height - 10}" fill="#68645d" font-size="18">{max_x:.1f} km</text>'
        f'<text x="{pad_x}" y="18" fill="#68645d" font-size="18">{min_y:.0f}–{max_y:.0f} m</text></svg>'
    )


def _seo_segment_profile_svg(profile: list[dict], from_km: float, to_km: float) -> str:
    """Colored elevation profile for one aid-station-to-aid-station leg."""
    if to_km <= from_km:
        return ""
    source = [
        point for point in profile or []
        if isinstance(point, dict)
        and isinstance(point.get("distance_km"), (int, float))
        and isinstance(point.get("elevation_m"), (int, float))
    ]
    if len(source) < 2:
        return ""
    inside = [point for point in source if from_km <= float(point["distance_km"]) <= to_km]
    before = [point for point in source if float(point["distance_km"]) < from_km]
    after = [point for point in source if float(point["distance_km"]) > to_km]
    points = ([before[-1]] if before else []) + inside + ([after[0]] if after else [])
    if len(points) < 2:
        return ""
    if len(points) > 120:
        step = max(1, math.ceil(len(points) / 120))
        compacted = points[::step]
        if compacted[-1] is not points[-1]:
            compacted.append(points[-1])
        points = compacted
    width, height, pad_x, pad_top, pad_bottom = 680, 170, 20, 18, 26
    min_y = min(float(point["elevation_m"]) for point in points)
    max_y = max(float(point["elevation_m"]) for point in points)
    span_x, span_y = max(.01, to_km - from_km), max(1.0, max_y - min_y)
    def coord(point):
        x = pad_x + ((float(point["distance_km"]) - from_km) / span_x) * (width - 2 * pad_x)
        y = pad_top + ((max_y - float(point["elevation_m"])) / span_y) * (height - pad_top - pad_bottom)
        return x, y
    coords = [coord(point) for point in points]
    def color(grade):
        grade = float(grade or 0)
        if grade <= -8: return "#5bb9e6"
        if grade < -2: return "#83cceb"
        if grade < 5: return "#dfe68d"
        if grade < 12: return "#f4c446"
        if grade < 20: return "#f38b2d"
        return "#e94c34"
    base_y = height - pad_bottom
    areas = "".join(
        f'<polygon points="{previous[0]:.1f},{base_y} {previous[0]:.1f},{previous[1]:.1f} {current[0]:.1f},{current[1]:.1f} {current[0]:.1f},{base_y}" fill="{color(points[index].get("grade_percent"))}"/>'
        for index, (previous, current) in enumerate(zip(coords, coords[1:]), start=1)
    )
    line = " ".join(f"{x:.1f},{y:.1f}" for x, y in coords)
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Profil coloré selon la pente, de {from_km:.1f} à {to_km:.1f} kilomètres">'
        f'{areas}<polyline points="{line}" fill="none" stroke="#4a4741" stroke-width="1.4" stroke-linejoin="round"/>'
        f'<line x1="{pad_x}" y1="{base_y}" x2="{width - pad_x}" y2="{base_y}" stroke="#cfc9bf"/>'
        f'<text x="{pad_x}" y="{height - 7}" fill="#68645d" font-size="14">{from_km:.1f} km</text><text x="{width - pad_x - 53}" y="{height - 7}" fill="#68645d" font-size="14">{to_km:.1f} km</text></svg>'
    )


def _seo_course_analysis(points: list[dict]) -> dict:
    """Turn official checkpoints into readable, course-specific pacing sections."""
    clean_points = [point for point in points if isinstance(point, dict)]
    legs = []
    for previous, current in zip(clean_points, clean_points[1:]):
        try:
            distance = float(current.get("km") or 0) - float(previous.get("km") or 0)
            gain = float(current.get("elevation_gain_m") or 0) - float(previous.get("elevation_gain_m") or 0)
            loss = float(current.get("elevation_loss_m") or 0) - float(previous.get("elevation_loss_m") or 0)
        except (TypeError, ValueError):
            continue
        if distance <= 0:
            continue
        density = gain / distance
        if gain >= max(120, loss * 1.25):
            terrain, advice = "Montée dominante", "Garde une intensité durable ; la marche active peut être plus efficace qu’une relance forcée."
        elif loss >= max(120, gain * 1.25):
            terrain, advice = "Descente dominante", "Protège les quadriceps et l’attention : la vitesse se gagne surtout en restant relâché et précis."
        else:
            terrain, advice = "Terrain mixte", "Utilise les portions roulantes pour retrouver un rythme régulier sans transformer chaque relance en accélération."
        legs.append({
            "from": previous.get("name") or "Départ",
            "to": current.get("name") or "Point suivant",
            "from_km": round(float(previous.get("km") or 0), 1),
            "to_km": round(float(current.get("km") or 0), 1),
            "distance_km": round(distance, 1),
            "gain_m": round(max(gain, 0)),
            "loss_m": round(max(loss, 0)),
            "ascent_density": round(density),
            "terrain": terrain,
            "advice": advice,
        })
    longest_leg = max(legs, key=lambda leg: leg["distance_km"], default=None)
    biggest_climb = max(legs, key=lambda leg: leg["gain_m"], default=None)
    biggest_descent = max(legs, key=lambda leg: leg["loss_m"], default=None)
    return {"legs": legs, "longest_leg": longest_leg, "biggest_climb": biggest_climb, "biggest_descent": biggest_descent}


TEMPLIERS_2026_DEPARTURES = {
    "endurance-trail-des-templiers-2026": ("vendredi 16 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
    "integrale-des-causses-2026": ("vendredi 16 octobre 2026", "Peyreleau"),
    "marathon-du-larzac-2026": ("vendredi 16 octobre 2026", "Notre-Dame de la Salvage"),
    "rock-voizine-2026": ("vendredi 16 octobre 2026", "Saint-André-de-Vézines"),
    "boffi-fifty-2026": ("samedi 17 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
    "dourbie-formi-2026": ("samedi 17 octobre 2026", "site de la Graufesenque, Millau"),
    "monna-lisa-trail-2026": ("samedi 17 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
    "marathon-des-causses-2026": ("samedi 17 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
    "les-troubadours-2026": ("samedi 17 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
    "vo2-trail-2026": ("samedi 17 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
    "grand-trail-des-templiers-2026": ("dimanche 18 octobre 2026", "zone basse du Domaine de St-Estève, Millau"),
}


def _course_event_name(course_id: str) -> str:
    """Nom court de l'événement utilisé dans les habillages de parcours."""
    normalized_id = str(course_id or "").strip().lower()
    if normalized_id.startswith("saintelyon-"):
        return "SaintéLyon"
    if normalized_id.startswith("ecotrail-paris-"):
        return "EcoTrail Paris"
    if normalized_id.startswith("maxi-race-"):
        return "MaXi-Race du lac d’Annecy"
    if normalized_id.startswith("vvx-"):
        return "Volvic Volcanic Experience"
    if normalized_id in TEMPLIERS_2026_DEPARTURES:
        return "Festival des Templiers"
    if normalized_id.startswith("grp-"):
        return "Grand Raid des Pyrénées"
    if normalized_id in {"diagonale-des-fous-2026", "trail-de-bourbon-2026"}:
        return "Grand Raid de la Réunion"
    if normalized_id in {"utmb-2026", "ccc-2026", "occ-2026", "tds-2026", "mcc-2026", "etc-2026"}:
        return "UTMB Mont-Blanc"
    if normalized_id == "tpsb-68km-2026":
        return "Trail du Petit Saint-Bernard"
    if normalized_id == "marathon-de-la-meije-2026":
        return "Trail de la Meije"
    return "Running Data Plan"


def _seo_course_editorial(course: dict, analysis: dict) -> dict:
    """Build useful, course-specific editorial copy from the local route data."""
    name = str(course.get("name") or "ce trail")
    distance = float(course.get("distance_km") or 0)
    distance_text = str(course.get("distance_label") or (f"{distance:.0f} km" if distance > 0 else "une distance encore à confirmer"))
    gain_value = course.get("elevation_gain_m")
    gain = int(round(float(gain_value))) if isinstance(gain_value, (int, float)) else None
    points = list(course.get("points") or [])
    aid_points = [point for point in points if point.get("type") in {"aid_station", "aid_station_assistance"}]
    cutoff_points = [point for point in points if point.get("cutoff_label")]
    start = next((point for point in points if point.get("type") == "start"), {})
    start_window = " à ".join(filter(None, [start.get("fastest_label"), start.get("slowest_label")]))
    course_id = str(course.get("id") or "")
    festival_departure = TEMPLIERS_2026_DEPARTURES.get(course_id)
    is_templiers = festival_departure is not None
    longest_leg = analysis.get("longest_leg") or {}
    biggest_climb = analysis.get("biggest_climb") or {}
    longest_text = (
        f"Le tronçon le plus long relie {longest_leg.get('from')} à {longest_leg.get('to')} "
        f"({longest_leg.get('distance_km')} km). Prépare son contenu de sac et son effort avant le départ."
        if longest_leg else "Découpe la course par tronçon afin de prévoir l’effort, l’eau et les apports jusqu’au point suivant."
    )
    climb_text = (
        f"La principale accumulation de dénivelé du découpage se situe entre {biggest_climb.get('from')} et "
        f"{biggest_climb.get('to')} (+{biggest_climb.get('gain_m')} m). Commence cette section en réserve."
        if biggest_climb else "Utilise la pente et le ressenti pour choisir entre course et marche active."
    )
    aid_names = ", ".join(str(point.get("name")) for point in aid_points[:4])
    start_sentence = f"La fenêtre de départ indiquée dans les données du parcours est {start_window}. " if start_window else ""
    if festival_departure:
        event_date, departure_location = festival_departure
        start_sentence = (
            f"Le départ est prévu le {event_date}, depuis {departure_location}. "
            + (f"Les vagues indiquées sont {start_window}. " if start_window else "")
        )
    effort_summary = f"{distance:.1f} km et {gain} m de dénivelé positif" if gain is not None else f"un format annoncé de {distance:.0f} km"
    intro = (
        f"{name} se prépare avec une logique de trail : {effort_summary} ne se résument pas à une allure moyenne. "
        f"{start_sentence}Le plan de course sert à répartir l’effort selon la pente, à anticiper les arrêts et à garder une marge pour les portions techniques."
    )
    ravito_text = (
        f"Les points de ravitaillement référencés sont {aid_names}. Pour chacun, associe un objectif simple : boire, refaire le plein et repartir avec ce qui est nécessaire jusqu’au point suivant."
        if aid_names else "Le parcours ne référence pas de ravitaillement intermédiaire : prévois l’autonomie en boisson et en énergie dès le départ."
    )
    cutoff_text = (
        f"{len(cutoff_points)} barrière{'s' if len(cutoff_points) > 1 else ''} horaire{'s' if len(cutoff_points) > 1 else ''} apparaissent dans les données. Regarde ton avance avant chaque contrôle et compare-la à la difficulté du tronçon suivant, pas seulement au chrono global."
        if cutoff_points else "Aucune barrière horaire n’est renseignée dans ce fichier. Vérifie tout de même les dernières consignes de l’organisateur avant le départ."
    )
    festival_sections = []
    if is_templiers:
        meal_courses = {"endurance-trail-des-templiers-2026", "integrale-des-causses-2026", "grand-trail-des-templiers-2026"}
        meal_text = (
            "Un repas d’après-course est annoncé avec l’inscription ; pour l’Endurance Trail et l’Intégrale des Causses, il est prévu dans la Grange près de l’arrivée le vendredi à partir de 13 h. Pour le Grand Trail des Templiers, il est annoncé sous la tente du Salon du Trail le dimanche à partir de 13 h."
            if course_id in meal_courses else
            "Un ravitaillement d’arrivée est annoncé pour toutes les courses. Vérifie les modalités pratiques et les horaires définitifs auprès de l’organisation."
        )
        festival_sections = [
            ("Logistique du départ au Festival des Templiers", start_sentence + "Prévois une heure sur site, et jusqu’à deux heures le samedi si le retrait du dossard doit encore être effectué. Le Salon du Trail se trouve avenue de Millau Plage à Millau ; ses horaires annoncés sont jeudi après-midi et vendredi après-midi de 12 h à 19 h, puis samedi de 9 h à 19 h."),
            ("Assistance personnelle et postes de ravitaillement", "L’assistance est autorisée uniquement sur les zones officielles de ravitaillement, jamais aux points d’eau. Le passage dans le poste est obligatoire. L’accès au poste est interdit aux suiveurs ; l’assistance personnelle est prévue dans une zone de 50 mètres avant le poste, sauf au Truel où l’accès est interdit. La liste définitive des zones est communiquée avec le dossard."),
            ("Arrivée, récupération et résultats", "Les arrivées sont prévues sur la zone haute du Domaine de St-Estève, avenue de Millau Plage à Millau. " + meal_text + " Après l’événement, l’organisation annonce le dépôt des résultats pour l’UTMB Index et l’ITRA."),
        ]
    route_pending = not bool(course.get("route_available", True))
    event_name = str(course.get("event_name") or "cet événement")
    event_date_label = str(course.get("event_date_label") or "")
    passage_names = [str(point.get("name")) for point in points if point.get("name")]
    passage_preview = ", ".join(passage_names[:6]) + ("…" if len(passage_names) > 6 else "")
    route_sections = [(
        f"Parcours, carte GPX et points de passage de {name}",
        f"La carte interactive permet de suivre le parcours GPX et son relief sur {distance:.1f} km. "
        + (f"Les principaux points de passage référencés sont {passage_preview}. " if passage_preview else "Les points de passage sont positionnés le long de la trace. ")
        + "Utilise la carte, le profil altimétrique et le tableau kilométrique ensemble pour repérer les montées, descentes, ravitaillements et contrôles.",
    )]
    pending_sections = []
    if route_pending:
        intro = (
            f"{name} est un format de {event_name} annoncé sur {distance_text}"
            + (f", programmé {event_date_label}" if event_date_label else "")
            + ". La trace GPX officielle n’est pas encore disponible dans Running Data Plan : "
            "les dénivelés, ravitaillements, barrières et temps de passage ne sont donc pas inventés. Cette page sera enrichie dès la publication des données officielles."
        )
        pending_sections = [
            ("Anticiper avant la sortie du GPX officiel", "Abonne-toi dès maintenant et synchronise régulièrement tes sorties. Un historique riche en montées, descentes, terrain roulant, durée et fatigue permettra de construire un pacing vraiment pertinent lorsque la trace officielle sera publiée."),
            ("Préparer le pacing sans fausse précision", "Travaille déjà une intensité durable, la marche active, les relances et la gestion de la nuit. Le découpage kilométrique précis viendra ensuite du profil GPX officiel et de tes allures observées sur des pentes comparables."),
            ("Tester nutrition et hydratation", "Teste à l’entraînement une routine régulière de boisson et d’apports tolérés. Les quantités entre deux ravitaillements seront calculées lorsque leurs emplacements officiels seront connus ; vérifie toujours les consignes de l’organisation."),
        ]
    return {
        "intro": intro,
        "sections": pending_sections or route_sections + [
            ("Construire un plan d’allure pour " + name, "Pars avec une première estimation prudente, puis règle les allures par pente : effort contrôlé en montée, relance durable sur le roulant et descente précise plutôt que précipitée. " + climb_text),
            ("Planifier les ravitaillements et l’autonomie", ravito_text + " " + longest_text),
            ("Organiser l’heure de départ et les temps de passage", start_sentence + cutoff_text + " Les horaires, parcours et produits proposés peuvent évoluer : les documents de l’organisateur restent la référence."),
            ("Préparer l’entraînement spécifique", f"Travaille les montées, les descentes et les sorties longues adaptées à {distance:.1f} km. Teste l’alimentation et le matériel sur un terrain proche du profil, plutôt que de les découvrir le jour de la course."),
        ] + festival_sections,
        "faq": [
            (f"Comment préparer un plan de course pour {name} ?", "Enrichis d’abord ton historique avec tes sorties et tes allures selon la pente. Dès que le GPX officiel sera disponible, il permettra de croiser ton profil avec le relief, les tronçons et les ravitaillements." if route_pending else "Utilise le profil, les tronçons et tes propres allures par pente. Ajoute un temps d’arrêt réaliste aux ravitaillements et garde une marge sur les portions longues ou techniques."),
            ("Quelle allure viser en trail ?", "Ne cherche pas une allure unique au kilomètre. En montée, appuie-toi sur l’effort et la marche active ; sur le roulant, choisis une intensité durable ; en descente, privilégie la régularité et la sécurité."),
            ("Comment gérer les ravitaillements ?", "Prépare pour chaque point ce que tu bois, manges et emportes pour le tronçon suivant. Les arrêts font partie du temps de course : mieux vaut les anticiper que les subir."),
            ("Les horaires et barrières affichés sont-ils définitifs ?", "Non. Ils servent à organiser une préparation, mais l’organisateur doit toujours être consulté pour les informations officielles et les éventuelles mises à jour."),
            (f"Où voir la carte et les points de passage de {name} ?", "La carte du parcours, le profil GPX et le tableau des points de passage sont réunis sur cette page. Ils permettent de localiser les ravitaillements, contrôles et principales étapes kilométriques." if not route_pending else "La carte et les points de passage seront ajoutés sur cette page dès que la trace GPX officielle sera disponible."),
        ],
    }


@lru_cache(maxsize=32)
def _seo_course_payload(course_id: str) -> dict | None:
    loaded = _load_official_course(course_id, require_route=False)
    if not loaded:
        return None
    course = loaded["course"]
    route_path = loaded.get("route_path")
    try:
        if not route_path:
            raise OSError("GPX not available yet")
        _bands, gpx_distance_m, _segments, profile = _compute_slope_distribution_from_gpx(
            open(route_path, "rb").read(),
            max_profile_points=2500,
        )
    except (OSError, ValueError, TypeError):
        gpx_distance_m, profile = 0.0, []
    points = [
        {
            **point,
            "altitude_m": point.get("altitude_m"),
            "elevation_gain_m": point.get("elevation_gain_m"),
            "elevation_loss_m": point.get("elevation_loss_m"),
            "cutoff_label": point.get("cutoff_label"),
        }
        for point in sorted(course.get("points") or [], key=lambda point: float(point.get("km") or 0))
    ]
    route_stops = [point for point in points if point.get("type") == "start"]
    route_stops += [point for point in points if point.get("type") in {"aid_station", "aid_station_assistance"}]
    route_stops += [point for point in points if point.get("type") == "finish"]
    analysis = _seo_course_analysis(route_stops)
    for leg in analysis["legs"]:
        leg["profile_svg"] = _seo_segment_profile_svg(profile, float(leg["from_km"]), float(leg["to_km"]))
    map_profile = []
    cumulative_gain_m = 0.0
    previous_elevation_m = None
    for point in profile:
        if not isinstance(point, dict) or not isinstance(point.get("longitude"), (int, float)) or not isinstance(point.get("latitude"), (int, float)):
            continue
        elevation_m = float(point.get("elevation_m") or 0.0)
        if previous_elevation_m is not None:
            cumulative_gain_m += max(0.0, elevation_m - previous_elevation_m)
        previous_elevation_m = elevation_m
        map_profile.append({
            "longitude": round(float(point["longitude"]), 6),
            "latitude": round(float(point["latitude"]), 6),
            "elevation_m": round(elevation_m),
            "distance_km": round(float(point["distance_km"]), 3),
            "grade_percent": round(float(point.get("grade_percent") or 0), 1),
            "elevation_gain_cumulative_m": round(cumulative_gain_m),
        })
    def compact_profile(points: list[dict], max_points: int) -> list[dict]:
        if len(points) <= max_points:
            return points
        step = max(1, math.ceil(len(points) / max_points))
        compacted = points[::step]
        if compacted[-1]["distance_km"] != points[-1]["distance_km"]:
            compacted.append(points[-1])
        return compacted

    map_profile_3d = compact_profile(map_profile, 2500)
    map_profile = compact_profile(map_profile, 180)
    route_status = str(loaded.get("route_status") or ("official" if route_path else "pending"))
    course = {
        **course,
        "route_available": bool(route_path),
        "route_status": route_status,
        "route_edition_year": course.get("route_edition_year") or (course.get("edition_year") if route_status == "official" else None),
    }
    editorial = _seo_course_editorial(course, analysis)
    return {
        **course,
        "video_event_name": _course_event_name(str(course.get("id") or course_id)),
        "slug": _seo_course_slug(course_id),
        "gpx_distance_km": round(float(gpx_distance_m or 0) / 1000, 1) if gpx_distance_m else None,
        "profile_svg": _seo_elevation_svg(profile),
        "points": points,
        "aid_points": [point for point in points if point.get("type") in {"aid_station", "aid_station_assistance"}],
        "cutoff_points": [point for point in points if point.get("cutoff_label")],
        "analysis": analysis,
        "editorial": editorial,
        "map_profile": map_profile,
        "map_profile_3d": map_profile_3d,
    }


def _slope_band_center(min_v: float, max_v: float) -> float:
    if max_v > 500:   # bornes ouvertes sur +inf
        return min_v + 10.0
    if min_v < -500:  # bornes ouvertes sur -inf
        return max_v - 10.0
    return (min_v + max_v) / 2.0


SLOPE_BAND_CENTER = {
    band_id: _slope_band_center(min_v, max_v)
    for min_v, max_v, band_id, _label in SLOPE_BANDS_DEF
}


def _get_dexcom_share_record(tokens: list[DexcomToken] | None) -> Optional[DexcomToken]:
    if not tokens:
        return None
    for token in sorted(tokens, key=lambda item: item.id or 0, reverse=True):
        if has_dexcom_share_credentials(token):
            return token
    return None


def _activity_enrichment_key(user_id: int, activity_id: int) -> tuple[int, int]:
    return int(user_id), int(activity_id)


def _acquire_activity_enrichment_lock(user_id: int, activity_id: int) -> bool:
    key = _activity_enrichment_key(user_id, activity_id)
    with ENRICHMENT_ACTIVITY_LOCK:
        if key in ENRICHMENT_ACTIVE_KEYS:
            return False
        ENRICHMENT_ACTIVE_KEYS.add(key)
        return True


def _release_activity_enrichment_lock(user_id: int, activity_id: int) -> None:
    key = _activity_enrichment_key(user_id, activity_id)
    with ENRICHMENT_ACTIVITY_LOCK:
        ENRICHMENT_ACTIVE_KEYS.discard(key)


def _is_retryable_enrichment_reason(reason: str | None) -> bool:
    return bool(reason and reason in ENRICHMENT_RETRYABLE_REASONS)


def _compute_enrichment_retry_delay_seconds(attempts: int) -> int:
    if attempts <= 1:
        return ENRICHMENT_RETRY_BASE_SECONDS
    delay = ENRICHMENT_RETRY_BASE_SECONDS * (2 ** (attempts - 1))
    return min(delay, ENRICHMENT_RETRY_MAX_SECONDS)


def _get_or_create_enrichment_job(db: Session, user_id: int, activity_id: int) -> ActivityEnrichmentJob:
    job = (
        db.query(ActivityEnrichmentJob)
        .filter(
            ActivityEnrichmentJob.user_id == int(user_id),
            ActivityEnrichmentJob.strava_activity_id == int(activity_id),
        )
        .one_or_none()
    )
    if job is None:
        job = ActivityEnrichmentJob(
            user_id=int(user_id),
            strava_activity_id=int(activity_id),
            status="pending",
        )
        db.add(job)
        db.flush()
    return job


def _schedule_enrichment_retry(
    db: Session,
    *,
    job: ActivityEnrichmentJob,
    reason: str,
    last_error: str | None = None,
    trigger_source: str | None = None,
) -> None:
    attempts = max(int(job.attempts or 0), 1)
    if attempts >= ENRICHMENT_MAX_ATTEMPTS:
        job.status = "failed"
        job.last_reason = reason
        job.last_error = (last_error or reason or "")[:1000] or None
        job.next_retry_at = None
        job.completed_at = dt.datetime.utcnow()
        if trigger_source:
            job.trigger_source = trigger_source[:32]
        return

    delay_seconds = _compute_enrichment_retry_delay_seconds(attempts)
    retry_at = dt.datetime.utcnow() + dt.timedelta(seconds=delay_seconds)
    job.status = "retry"
    job.last_reason = reason
    job.last_error = (last_error or reason or "")[:1000] or None
    job.next_retry_at = retry_at
    job.locked_at = None
    job.completed_at = None
    if trigger_source:
        job.trigger_source = trigger_source[:32]
    logger.info(
        "[ENRICHMENT] user_id=%s activity_id=%s retry scheduled in %ss (reason=%s attempts=%s/%s)",
        job.user_id,
        job.strava_activity_id,
        delay_seconds,
        reason,
        attempts,
        ENRICHMENT_MAX_ATTEMPTS,
    )


def _mark_enrichment_job_success(
    job: ActivityEnrichmentJob,
    *,
    reason: str | None = None,
    trigger_source: str | None = None,
) -> None:
    job.status = "succeeded"
    job.last_reason = reason
    job.last_error = None
    job.next_retry_at = None
    job.locked_at = None
    job.completed_at = dt.datetime.utcnow()
    if trigger_source:
        job.trigger_source = trigger_source[:32]


def _mark_enrichment_job_failed(
    job: ActivityEnrichmentJob,
    *,
    reason: str,
    last_error: str | None = None,
    trigger_source: str | None = None,
) -> None:
    job.status = "failed"
    job.last_reason = reason
    job.last_error = (last_error or reason or "")[:1000] or None
    job.next_retry_at = None
    job.locked_at = None
    job.completed_at = dt.datetime.utcnow()
    if trigger_source:
        job.trigger_source = trigger_source[:32]


def _attach_enrichment_job_snapshot(result: dict, job: ActivityEnrichmentJob | None) -> dict:
    payload = dict(result or {})
    if job is None:
        return payload
    payload["job_id"] = int(job.id)
    payload["job_status"] = job.status
    payload["job_attempts"] = int(job.attempts or 0)
    payload["job_last_reason"] = job.last_reason
    payload["job_next_retry_at"] = job.next_retry_at
    return payload


def _build_pace_lookup_from_profile(profile_data: dict | None, hr_zone_names: list[str] | None) -> dict:
    """
    Construit un lookup slope→zone→allure (s/km) et comble les trous en appliquant
    un facteur de -7% par zone manquante ou par pente adjacente quand aucune zone n’est renseignée.
    """

    pace_lookup: dict[str, dict[str, float]] = {}
    if not profile_data:
        return pace_lookup

    zones_data = profile_data.get("zones") or {}
    if not zones_data:
        return pace_lookup

    for zone_name, slopes in zones_data.items():
        if not slopes:
            continue
        for slope_id, cell in slopes.items():
            if not cell:
                continue
            pace_val = cell.get("avg_pace_s_per_km")
            if pace_val is None or pace_val <= 0:
                continue
            slope_entry = pace_lookup.setdefault(slope_id, {})
            slope_entry[zone_name] = float(pace_val)

    _fill_missing_zone_paces(pace_lookup, hr_zone_names or [])
    return pace_lookup


def _percentile_from_sorted(values: list[float], quantile: float) -> float | None:
    """Percentile interpolé, sans dépendance externe."""
    if not values:
        return None
    index = max(0.0, min(1.0, quantile)) * (len(values) - 1)
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    weight = index - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def _build_anonymized_pace_benchmarks(
    db: Session,
    *,
    sport: str = "run",
    excluded_user_id: int | None = None,
    minimum_runners: int = 8,
    minimum_cell_duration_sec: int = 300,
) -> dict:
    """Construit des repères anonymisés P20/P50/P80 par zone et pente.

    Chaque coureur ne contribue qu'une fois à une cellule, avec son allure
    moyenne pondérée par durée. Cela évite de surpondérer les utilisateurs qui
    importent beaucoup plus de sorties que les autres.
    """
    q = (
        db.query(
            models.RunnerProfileMonthly.user_id.label("user_id"),
            models.RunnerProfileMonthly.hr_zone.label("hr_zone"),
            models.RunnerProfileMonthly.slope_band.label("slope_band"),
            func.sum(models.RunnerProfileMonthly.sum_pace_x_duration).label("pace_sum"),
            func.sum(models.RunnerProfileMonthly.pace_duration_sec).label("pace_duration"),
        )
        .filter(
            sport_column_condition(models.RunnerProfileMonthly.sport, canonicalize_sport_label(sport)),
            models.RunnerProfileMonthly.metric_scope == "slope_zone",
            models.RunnerProfileMonthly.hr_zone.isnot(None),
            models.RunnerProfileMonthly.slope_band.isnot(None),
        )
        .group_by(
            models.RunnerProfileMonthly.user_id,
            models.RunnerProfileMonthly.hr_zone,
            models.RunnerProfileMonthly.slope_band,
        )
    )
    if excluded_user_id is not None:
        q = q.filter(models.RunnerProfileMonthly.user_id != excluded_user_id)

    values_by_cell: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in q.all():
        duration = float(row.pace_duration or 0.0)
        pace = float(row.pace_sum or 0.0) / duration if duration > 0 else 0.0
        if duration < minimum_cell_duration_sec or not (120.0 <= pace <= 7200.0):
            continue
        values_by_cell[(str(row.hr_zone), str(row.slope_band))].append(pace)

    zones: dict[str, dict[str, dict]] = {}
    for (zone, slope_band), values in values_by_cell.items():
        if len(values) < minimum_runners:
            continue
        values.sort()
        zones.setdefault(zone, {})[slope_band] = {
            "p10": _percentile_from_sorted(values, 0.10),
            "p20": _percentile_from_sorted(values, 0.20),
            "p50": _percentile_from_sorted(values, 0.50),
            "p80": _percentile_from_sorted(values, 0.80),
            "p90": _percentile_from_sorted(values, 0.90),
            "count": len(values),
        }

    return {"zones": zones, "minimum_runners": minimum_runners}


def _fill_missing_zone_paces(pace_lookup: dict[str, dict[str, float]], hr_zone_names: list[str]):
    """Applique les règles de fallback (-7% par zone ou par pente adjacente)."""
    if not pace_lookup or not hr_zone_names:
        return

    slope_ids = [band_id for band_id, _label in SLOPE_ORDER]
    zone_factor = 0.93  # -7 %

    for slope_id in slope_ids:
        zone_map = pace_lookup.setdefault(slope_id, {})

        prev_idx = None
        prev_val = None
        for idx, zone in enumerate(hr_zone_names):
            val = zone_map.get(zone)
            if val and val > 0:
                prev_idx = idx
                prev_val = val
                continue
            if prev_val:
                steps = idx - prev_idx
                zone_map[zone] = prev_val * (zone_factor ** steps)

        next_idx = None
        next_val = None
        for idx in range(len(hr_zone_names) - 1, -1, -1):
            zone = hr_zone_names[idx]
            val = zone_map.get(zone)
            if val and val > 0:
                next_idx = idx
                next_val = val
                continue
            if next_val:
                steps = next_idx - idx
                zone_map[zone] = next_val / (zone_factor ** steps)

    def _neighbor_value(current_idx: int, zone_name: str) -> float | None:
        current_slope_id = slope_ids[current_idx]
        current_intensity = abs(SLOPE_BAND_CENTER.get(current_slope_id, 0.0))
        for offset in range(1, len(slope_ids)):
            candidates = []
            left_idx = current_idx - offset
            if left_idx >= 0:
                candidates.append(left_idx)
            right_idx = current_idx + offset
            if right_idx < len(slope_ids):
                candidates.append(right_idx)

            for neighbor_idx in candidates:
                neighbor_id = slope_ids[neighbor_idx]
                neighbor = pace_lookup.get(neighbor_id, {})
                val = neighbor.get(zone_name)
                if not (val and val > 0):
                    continue

                neighbor_intensity = abs(SLOPE_BAND_CENTER.get(neighbor_id, 0.0))
                if current_intensity and neighbor_intensity:
                    if current_intensity >= neighbor_intensity:
                        return val / (zone_factor ** offset)
                    else:
                        return val * (zone_factor ** offset)
                return val
        return None

    for idx, slope_id in enumerate(slope_ids):
        zone_map = pace_lookup.setdefault(slope_id, {})
        for zone in hr_zone_names:
            val = zone_map.get(zone)
            if val and val > 0:
                continue
            neighbor_val = _neighbor_value(idx, zone)
            if neighbor_val and neighbor_val > 0:
                zone_map[zone] = neighbor_val


def _classify_activity_profile(zone_durations: dict[str, float]) -> str | None:
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

from app import auth
from app import models
from app.auth import pwd_context
from app.cgm_service import (
    run_polling_loop,
    fetch_realtime_points_for_user,
    fetch_libre_points_guarded,
    record_glucose_page_view,
    should_attempt_page_refresh,
    test_libre_credentials_guarded,
)
from app.indicators.slope_cadence import build_slope_cadence_data
from app.routers import auth_strava, auth_dexcom, webhooks

from statistics import mean
import math
import xml.etree.ElementTree as ET
import os

# Helper pour s'assurer que les datetime sont bien tz-aware
def _safe_dt(ts):
    return ts if (ts is None or ts.tzinfo is not None) else ts.replace(tzinfo=dt.timezone.utc)


def _format_pace(pace_seconds: float | None) -> str | None:
    if pace_seconds is None or pace_seconds <= 0:
        return None
    s = int(round(pace_seconds))
    minutes = s // 60
    seconds = s % 60
    return f"{minutes}:{seconds:02d} /km"


def _format_story_duration_short(sec: float | None) -> str:
    if sec is None or sec <= 0:
        return "–"
    s = int(round(sec))
    h = s // 3600
    m = (s % 3600) // 60
    if h > 0:
        return f"{h}h{m:02d}"
    if m > 0:
        return f"{m} min"
    return f"{s}s"


def _format_story_distance_short(meters: float | None) -> str:
    if meters is None or meters <= 0:
        return "–"
    km = float(meters) / 1000.0
    return f"{km:.2f}".rstrip("0").rstrip(".") + " km"


def _format_story_pace_short(meters: float | None, seconds: float | None) -> str:
    if meters is None or seconds is None or meters <= 0 or seconds <= 0:
        return "–"
    sec_per_km = float(seconds) / (float(meters) / 1000.0)
    mins = int(sec_per_km // 60)
    secs = int(round(sec_per_km % 60))
    if secs == 60:
        mins += 1
        secs = 0
    return f"{mins}:{secs:02d}/km"


def _format_story_speed_short(meters: float | None, seconds: float | None) -> str:
    if meters is None or seconds is None or meters <= 0 or seconds <= 0:
        return "–"
    kmh = (float(meters) / float(seconds)) * 3.6
    return f"{kmh:.1f} km/h"


def _build_story_export_data(
    activity: Activity,
    glucose_chart_points: list[dict],
    *,
    route_points: list[list[float]] | None = None,
    altitude_profile_points: list[list[float]] | None = None,
    club_data: dict | None = None,
    share_show_club_logo: bool = False,
) -> dict | None:
    # Le partage doit rester disponible sans CGM. Les données glycémiques sont
    # simplement optionnelles : les visuels de parcours et de performance ont
    # alors leur propre lecture, sans valeur factice.
    has_glucose = len(glucose_chart_points) >= 2

    sport_norm = (activity.sport or activity.activity_type or "").lower()
    effort_metric_label = None
    effort_metric_value = None
    sport_profile = "other"

    if sport_norm == "run":
        effort_metric_label = "ALLURE"
        effort_metric_value = _format_story_pace_short(activity.distance, activity.elapsed_time)
        sport_profile = "run"
    elif sport_norm == "ride":
        effort_metric_label = "VITESSE"
        effort_metric_value = _format_story_speed_short(activity.distance, activity.elapsed_time)
        sport_profile = "ride"
    elif sport_norm in {"workout", "crossfit", "weighttraining", "weight_training", "weights", "gym"}:
        effort_metric_label = "FC MOY"
        effort_metric_value = (
            f"{round(float(activity.average_heartrate))} bpm"
            if activity.average_heartrate is not None
            else "–"
        )
        sport_profile = "gym"
    elif sport_norm in {"trailrun", "hike", "walk"}:
        effort_metric_label = "RYTHME"
        effort_metric_value = _format_story_pace_short(activity.distance, activity.elapsed_time)
        sport_profile = "run"

    return {
        "activity_id": activity.id,
        "activity_name": activity.name or "Activite",
        "activity_date": _safe_dt(activity.start_date).strftime("%d/%m/%Y") if _safe_dt(activity.start_date) else None,
        "sport_label": activity.sport or activity.activity_type or "Activite",
        "sport_profile": sport_profile,
        "distance_km": float(activity.distance or 0.0) / 1000.0 if activity.distance else None,
        "duration_sec": float(activity.elapsed_time or 0.0) if activity.elapsed_time else None,
        "dplus_m": float(activity.total_elevation_gain or 0.0) if activity.total_elevation_gain is not None else None,
        "distance_label": _format_story_distance_short(activity.distance),
        "duration_label": _format_story_duration_short(activity.elapsed_time),
        "dplus_label": (
            f"{round(float(activity.total_elevation_gain))} m"
            if activity.total_elevation_gain is not None
            else "–"
        ),
        "vam_5m_label": (
            f"{round(float(activity.max_vam_5m))} m/h"
            if activity.max_vam_5m is not None
            else "–"
        ),
        "effort_metric_label": effort_metric_label,
        "effort_metric_value": effort_metric_value,
        "tir_label": (
            f"{round(float(activity.time_in_range_percent))}%"
            if activity.time_in_range_percent is not None
            else "–"
        ),
        "gly_avg_label": (
            f"{round(float(activity.avg_glucose))} mg/dL"
            if activity.avg_glucose is not None
            else "–"
        ),
        "fc_avg_label": (
            f"{round(float(activity.average_heartrate))} bpm"
            if activity.average_heartrate is not None
            else "–"
        ),
        "hypo_label": str(int(activity.hypo_count or 0)),
        "hyper_label": str(int(activity.hyper_count or 0)),
        "min_label": (
            f"{round(float(activity.min_glucose))} mg/dL"
            if activity.min_glucose is not None
            else "–"
        ),
        "max_label": (
            f"{round(float(activity.max_glucose))} mg/dL"
            if activity.max_glucose is not None
            else "–"
        ),
        "glucose_points": glucose_chart_points,
        "has_glucose": has_glucose,
        "route_points": route_points or [],
        "altitude_profile_points": altitude_profile_points or [],
        "club_name": club_data.get("name") if club_data else None,
        "club_logo_url": (
            club_data.get("logo_url")
            if club_data and share_show_club_logo
            else None
        ),
        "share_show_club_logo": bool(share_show_club_logo and club_data and club_data.get("logo_url")),
    }


def _align_stream_pairs(time_stream, value_stream):
    pairs = []
    n = min(len(time_stream or []), len(value_stream or []))
    for i in range(n):
        t = time_stream[i]
        v = value_stream[i]
        if t is None or v is None:
            continue
        try:
            pairs.append((float(t), float(v)))
        except (TypeError, ValueError):
            continue
    return pairs


def _max_speed_cap_for_sport(sport_label: str | None) -> float | None:
    sport = (sport_label or "").lower()
    if sport == "run":
        return 6.0  # ~3:20 /km
    if sport == "ride":
        return 25.0  # ~90 km/h
    if sport in {"ski_alpine", "ski_nordic", "ski_rando"}:
        return 15.0  # ~54 km/h
    return None


def _compute_best_pace_windows(time_stream, distance_stream, windows_sec: list[int], max_speed_mps: float | None = None) -> dict[int, dict]:
    pairs = _align_stream_pairs(time_stream, distance_stream)
    if len(pairs) < 2:
        return {}

    times = [p[0] for p in pairs]
    dist = [p[1] for p in pairs]
    n = len(times)

    if max_speed_mps is not None and n >= 2:
        smoothed_dist = [dist[0]]
        for i in range(1, n):
            dt_s = times[i] - times[i - 1]
            if dt_s <= 0:
                smoothed_dist.append(smoothed_dist[-1])
                continue
            delta = dist[i] - dist[i - 1]
            if delta < 0:
                delta = 0.0
            max_delta = max_speed_mps * dt_s
            if delta > max_delta:
                delta = max_delta
            smoothed_dist.append(smoothed_dist[-1] + delta)
        dist = smoothed_dist

    results: dict[int, dict] = {}

    for window in windows_sec:
        best_entry = None
        best_pace = None
        for i in range(n - 1):
            t0 = times[i]
            target = t0 + window
            j = bisect_left(times, target, i + 1, n)
            if j >= n:
                continue
            for idx in range(j, min(j + 3, n)):
                duration = times[idx] - t0
                if duration <= 0:
                    continue
                distance_gain = dist[idx] - dist[i]
                if distance_gain <= 0:
                    continue
                pace_sec_per_km = (duration / distance_gain) * 1000.0
                if pace_sec_per_km <= 0:
                    continue
                if best_pace is None or pace_sec_per_km < best_pace:
                    best_pace = pace_sec_per_km
                    best_entry = {
                        "duration": duration,
                        "distance": distance_gain,
                        "pace_sec_per_km": pace_sec_per_km,
                    }
        if best_entry:
            results[window] = best_entry
    return results


def _compute_best_gain_windows(time_stream, altitude_stream, windows_sec: list[int]) -> dict[int, dict]:
    pairs = _align_stream_pairs(time_stream, altitude_stream)
    if len(pairs) < 2:
        return {}

    times = [p[0] for p in pairs]
    alts = [p[1] for p in pairs]
    n = len(times)
    results: dict[int, dict] = {}

    for window in windows_sec:
        best_entry = None
        best_gain = 0.0
        for i in range(n - 1):
            t0 = times[i]
            target = t0 + window
            j = bisect_left(times, target, i + 1, n)
            if j >= n:
                continue
            for idx in range(j, min(j + 3, n)):
                duration = times[idx] - t0
                if duration <= 0:
                    continue
                gain = alts[idx] - alts[i]
                if gain <= 0:
                    continue
                if gain > best_gain:
                    vam = (gain / duration) * 3600.0
                    best_gain = gain
                    best_entry = {
                        "gain_m": gain,
                        "duration": duration,
                        "vam_m_per_h": vam,
                    }
        if best_entry:
            results[window] = best_entry
    return results


def _compute_best_drop_windows(time_stream, altitude_stream, windows_sec: list[int]) -> dict[int, dict]:
    pairs = _align_stream_pairs(time_stream, altitude_stream)
    if len(pairs) < 2:
        return {}

    times = [p[0] for p in pairs]
    alts = [p[1] for p in pairs]
    n = len(times)
    results: dict[int, dict] = {}

    for window in windows_sec:
        best_entry = None
        best_drop = 0.0
        for i in range(n - 1):
            t0 = times[i]
            target = t0 + window
            j = bisect_left(times, target, i + 1, n)
            if j >= n:
                continue
            for idx in range(j, min(j + 3, n)):
                duration = times[idx] - t0
                if duration <= 0:
                    continue
                drop = alts[i] - alts[idx]
                if drop <= 0:
                    continue
                if drop > best_drop:
                    vam = (drop / duration) * 3600.0
                    best_drop = drop
                    best_entry = {
                        "drop_m": drop,
                        "duration": duration,
                        "vam_m_per_h": vam,
                    }
        if best_entry:
            results[window] = best_entry
    return results


def _compute_cadence_buckets(time_stream, cadence_stream) -> dict[str, float]:
    pairs = _align_stream_pairs(time_stream, cadence_stream)
    if len(pairs) < 2:
        return {}

    buckets = {"walk": 0.0, "trot": 0.0, "run": 0.0}
    for i in range(len(pairs) - 1):
        t0, cad = pairs[i]
        t1 = pairs[i + 1][0]
        duration = t1 - t0
        if duration <= 0:
            continue
        cadence_spm = float(cad) * 2.0
        if cadence_spm < 120:
            bucket = "walk"
        elif cadence_spm <= 150:
            bucket = "trot"
        else:
            bucket = "run"
        buckets[bucket] += duration
    return buckets


def _resample_series(time_stream, value_stream, step_sec: float = 1.0) -> list[float]:
    pairs = _align_stream_pairs(time_stream, value_stream)
    if len(pairs) < 2 or step_sec <= 0:
        return []
    step = max(0.5, float(step_sec))
    start = pairs[0][0]
    end = pairs[-1][0]
    if end <= start:
        return []
    num_steps = int((end - start) / step) + 1
    values: list[float] = []
    idx = 0
    current_val = pairs[0][1]
    t = start
    for _ in range(num_steps):
        while idx + 1 < len(pairs) and pairs[idx + 1][0] <= t:
            idx += 1
            current_val = pairs[idx][1]
        values.append(current_val)
        t += step
    return values


def _compute_avg_value_windows(time_stream, value_stream, windows_sec: list[int]) -> dict[int, float]:
    samples = _resample_series(time_stream, value_stream, step_sec=1.0)
    if not samples:
        return {}
    cum = [0.0]
    for val in samples:
        cum.append(cum[-1] + float(val))
    n = len(samples)
    results: dict[int, float] = {}
    for window in windows_sec:
        size = int(round(window))
        if size <= 0 or size > n:
            continue
        best_avg = None
        for i in range(0, n - size + 1):
            total = cum[i + size] - cum[i]
            avg = total / size
            if best_avg is None or avg > best_avg:
                best_avg = avg
        if best_avg is not None:
            results[window] = best_avg
    return results


def _compute_time_weighted_avg_and_max(time_stream, value_stream) -> tuple[float | None, float | None]:
    pairs = _align_stream_pairs(time_stream, value_stream)
    if len(pairs) < 2:
        return None, None
    total = 0.0
    duration = 0.0
    max_val = None
    for i in range(len(pairs) - 1):
        t0, v = pairs[i]
        t1 = pairs[i + 1][0]
        dt = t1 - t0
        if dt <= 0:
            continue
        total += float(v) * dt
        duration += dt
        if max_val is None or v > max_val:
            max_val = float(v)
    avg = (total / duration) if duration > 0 else None
    return avg, max_val


def _build_time_distance_alt_points(time_stream, distance_stream, altitude_stream) -> list[tuple[float, float, float]]:
    n = min(len(time_stream or []), len(distance_stream or []), len(altitude_stream or []))
    if n < 2:
        return []

    points: list[tuple[float, float, float]] = []
    last_time = None
    last_dist = None
    for i in range(n):
        try:
            t = float(time_stream[i])
            d = float(distance_stream[i])
            a = float(altitude_stream[i])
        except (TypeError, ValueError):
            continue
        if last_time is not None and t <= last_time:
            continue
        if last_dist is not None and d < last_dist:
            d = last_dist
        points.append((t, d, a))
        last_time = t
        last_dist = d
    return points


def _smooth_altitudes_by_distance(
    points: list[tuple[float, float, float]],
    *,
    radius_m: float = 25.0,
) -> list[float]:
    if not points:
        return []
    if radius_m <= 0:
        return [float(p[2]) for p in points]

    distances = [float(p[1]) for p in points]
    altitudes = [float(p[2]) for p in points]
    prefix_alt = [0.0]
    for altitude in altitudes:
        prefix_alt.append(prefix_alt[-1] + altitude)

    smoothed: list[float] = []
    left = 0
    right = -1
    n = len(points)
    for i in range(n):
        center_distance = distances[i]
        min_distance = center_distance - radius_m
        max_distance = center_distance + radius_m
        while left < n and distances[left] < min_distance:
            left += 1
        while right + 1 < n and distances[right + 1] <= max_distance:
            right += 1
        if right < left:
            smoothed.append(altitudes[i])
            continue
        total_alt = prefix_alt[right + 1] - prefix_alt[left]
        count = right - left + 1
        smoothed.append(total_alt / count if count > 0 else altitudes[i])
    return smoothed


def _compute_local_min_indices(distances: list[float], values: list[float], window_m: float) -> list[int]:
    lows: list[int] = []
    window: deque[int] = deque()
    left = 0
    for i, distance in enumerate(distances):
        min_distance = distance - window_m
        while left < i and distances[left] < min_distance:
            if window and window[0] == left:
                window.popleft()
            left += 1
        while window and values[window[-1]] >= values[i]:
            window.pop()
        window.append(i)
        if window and window[0] == i:
            lows.append(i)
    return lows


def _compute_local_max_indices(distances: list[float], values: list[float], window_m: float) -> list[int]:
    highs: list[int] = []
    window: deque[int] = deque()
    right = len(distances) - 1
    for i in range(len(distances) - 1, -1, -1):
        max_distance = distances[i] + window_m
        while right > i and distances[right] > max_distance:
            if window and window[0] == right:
                window.popleft()
            right -= 1
        while window and values[window[-1]] <= values[i]:
            window.pop()
        window.append(i)
        if window and window[0] == i:
            highs.append(i)
    highs.reverse()
    return highs


def _build_extrema_based_climb_candidates(
    points: list[tuple[float, float, float]],
    *,
    smoothing_radius_m: float = 25.0,
    extrema_window_m: float = 400.0,
    min_distance_m: float = 200.0,
    min_vertical_m: float = 10.0,
    min_grade_pct: float = 5.0,
) -> list[dict]:
    """
    Détecte les segments de montée en exigeant que la montée démarre 
    au moment où on détecte un segment de 200m+ avec pente >= 5%,
    et se termine au prochain maximum local.
    """
    if len(points) < 2:
        return []

    distances = [float(p[1]) for p in points]
    times = [float(p[0]) for p in points]
    altitudes = [float(p[2]) for p in points]
    smoothed_altitudes = _smooth_altitudes_by_distance(points, radius_m=smoothing_radius_m)

    # Détecte les maxima locaux d'altitude lissée
    high_indices = _compute_local_max_indices(distances, smoothed_altitudes, extrema_window_m)
    if not high_indices:
        return []

    # Détecte les segments de 200m+ avec pente >= min_grade_pct (5% par défaut)
    steep_segment_starts = []
    for i in range(len(points) - 1):
        # Regarde forward jusqu'à trouver un segment de 200m
        for j in range(i + 1, len(points)):
            segment_distance = distances[j] - distances[i]
            if segment_distance >= min_distance_m:
                segment_altitude_delta = smoothed_altitudes[j] - smoothed_altitudes[i]
                segment_grade = (segment_altitude_delta / segment_distance) * 100.0 if segment_distance > 0 else 0.0
                if segment_grade >= min_grade_pct:
                    # Marque le début de ce segment comme potentiel démarrage de montée
                    steep_segment_starts.append(i)
                break

    if not steep_segment_starts:
        return []

    candidates: list[dict] = []
    high_lookup = set(high_indices)

    # Pour chaque segment abrupt détecté, cherche le prochain maximum
    for start_idx in steep_segment_starts:
        # Trouve le prochain maximum local après ce point de départ
        next_high_idx = None
        for high_idx in high_indices:
            if high_idx > start_idx:
                next_high_idx = high_idx
                break
        
        if next_high_idx is None:
            continue

        end_idx = next_high_idx
        distance_m = distances[end_idx] - distances[start_idx]
        duration_sec = times[end_idx] - times[start_idx]
        net_vertical_m = smoothed_altitudes[end_idx] - smoothed_altitudes[start_idx]
        
        if distance_m < min_distance_m or duration_sec <= 0 or net_vertical_m < min_vertical_m:
            continue

        avg_grade_pct = (net_vertical_m / distance_m) * 100.0 if distance_m > 0 else 0.0

        avg_speed_kmh = (distance_m / duration_sec) * 3.6 if duration_sec > 0 else None
        avg_pace_sec_per_km = (duration_sec / distance_m) * 1000.0 if distance_m > 0 else None
        candidates.append({
            "start_idx": start_idx,
            "end_idx": end_idx,
            "distance_m": distance_m,
            "duration_sec": duration_sec,
            "gain_m": max(altitudes[end_idx] - altitudes[start_idx], 0.0),
            "drop_m": max(altitudes[start_idx] - altitudes[end_idx], 0.0),
            "start_distance_m": distances[start_idx],
            "end_distance_m": distances[end_idx],
            "start_time_sec": times[start_idx],
            "end_time_sec": times[end_idx],
            "start_altitude_m": altitudes[start_idx],
            "end_altitude_m": altitudes[end_idx],
            "start_altitude_smoothed_m": smoothed_altitudes[start_idx],
            "end_altitude_smoothed_m": smoothed_altitudes[end_idx],
            "vertical_m": net_vertical_m,
            "net_vertical_m": net_vertical_m,
            "avg_grade_pct": avg_grade_pct,
            "avg_speed_kmh": avg_speed_kmh,
            "avg_pace_sec_per_km": avg_pace_sec_per_km,
            "vam_m_per_h": (net_vertical_m / duration_sec) * 3600.0 if duration_sec > 0 else None,
        })
    return candidates


def _build_longest_climb_parts(longest_climb: dict | None, sport_norm: str) -> list[str]:
    if not longest_climb:
        return []

    climb_parts = [f"{(longest_climb.get('distance_m', 0.0) / 1000.0):.1f} km"]
    net_vertical = round(longest_climb.get("net_vertical_m", 0.0))
    if net_vertical > 0:
        climb_parts.append(f"D+ {net_vertical} m")

    vam = longest_climb.get("vam_m_per_h")
    if vam:
        climb_parts.append(f"{round(vam)} m/h")

    if sport_norm == "ride":
        speed_kmh = longest_climb.get("avg_speed_kmh")
        if speed_kmh and speed_kmh > 0:
            climb_parts.append(f"{speed_kmh:.1f} km/h")
    else:
        pace_str = _format_pace(longest_climb.get("avg_pace_sec_per_km"))
        if pace_str:
            climb_parts.append(pace_str)

    return climb_parts


def _build_directional_segments(
    time_stream,
    distance_stream,
    altitude_stream,
    direction: str,
    *,
    altitude_deadband_m: float = 0.8,
    max_gap_distance_m: float = 150.0,
    max_gap_time_s: float = 90.0,
    min_distance_m: float = 400.0,
    min_vertical_m: float = 40.0,
    min_grade_pct: float = 2.0,
) -> list[dict]:
    points = _build_time_distance_alt_points(time_stream, distance_stream, altitude_stream)
    if len(points) < 2:
        return []

    direction_sign = 1 if direction == "climb" else -1
    segments: list[dict] = []
    current = None
    pending_gap_distance = 0.0
    pending_gap_time = 0.0
    pending_gap_vertical = 0.0

    def _new_segment(start_idx: int, end_idx: int, delta_d: float, delta_a: float, delta_t: float) -> dict:
        gain = max(delta_a, 0.0)
        drop = max(-delta_a, 0.0)
        return {
            "start_idx": start_idx,
            "end_idx": end_idx,
            "distance_m": max(delta_d, 0.0),
            "duration_sec": max(delta_t, 0.0),
            "gain_m": gain,
            "drop_m": drop,
            "start_distance_m": points[start_idx][1],
            "end_distance_m": points[end_idx][1],
            "start_time_sec": points[start_idx][0],
            "end_time_sec": points[end_idx][0],
            "start_altitude_m": points[start_idx][2],
            "end_altitude_m": points[end_idx][2],
        }

    def _finalize_segment(segment: dict | None):
        if not segment:
            return
        distance_m = float(segment.get("distance_m") or 0.0)
        duration_sec = float(segment.get("duration_sec") or 0.0)
        gain_m = float(segment.get("gain_m") or 0.0)
        drop_m = float(segment.get("drop_m") or 0.0)
        vertical_m = gain_m if direction == "climb" else drop_m
        opposite_m = drop_m if direction == "climb" else gain_m
        net_vertical_m = max(vertical_m - opposite_m, 0.0)
        if distance_m < min_distance_m or vertical_m < min_vertical_m or duration_sec <= 0:
            return
        grade_pct = (net_vertical_m / distance_m) * 100.0 if distance_m > 0 else 0.0
        if grade_pct < min_grade_pct:
            return
        speed_kmh = (distance_m / duration_sec) * 3.6 if duration_sec > 0 else None
        segment["vertical_m"] = vertical_m
        segment["net_vertical_m"] = net_vertical_m
        segment["avg_grade_pct"] = grade_pct
        segment["avg_speed_kmh"] = speed_kmh
        segment["vam_m_per_h"] = (net_vertical_m / duration_sec) * 3600.0 if direction == "climb" and duration_sec > 0 else None
        segments.append(segment)

    for idx in range(1, len(points)):
        prev_t, prev_d, prev_a = points[idx - 1]
        curr_t, curr_d, curr_a = points[idx]
        delta_t = curr_t - prev_t
        delta_d = curr_d - prev_d
        delta_a = curr_a - prev_a
        if delta_t <= 0 or delta_d < 0:
            continue

        if abs(delta_a) <= altitude_deadband_m:
            step_dir = 0
        elif delta_a * direction_sign > 0:
            step_dir = direction_sign
        else:
            step_dir = -direction_sign

        if step_dir == direction_sign:
            if current is None:
                current = _new_segment(idx - 1, idx, delta_d, delta_a, delta_t)
            else:
                if pending_gap_distance and (
                    pending_gap_distance <= max_gap_distance_m or pending_gap_time <= max_gap_time_s
                ):
                    current["distance_m"] += pending_gap_distance
                    current["duration_sec"] += pending_gap_time
                    if pending_gap_vertical > 0:
                        current["gain_m"] += pending_gap_vertical
                    elif pending_gap_vertical < 0:
                        current["drop_m"] += -pending_gap_vertical
                    current["end_idx"] = idx - 1
                    current["end_distance_m"] = points[idx - 1][1]
                    current["end_time_sec"] = points[idx - 1][0]
                    current["end_altitude_m"] = points[idx - 1][2]
                pending_gap_distance = 0.0
                pending_gap_time = 0.0
                pending_gap_vertical = 0.0
                current["distance_m"] += delta_d
                current["duration_sec"] += delta_t
                if delta_a > 0:
                    current["gain_m"] += delta_a
                elif delta_a < 0:
                    current["drop_m"] += -delta_a
                current["end_idx"] = idx
                current["end_distance_m"] = curr_d
                current["end_time_sec"] = curr_t
                current["end_altitude_m"] = curr_a
            continue

        if current is None:
            continue

        if step_dir == 0:
            pending_gap_distance += delta_d
            pending_gap_time += delta_t
            pending_gap_vertical += delta_a
            if pending_gap_distance > max_gap_distance_m and pending_gap_time > max_gap_time_s:
                _finalize_segment(current)
                current = None
                pending_gap_distance = 0.0
                pending_gap_time = 0.0
                pending_gap_vertical = 0.0
            continue

        _finalize_segment(current)
        current = None
        pending_gap_distance = 0.0
        pending_gap_time = 0.0
        pending_gap_vertical = 0.0

    _finalize_segment(current)
    return segments


def _compute_longest_climb_summary(time_stream, distance_stream, altitude_stream) -> dict | None:
    points = _build_time_distance_alt_points(time_stream, distance_stream, altitude_stream)
    climbs = _build_extrema_based_climb_candidates(
        points,
        smoothing_radius_m=25.0,
        extrema_window_m=400.0,
        min_distance_m=200.0,
        min_vertical_m=10.0,
        min_grade_pct=0.0,
    )
    if not climbs:
        return None
    return max(
        climbs,
        key=lambda seg: (
            seg.get("net_vertical_m") or 0.0,
            seg.get("distance_m") or 0.0,
        ),
    )


def _compute_descent_summaries(time_stream, distance_stream, altitude_stream) -> dict:
    descents = _build_directional_segments(
        time_stream,
        distance_stream,
        altitude_stream,
        "descent",
        min_distance_m=300.0,
        min_vertical_m=30.0,
        min_grade_pct=3.0,
    )
    if not descents:
        return {"count": 0, "longest_descent": None}
    longest = max(descents, key=lambda seg: (seg.get("distance_m") or 0.0, seg.get("net_vertical_m") or 0.0))
    return {"count": len(descents), "longest_descent": longest}


def _slope_band_from_grade(grade_percent: float | None) -> str | None:
    if grade_percent is None:
        return None
    for min_v, max_v, band_id, _label in SLOPE_BANDS_DEF:
        if min_v <= grade_percent < max_v:
            return band_id
    return None


def _compute_slope_distribution_from_gpx(
    content: bytes,
    *,
    smoothing_radius_m: float = 0.0,
    max_profile_points: int = 800,
) -> tuple[dict[str, float], float, list[dict], list[dict]]:
    try:
        root = ET.fromstring(content)
    except ET.ParseError as exc:
        raise ValueError(f"GPX invalide ({exc})")

    ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}
    points = []
    for trkpt in root.findall(".//gpx:trkpt", ns):
        lat = trkpt.get("lat")
        lon = trkpt.get("lon")
        ele_node = trkpt.find("gpx:ele", ns)
        if lat is None or lon is None:
            continue
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except ValueError:
            continue
        ele = None
        if ele_node is not None and ele_node.text:
            try:
                ele = float(ele_node.text)
            except ValueError:
                pass
        points.append((lat_f, lon_f, ele))

    if len(points) < 2:
        raise ValueError("GPX nécessite au moins deux points pour calculer la pente.")

    # Certaines traces publiques contiennent un bruit altimétrique très fin. Sans
    # filtrage, chacune de ces oscillations est comptée comme du D+, ce qui peut
    # largement gonfler le dénivelé. Le lissage est opt-in, course par course.
    elevations = [point[2] for point in points]
    if smoothing_radius_m > 0 and all(elevation is not None for elevation in elevations):
        cumulative_distances = [0.0]
        for index in range(1, len(points)):
            cumulative_distances.append(
                cumulative_distances[-1]
                + _haversine_m(points[index - 1][0], points[index - 1][1], points[index][0], points[index][1])
            )
        elevations = _smooth_altitudes_by_distance(
            [(0.0, distance, float(points[index][2])) for index, distance in enumerate(cumulative_distances)],
            radius_m=smoothing_radius_m,
        )

    dist_by_band: dict[str, float] = {}
    total_distance = 0.0
    prev = points[0]
    elevation_profile = []
    if elevations[0] is not None:
        elevation_profile.append({"distance_km": 0.0, "elevation_m": elevations[0], "grade_percent": 0.0, "latitude": prev[0], "longitude": prev[1]})

    km_segments: list[dict] = []

    def _get_km_segment(idx: int) -> dict:
        while len(km_segments) <= idx:
            km_segments.append(
                {
                    "km_index": len(km_segments) + 1,
                    "start_distance_m": len(km_segments) * 1000.0,
                    "distance_m": 0.0,
                    "elevation_gain_m": 0.0,
                    "elevation_loss_m": 0.0,
                    "slope_dist": {},
                }
            )
        return km_segments[idx]

    cumulative_distance = 0.0

    for idx in range(1, len(points)):
        lat1, lon1, _raw_ele1 = prev
        lat2, lon2, _raw_ele2 = points[idx]
        ele1 = elevations[idx - 1]
        ele2 = elevations[idx]
        d = _haversine_m(lat1, lon1, lat2, lon2)
        if d <= 0.5:
            prev = points[idx]
            continue

        if d <= 0:
            prev = points[idx]
            continue

        total_distance += d
        grade = None
        if ele1 is not None and ele2 is not None:
            grade = ((ele2 - ele1) / d) * 100.0
        if ele2 is not None:
            elevation_profile.append(
                {
                    "distance_km": total_distance / 1000.0,
                    "elevation_m": ele2,
                    "grade_percent": grade if grade is not None else 0.0,
                    "latitude": lat2,
                    "longitude": lon2,
                }
            )

        delta_ele = 0.0
        if ele1 is not None and ele2 is not None:
            delta_ele = ele2 - ele1
        gain = max(delta_ele, 0.0)
        loss = max(-delta_ele, 0.0)

        band = _slope_band_from_grade(grade)
        if band:
            dist_by_band[band] = dist_by_band.get(band, 0.0) + d

        remaining = d
        while remaining > 0:
            km_idx = int(cumulative_distance // 1000)
            segment = _get_km_segment(km_idx)
            next_boundary = (km_idx + 1) * 1000.0
            room = next_boundary - cumulative_distance
            take = min(remaining, room)
            fraction = take / d
            segment["distance_m"] += take
            if gain > 0:
                segment["elevation_gain_m"] += gain * fraction
            if loss > 0:
                segment["elevation_loss_m"] += loss * fraction
            if band:
                slope_dist = segment["slope_dist"]
                slope_dist[band] = slope_dist.get(band, 0.0) + take
            remaining -= take
            cumulative_distance += take

        prev = points[idx]

    if not dist_by_band:
        raise ValueError("Impossible de déterminer les pentes (altitudes manquantes ?).")

    # Limite la charge du navigateur tout en gardant le relief lisible.
    if max_profile_points > 0 and len(elevation_profile) > max_profile_points:
        step = max(1, len(elevation_profile) // max_profile_points)
        elevation_profile = elevation_profile[::step]
        if elevation_profile[-1]["distance_km"] != total_distance / 1000.0 and points[-1][2] is not None:
            elevation_profile.append(
                {
                    "distance_km": total_distance / 1000.0,
                    "elevation_m": points[-1][2],
                    "grade_percent": elevation_profile[-1].get("grade_percent", 0.0),
                    "latitude": points[-1][0],
                    "longitude": points[-1][1],
                }
            )

    return dist_by_band, total_distance, km_segments, elevation_profile


def _calibrate_gpx_projection_to_official_course(
    dist_by_band: dict[str, float],
    total_distance_m: float,
    km_segments: list[dict],
    elevation_profile: list[dict],
    course: dict | None,
) -> tuple[dict[str, float], float, list[dict], list[dict]]:
    """Calibrate a noisy/short GPX to the organiser's published course totals.

    This is deliberately enabled in the course JSON, rather than globally: a
    runner-uploaded GPX must always keep its own measured characteristics.
    """
    if not course or not course.get("calibrate_gpx_to_official"):
        return dist_by_band, total_distance_m, km_segments, elevation_profile

    try:
        official_distance_m = float(course.get("distance_km")) * 1000.0
    except (TypeError, ValueError):
        official_distance_m = 0.0
    if total_distance_m > 0 and official_distance_m > 0:
        distance_ratio = official_distance_m / total_distance_m
        total_distance_m = official_distance_m
        dist_by_band = {band: distance * distance_ratio for band, distance in dist_by_band.items()}
        for segment in km_segments:
            segment["distance_m"] = float(segment.get("distance_m") or 0.0) * distance_ratio
            segment["start_distance_m"] = float(segment.get("start_distance_m") or 0.0) * distance_ratio
        for point in elevation_profile:
            point["distance_km"] = float(point.get("distance_km") or 0.0) * distance_ratio

    for field, official_field in (("elevation_gain_m", "elevation_gain_m"), ("elevation_loss_m", "elevation_loss_m")):
        try:
            target = float(course.get(official_field))
        except (TypeError, ValueError):
            continue
        current = sum(float(segment.get(field) or 0.0) for segment in km_segments)
        if current <= 0 or target < 0:
            continue
        ratio = target / current
        for segment in km_segments:
            segment[field] = float(segment.get(field) or 0.0) * ratio

    return dist_by_band, total_distance_m, km_segments, elevation_profile


def _get_session_user_id(request: Request) -> int | None:
    if not hasattr(request, "session"):
        return None
    raw = request.session.get("user_id")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        request.session.clear()
        return None


def _request_relative_url(request: Request) -> str:
    query = request.url.query
    if query:
        return f"{request.url.path}?{query}"
    return request.url.path


def _sanitize_next_path(raw: str | None) -> str | None:
    candidate = (raw or "").strip()
    if not candidate:
        return None
    parts = urlsplit(candidate)
    if parts.scheme or parts.netloc:
        return None
    if not parts.path.startswith("/"):
        return None
    if not parts.path.startswith("/ui/"):
        return None
    safe = parts.path
    if parts.query:
        safe = f"{safe}?{parts.query}"
    return safe


def _guard_user_route(request: Request, user_id: int | None = None):
    session_user_id = _get_session_user_id(request)
    if session_user_id is None:
        next_path = _sanitize_next_path(_request_relative_url(request))
        login_url = "/ui/login"
        if next_path:
            login_url = f"{login_url}?next={quote_plus(next_path)}"
        return RedirectResponse(url=login_url, status_code=302)

    if user_id is not None and session_user_id != int(user_id):
        if session_user_id == 1:
            return None
        return RedirectResponse(url=f"/ui/user/{session_user_id}", status_code=302)

    return None


def _guard_admin(request: Request):
    session_user_id = _get_session_user_id(request)
    if session_user_id is None:
        return RedirectResponse(url="/ui/login", status_code=302)
    if session_user_id != 1:
        return RedirectResponse(url=f"/ui/user/{session_user_id}", status_code=302)
    return None

def _normalize_connection_filter(value: str | None) -> str:
    value = (value or "all").strip().lower()
    if value in {"connected", "disconnected"}:
        return value
    return "all"


def _matches_connection_filter(is_connected: bool, filter_value: str) -> bool:
    if filter_value == "connected":
        return is_connected
    if filter_value == "disconnected":
        return not is_connected
    return True


def _safe_positive_int(value: str | None, default: int) -> int:
    try:
        parsed = int(value or "")
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _build_url_with_query(request: Request, **updates) -> str:
    params = dict(request.query_params)
    for key, value in updates.items():
        if value is None:
            params.pop(key, None)
        else:
            params[key] = str(value)
    query = urlencode(params)
    return f"{request.url.path}?{query}" if query else request.url.path


def _collect_admin_user_rows(
    db: Session,
    *,
    strava_filter: str = "all",
    libre_filter: str = "all",
    dexcom_filter: str = "all",
    carelink_filter: str = "all",
    nightscout_filter: str = "all",
) -> list[dict]:
    credit_balances = {
        user_id: int(credits or 0)
        for user_id, credits in db.query(PlanCreditWallet.user_id, PlanCreditWallet.credits).all()
    }
    users = (
        db.query(User)
        .options(
            selectinload(User.strava_tokens),
            selectinload(User.libre_credentials),
            selectinload(User.dexcom_tokens),
            selectinload(User.carelink_credentials),
        )
        .order_by(User.id.asc())
        .all()
    )
    rows: list[dict] = []
    for u in users:
        carelink = u.carelink_credentials
        row = {
            "id": u.id,
            "email": u.email,
            "created_at": u.created_at,
            "has_strava": bool(u.strava_tokens),
            "libre_email": u.libre_credentials.email if u.libre_credentials else None,
            "has_dexcom": has_dexcom_share_credentials(u.dexcom_tokens),
            "has_carelink": bool(carelink and carelink.username),
            "has_nightscout": bool(u.nightscout_credentials and u.nightscout_credentials.base_url),
            "nightscout_url": u.nightscout_credentials.base_url if u.nightscout_credentials else None,
            "carelink_region": carelink.region if carelink else None,
            "carelink_status": carelink.status if carelink else None,
            "carelink_last_sync_at": carelink.last_sync_at if carelink else None,
            "carelink_error_message": carelink.error_message if carelink else None,
            "glucose_provider": (u.glucose_provider or "").upper() if u.glucose_provider else None,
            "cgm_source": (u.cgm_source or "").upper() if u.cgm_source else None,
            # A wallet is created lazily; accounts without one start at zero credit.
            "plan_credits": credit_balances.get(u.id, 1),
        }
        has_libre = bool(row["libre_email"])
        if not _matches_connection_filter(row["has_strava"], strava_filter):
            continue
        if not _matches_connection_filter(has_libre, libre_filter):
            continue
        if not _matches_connection_filter(row["has_dexcom"], dexcom_filter):
            continue
        if not _matches_connection_filter(row["has_carelink"], carelink_filter):
            continue
        if not _matches_connection_filter(row["has_nightscout"], nightscout_filter):
            continue
        rows.append(row)
    return rows


def _collect_enrichment_admin_rows(
    db: Session,
    *,
    limit: int = 25,
) -> dict:
    status_counts = {
        "pending": 0,
        "processing": 0,
        "retry": 0,
        "succeeded": 0,
        "failed": 0,
    }
    for status, count in (
        db.query(ActivityEnrichmentJob.status, func.count(ActivityEnrichmentJob.id))
        .group_by(ActivityEnrichmentJob.status)
        .all()
    ):
        if status:
            status_counts[str(status)] = int(count or 0)

    recent_jobs_query = (
        db.query(ActivityEnrichmentJob, User)
        .join(User, ActivityEnrichmentJob.user_id == User.id)
        .order_by(
            ActivityEnrichmentJob.updated_at.desc(),
            ActivityEnrichmentJob.created_at.desc(),
            ActivityEnrichmentJob.id.desc(),
        )
        .limit(max(1, limit))
    )

    recent_jobs = []
    for job, user in recent_jobs_query.all():
        recent_jobs.append(
            {
                "id": job.id,
                "user_id": job.user_id,
                "user_email": user.email,
                "activity_id": job.strava_activity_id,
                "status": job.status or "unknown",
                "trigger_source": job.trigger_source or "—",
                "attempts": int(job.attempts or 0),
                "last_reason": job.last_reason or "—",
                "last_error": (job.last_error or "").strip() or None,
                "next_retry_at": job.next_retry_at.strftime("%Y-%m-%d %H:%M:%S") if job.next_retry_at else "—",
                "locked_at": job.locked_at.strftime("%Y-%m-%d %H:%M:%S") if job.locked_at else "—",
                "started_at": job.started_at.strftime("%Y-%m-%d %H:%M:%S") if job.started_at else "—",
                "completed_at": job.completed_at.strftime("%Y-%m-%d %H:%M:%S") if job.completed_at else "—",
                "updated_at": job.updated_at.strftime("%Y-%m-%d %H:%M:%S") if job.updated_at else "—",
                "can_retry": (job.status or "") != "processing",
            }
        )

    return {
        "status_counts": status_counts,
        "total_jobs": sum(status_counts.values()),
        "recent_jobs": recent_jobs,
        "worker_poll_seconds": ENRICHMENT_WORKER_POLL_SECONDS,
        "retry_base_seconds": ENRICHMENT_RETRY_BASE_SECONDS,
        "retry_max_seconds": ENRICHMENT_RETRY_MAX_SECONDS,
        "max_attempts": ENRICHMENT_MAX_ATTEMPTS,
    }


# -----------------------------------------------------------------------------
# Instance FastAPI + static + templates
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
RETENTION_JOB_INTERVAL_DAYS = max(1, int(os.getenv("ACTIVITY_RETENTION_JOB_INTERVAL_DAYS", "5") or "5"))
RETENTION_JOB_HOUR_LOCAL = min(23, max(0, int(os.getenv("ACTIVITY_RETENTION_JOB_HOUR_LOCAL", "3") or "3")))
RETENTION_JOB_MINUTE_LOCAL = min(59, max(0, int(os.getenv("ACTIVITY_RETENTION_JOB_MINUTE_LOCAL", "0") or "0")))
RETENTION_JOB_ANCHOR_DATE = dt.date(2026, 1, 1)
PAGE_VIEW_REFRESH_LOCK = threading.Lock()
PAGE_VIEW_REFRESH_USERS: set[int] = set()
ENRICHMENT_ACTIVITY_LOCK = threading.Lock()
ENRICHMENT_ACTIVE_KEYS: set[tuple[int, int]] = set()
ENRICHMENT_WORKER_POLL_SECONDS = max(
    15, int(os.getenv("ACTIVITY_ENRICHMENT_WORKER_POLL_SECONDS", "60") or "60")
)
ENRICHMENT_RETRY_BASE_SECONDS = max(
    30, int(os.getenv("ACTIVITY_ENRICHMENT_RETRY_BASE_SECONDS", "180") or "180")
)
ENRICHMENT_RETRY_MAX_SECONDS = max(
    ENRICHMENT_RETRY_BASE_SECONDS,
    int(os.getenv("ACTIVITY_ENRICHMENT_RETRY_MAX_SECONDS", "1800") or "1800"),
)
ENRICHMENT_MAX_ATTEMPTS = max(
    1, int(os.getenv("ACTIVITY_ENRICHMENT_MAX_ATTEMPTS", "6") or "6")
)
ENRICHMENT_RETRYABLE_REASONS = {
    "libre_cooldown",
    "libre_rate_limited",
    "libre_error",
    "dexcom_error",
    "carelink_error",
    "nightscout_error",
    "provider_error",
    "strava_fetch_error",
    "strava_update_error",
    "activity_busy",
}

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY, same_site="lax")

_raw_template_response = templates.TemplateResponse


def _template_response_compat(*args, **kwargs):
    """
    Compatibilité Starlette:
    - ancien style: TemplateResponse("page.html", {"request": request, ...}, ...)
    - nouveau style: TemplateResponse(request, "page.html", {...}, ...)
    """
    if args and isinstance(args[0], str):
        name = args[0]
        context = args[1] if len(args) > 1 else kwargs.get("context")
        if not isinstance(context, dict):
            raise TypeError("TemplateResponse context must be a dict.")
        request = context.get("request") or kwargs.pop("request", None)
        if request is None:
            raise ValueError("TemplateResponse context must include 'request'.")
        remaining = args[2:]
        return _raw_template_response(request, name, context, *remaining, **kwargs)
    return _raw_template_response(*args, **kwargs)


templates.TemplateResponse = _template_response_compat

# -----------------------------------------------------------------------------
# Routers
# -----------------------------------------------------------------------------
app.include_router(auth.router)            # /auth/* (signup/login)
app.include_router(auth_strava.router)     # /auth/strava/* (oauth strava)
app.include_router(auth_dexcom.router)     # /auth/dexcom/* (oauth dexcom)
app.include_router(webhooks.router)        # /webhooks/strava


# -----------------------------------------------------------------------------
# Helpers CGM : stockage & récupération des courbes
# -----------------------------------------------------------------------------

def store_glucose_points_from_graph(db, user_id: int, points: list, source: str = "archive") -> int:
    """
    Stocke en base les points de glycémie provenant d'une source CGM
    (LibreLinkUp, Dexcom, etc.) dans la table glucose_points.

    - Normalise tous les timestamps en UTC naïf (comme cgm_service).
    - Évite les doublons grâce au couple (user_id, ts).
    - Paramètre `source` permet d'étiqueter les points : "archive_libre", "dexcom", etc.

    Retourne le nombre de nouveaux points insérés.
    """
    if not points:
        return 0

    inserted = 0

    for p in points:
        ts = p["ts"]

        # Normalisation en UTC naïf
        if ts.tzinfo is not None:
            ts_utc_naive = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
        else:
            ts_utc_naive = ts  # on considère que c'est déjà du UTC

        mgdl = p.get("mgdl")
        trend = p.get("trend")

        if mgdl is None:
            continue

        # Vérifier si le point existe déjà pour (user_id, ts)
        existing = (
            db.query(GlucosePoint)
            .filter(GlucosePoint.user_id == user_id, GlucosePoint.ts == ts_utc_naive)
            .one_or_none()
        )
        if existing:
            continue

        gp = GlucosePoint(
            user_id=user_id,
            ts=ts_utc_naive,
            mgdl=mgdl,
            trend=trend,
            source=source,
        )
        db.add(gp)
        inserted += 1

    if inserted > 0:
        db.commit()

    return inserted


def _storage_source_from_provider(source_label: str | None) -> str:
    if source_label == "dexcom":
        return "dexcom"
    if source_label == "medtronic_carelink":
        return "medtronic_carelink"
    if source_label == "nightscout":
        return "nightscout"
    return "archive_libre"


def _provider_anchor(provider: str) -> str:
    return {
        "abbott": "libre",
        "dexcom": "dexcom",
        "medtronic_carelink": "carelink",
        "nightscout": "nightscout",
    }.get(provider, "profile")


def _provider_is_configured(user: User, provider: str) -> bool:
    if provider == "abbott":
        return user.libre_credentials is not None
    if provider == "dexcom":
        return has_dexcom_share_credentials(user.dexcom_tokens)
    if provider == "medtronic_carelink":
        return bool(user.carelink_credentials and user.carelink_credentials.username)
    if provider == "nightscout":
        return bool(user.nightscout_credentials and user.nightscout_credentials.base_url)
    return False


def _compute_next_retention_job_run(now_local: dt.datetime | None = None) -> dt.datetime:
    current = now_local or dt.datetime.now().astimezone()
    if current.tzinfo is None:
        current = current.astimezone()

    candidate_date = current.date()
    days_since_anchor = (candidate_date - RETENTION_JOB_ANCHOR_DATE).days
    remainder = days_since_anchor % RETENTION_JOB_INTERVAL_DAYS
    if remainder:
        candidate_date += dt.timedelta(days=RETENTION_JOB_INTERVAL_DAYS - remainder)

    candidate = current.replace(
        year=candidate_date.year,
        month=candidate_date.month,
        day=candidate_date.day,
        hour=RETENTION_JOB_HOUR_LOCAL,
        minute=RETENTION_JOB_MINUTE_LOCAL,
        second=0,
        microsecond=0,
    )
    if candidate <= current:
        candidate += dt.timedelta(days=RETENTION_JOB_INTERVAL_DAYS)
    return candidate


def run_activity_retention_loop():
    logger.info(
        "[RETENTION] Thread planifié lancé (tous les %s jours à %02d:%02d heure locale).",
        RETENTION_JOB_INTERVAL_DAYS,
        RETENTION_JOB_HOUR_LOCAL,
        RETENTION_JOB_MINUTE_LOCAL,
    )

    while True:
        next_run = _compute_next_retention_job_run()
        wait_seconds = max(1.0, (next_run - dt.datetime.now().astimezone()).total_seconds())
        logger.info("[RETENTION] Prochaine purge planifiée le %s.", next_run.isoformat())
        time.sleep(wait_seconds)

        db = SessionLocal()
        try:
            purged_by_user = purge_old_activities_for_all_users(
                db,
                now=dt.datetime.now(dt.timezone.utc),
            )
            total_purged = sum(purged_by_user.values())
            if total_purged:
                logger.info(
                    "[RETENTION] Purge planifiée terminée: %s activités live supprimées sur %s utilisateur(s).",
                    total_purged,
                    len(purged_by_user),
                )
            else:
                logger.info("[RETENTION] Purge planifiée terminée: aucune activité live à supprimer.")
        except Exception:
            logger.exception("[RETENTION] Erreur pendant la purge planifiée.")
        finally:
            db.close()


def get_cgm_graph_for_user(
    db,
    user_id: int,
    start: dt.datetime,
    end: dt.datetime,
):
    """
    Retourne (graph, source_label) pour un utilisateur donné.

    - graph: liste de dicts [{'ts', 'mgdl', 'trend'}]
    - source_label: 'libre', 'dexcom' ou None si aucune donnée.

    Stratégie :
      1) On lit user.cgm_source.
      2) Si 'libre' → LibreLinkUp en priorité, fallback Dexcom.
      3) Si 'dexcom' → Dexcom en priorité, fallback LibreLinkUp.
      4) Si None → stratégie par défaut Dexcom > LibreLinkUp.
    """

    user = db.query(User).get(user_id)
    if not user:
        return [], None, {"reason": "user_not_found", "attempted_sources": [], "skipped_sources": []}
    points, source_label, meta = fetch_realtime_points_for_user(
        db,
        user,
        context="activity_import",
    )
    return points, source_label, meta


def _run_page_view_glucose_refresh(user_id: int, page_name: str) -> None:
    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return

        should_refresh, reason = should_attempt_page_refresh(db, user)
        if not should_refresh:
            if reason:
                print(f"[CGM] user={user.id} -> refresh {page_name} ignoré ({reason}).")
            return

        points, source_label, fetch_meta = fetch_realtime_points_for_user(
            db,
            user,
            context=f"page_view:{page_name}",
        )
        if not points or not source_label:
            if fetch_meta["reason"] == "libre_cooldown":
                print(f"[CGM] user={user.id} -> refresh {page_name} reporté (cooldown Libre global).")
            else:
                print(f"[CGM] user={user.id} -> refresh {page_name} sans nouvelle donnée.")
            return

        source = _storage_source_from_provider(source_label)
        inserted = store_glucose_points_from_graph(db, user_id=user.id, points=points, source=source)
        print(
            f"[CGM] user={user.id} -> refresh {page_name} via {source_label}: "
            f"{len(points)} points lus, {inserted} nouveaux points."
        )
    except Exception:
        logger.exception("Erreur pendant le refresh glucose de page user=%s page=%s", user_id, page_name)
    finally:
        db.close()
        with PAGE_VIEW_REFRESH_LOCK:
            PAGE_VIEW_REFRESH_USERS.discard(user_id)


def _maybe_refresh_glucose_for_page_view(db, user: User, *, page_name: str) -> None:
    record_glucose_page_view(user.id, page_name)
    with PAGE_VIEW_REFRESH_LOCK:
        if user.id in PAGE_VIEW_REFRESH_USERS:
            print(
                f"[CGM] user={user.id} -> refresh {page_name} déjà en cours, "
                "on laisse la page se charger."
            )
            return
        PAGE_VIEW_REFRESH_USERS.add(user.id)

    print(f"[CGM] user={user.id} -> refresh {page_name} thread démarré.")
    threading.Thread(
        target=_run_page_view_glucose_refresh,
        args=(user.id, page_name),
        daemon=True,
    ).start()


# -----------------------------------------------------------------------------
# CGM Polling au démarrage
# -----------------------------------------------------------------------------

@app.on_event("startup")
def startup_event():
    # 1) Créer les tables si elles n'existent pas (dont glucose_points)
    init_db()
    print("[DB] Tables vérifiées/créées.")
    # 2) Démarrer le polling CGM dans un thread séparé
    t = threading.Thread(target=run_polling_loop, daemon=True)
    t.start()
    print("[CGM] Thread de polling lancé.")

    # 3) Démarrer la purge planifiée des activités live
    retention_thread = threading.Thread(target=run_activity_retention_loop, daemon=True)
    retention_thread.start()
    print(
        f"[RETENTION] Thread de purge planifiée lancé "
        f"(tous les {RETENTION_JOB_INTERVAL_DAYS} jours à "
        f"{RETENTION_JOB_HOUR_LOCAL:02d}:{RETENTION_JOB_MINUTE_LOCAL:02d})."
    )

    enrichment_thread = threading.Thread(target=run_enrichment_retry_loop, daemon=True)
    enrichment_thread.start()
    print(
        f"[ENRICHMENT] Thread de reprise lancé "
        f"(scan toutes les {ENRICHMENT_WORKER_POLL_SECONDS}s)."
    )


# -----------------------------------------------------------------------------
# Healthcheck
# -----------------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------------------------------------------------
# Calcul des VAM max (5 / 15 / 30 min) et enregistrement en base
# -------------------------------------------------------------------
def compute_and_store_vam_peaks(db, activity, user_id: int):
    """
    Calcule les pics de VAM sur 5 / 15 / 30 min pour une activité donnée
    et les stocke dans activity_vam_peaks.
    Met aussi à jour :
      - les caches sur Activity (max_vam_5m / 15m / 30m)
      - les records persos (user_vam_prs).
    """
    from app import models  # pour être sûr

    stream = (
        db.query(models.ActivityStreamPoint)
        .filter(models.ActivityStreamPoint.activity_id == activity.id)
        .order_by(models.ActivityStreamPoint.idx)
        .all()
    )
    if not stream or len(stream) < 2:
        print("⚠️ Pas assez de points pour calculer les VAM.")
        return

    # On construit des séries bien alignées (temps + altitude)
    times = []
    alts = []
    ts_abs = []
    valid_points = []

    for p in stream:
        if p.elapsed_time is None or p.altitude is None:
            continue
        times.append(float(p.elapsed_time))
        alts.append(float(p.altitude))
        ts_abs.append(p.timestamp)
        valid_points.append(p)

    if len(times) < 2:
        print("⚠️ Données incomplètes pour calculer les VAM (times/altitude).")
        return

    running_activity = canonicalize_sport_label(getattr(activity, "sport", "run")) == "run"
    invalid_interval_prefix = [0]
    for previous, current in zip(valid_points, valid_points[1:]):
        invalid_interval_prefix.append(
            invalid_interval_prefix[-1]
            + (0 if is_valid_activity_stream_interval(previous, current, running=running_activity) else 1)
        )

    def window_crosses_invalid_interval(start_index: int, end_index: int) -> bool:
        return invalid_interval_prefix[end_index] > invalid_interval_prefix[start_index]

    def compute_vam_for_window(window_min: int):
        window_sec = window_min * 60
        best_vam = 0.0
        best = None  # (i, j, gain)

        n = len(times)
        for i in range(n):
            t0 = times[i]
            t1_target = t0 + window_sec

            j = None
            for k in range(i + 1, n):
                if times[k] >= t1_target:
                    j = k
                    break
            if j is None:
                continue
            if window_crosses_invalid_interval(i, j):
                continue

            gain = alts[j] - alts[i]
            if gain <= 0:
                continue

            vam = (gain / window_sec) * 3600.0  # m/h
            if vam > best_vam:
                best_vam = vam
                best = (i, j, gain)

        return best_vam, best

    # --- Calcul VAM pour 5 / 15 / 30 min ---
    windows = [5, 15, 30]
    bests = {}  # window_min -> dict

    for window in windows:
        max_vam, seg = compute_vam_for_window(window)
        if not seg:
            continue
        i, j, gain = seg

        loss = 0.0  # ta version "simple" ne calculait pas la perte, on garde 0
        loss_pct = 0.0

        start_ts = ts_abs[i] if i < len(ts_abs) else None
        end_ts = ts_abs[j] if j < len(ts_abs) else None

        bests[window] = {
            "vam": max_vam,
            "gain": gain,
            "loss": loss,
            "loss_pct": loss_pct,
            "start_idx": i,
            "end_idx": j,
            "start_ts": _safe_dt(start_ts),
            "end_ts": _safe_dt(end_ts),
        }

    if not bests:
        # Rien trouvé sur aucune fenêtre
        return

    sport = activity.sport
    user_id = int(user_id)

    # --- Upsert dans ActivityVamPeak + MAJ caches Activity ---
    field_map = {
        5: "max_vam_5m",
        15: "max_vam_15m",
        30: "max_vam_30m",
    }

    for window, data in bests.items():
        vam_value = float(data["vam"])

        # a) MAJ cache dans Activity
        field_name = field_map.get(window)
        if field_name:
            setattr(activity, field_name, vam_value)

        # b) Upsert ActivityVamPeak
        peak = (
            db.query(models.ActivityVamPeak)
            .filter(
                models.ActivityVamPeak.activity_id == activity.id,
                models.ActivityVamPeak.window_min == window,
            )
            .one_or_none()
        )
        if peak is None:
            peak = models.ActivityVamPeak(
                user_id=user_id,
                activity_id=activity.id,
                sport=sport,
                window_min=window,
                max_vam_m_per_h=vam_value,
                start_idx=data["start_idx"],
                end_idx=data["end_idx"],
                start_ts=data["start_ts"],
                end_ts=data["end_ts"],
                gain_m=data["gain"],
                loss_m=data["loss"],
                loss_pct_vs_gain=data["loss_pct"],
                distance_m=None,
                method="simple",
            )
            db.add(peak)
        else:
            peak.user_id = user_id
            peak.sport = sport
            peak.max_vam_m_per_h = vam_value
            peak.start_idx = data["start_idx"]
            peak.end_idx = data["end_idx"]
            peak.start_ts = data["start_ts"]
            peak.end_ts = data["end_ts"]
            peak.gain_m = data["gain"]
            peak.loss_m = data["loss"]
            peak.loss_pct_vs_gain = data["loss_pct"]
            peak.distance_m = None
            peak.method = "simple"

        # c) PR utilisateur
        _update_user_vam_pr(
            db,
            user_id=user_id,
            sport=sport,
            window_min=window,
            vam_value=vam_value,
            activity_id=activity.id,
            start_ts=data["start_ts"],
        )

    db.add(activity)
    db.commit()


# -------------------------------------------------------------------
# Mise à jour des records personnels VAM (user_vam_prs)
# -------------------------------------------------------------------
def _update_user_vam_pr(db, user_id, sport, window_min, vam_value, activity_id, start_ts):
    """
    Met à jour la table user_vam_prs si l'utilisateur bat son record.
    """
    from app import models

    existing = (
        db.query(models.UserVamPR)
        .filter_by(user_id=user_id, sport=sport, window_min=window_min)
        .first()
    )

    if existing:
        if vam_value > existing.vam_m_per_h:
            existing.vam_m_per_h = vam_value
            existing.activity_id = activity_id
            existing.start_ts = _safe_dt(start_ts)
            existing.updated_at = dt.datetime.utcnow()
            db.commit()
            print(f"🏆 Nouveau record VAM {window_min} min : {vam_value:.1f} m/h ({sport})")
    else:
        new_pr = models.UserVamPR(
            user_id=user_id,
            sport=sport,
            window_min=window_min,
            vam_m_per_h=vam_value,
            activity_id=activity_id,
            start_ts=_safe_dt(start_ts),
        )
        db.add(new_pr)
        db.commit()
        print(f"🥇 Premier record VAM {window_min} min : {vam_value:.1f} m/h ({sport})")

# -----------------------------------------------------------------------------
# Fonction cœur : traite une activité générique (Strava, GPX, FIT...)
# -----------------------------------------------------------------------------
async def process_activity_core(
    act: dict,
    streams: dict,
    user_id: int = 1,
    activity_id: int | None = None,
    cli: StravaClient | None = None,
    update_strava_description: bool = False,
    trigger_source: str = "activity_import",
    fetch_glucose: bool = True,
):
    def to_utc_aware(d: dt.datetime | None) -> dt.datetime | None:
        if d is None:
            return None
        # Si naïf => on suppose que c'est de l'UTC et on ajoute le tzinfo
        if d.tzinfo is None:
            return d.replace(tzinfo=dt.timezone.utc)
        # Si aware => on force en UTC
        return d.astimezone(dt.timezone.utc)

    db = SessionLocal()
    try:
        live_fetch_reason = None
        live_fetch_source = None
        live_fetch_points_count = 0
        strava_description_updated = False

        # 1) Activité -> bornes temps (aware UTC)
        start_raw = dt.datetime.fromisoformat(act["start_date"].replace("Z", "+00:00"))
        elapsed = act.get("elapsed_time") or act.get("moving_time") or 0
        end_raw = start_raw + dt.timedelta(seconds=int(elapsed))

        start_aw = to_utc_aware(start_raw)
        end_aw   = to_utc_aware(end_raw)

        # 2) Glycémie depuis la BASE -> on récupère en AWARE UTC.
        # Les imports historiques ne servent qu'au profil sportif : aucune
        # requête CGM externe n'est nécessaire ni souhaitable.
        graph_db = []
        needs_live_fetch = False
        if fetch_glucose:
            graph_db = load_glucose_graph_from_db(
                db,
                user_id=user_id,
                start=start_aw,
                end=end_aw,
                margin_min=CGM_MATCH_MARGIN_MIN,
            )
            needs_live_fetch = not glucose_graph_has_activity_coverage(
                graph_db,
                start_aw,
                end_aw,
                max_delta_sec=CGM_MATCH_MAX_DELTA_SEC,
            )

        # Fallback : si la couverture est insuffisante, on lit “live”, on insère, puis on recharge DB
        if needs_live_fetch:
            try:
                # Les clients externes préfèrent souvent des datetimes aware
                graph_live, source_label, fetch_meta = get_cgm_graph_for_user(
                    db=db, user_id=user_id, start=start_aw, end=end_aw
                )
                live_fetch_source = source_label
                live_fetch_reason = fetch_meta.get("reason") if fetch_meta else None
                if graph_live:
                    live_fetch_points_count = len(graph_live)
                    logger.info(
                        "[CGM] activity_import user=%s provider=%s start=%s end=%s points=%s",
                        user_id,
                        source_label,
                        start_aw.isoformat(),
                        end_aw.isoformat(),
                        len(graph_live),
                    )
                    src = _storage_source_from_provider(source_label)
                    # L’insertion normalise côté DB (peu importe), on rechargera en aware
                    store_glucose_points_from_graph(db, user_id=user_id, points=graph_live, source=src)
                    graph_db = load_glucose_graph_from_db(
                        db,
                        user_id=user_id,
                        start=start_aw,
                        end=end_aw,
                        margin_min=CGM_MATCH_MARGIN_MIN,
                    )
                else:
                    logger.info(
                        "[CGM] activity_import user=%s provider=%s no_live_points reason=%s",
                        user_id,
                        source_label,
                        live_fetch_reason,
                    )
            except Exception as e:
                live_fetch_reason = "provider_error"
                logger.exception(
                    "[CGM] activity_import user=%s provider=%s error=%s",
                    user_id,
                    source_label if "source_label" in locals() else None,
                    e,
                )

        # 3) Streams + matching CGM
        time_stream = streams.get("time", {}).get("data") or []
        hr_stream   = streams.get("heartrate", {}).get("data") or []

        # start_aw est AWARE UTC ; graph_db contient des ts AWARE UTC
        g_vals, g_trends, g_sources = match_glucose_to_time_stream(
            graph=graph_db,
            start=start_aw,
            time_stream=time_stream,
            max_delta_sec=CGM_MATCH_MAX_DELTA_SEC,
        )
        streams["glucose_mgdl"]   = {"data": g_vals}
        streams["glucose_trend"]  = {"data": g_trends}
        streams["glucose_source"] = {"data": g_sources}

        def _first_valid(seq):
            for v in seq:
                if v is not None:
                    return v
            return None

        def _last_valid(seq):
            for v in reversed(seq):
                if v is not None:
                    return v
            return None

        aligned_samples = []
        if start_aw and time_stream:
            for idx, val in enumerate(g_vals):
                if val is None:
                    continue
                if idx >= len(time_stream):
                    break
                t = time_stream[idx]
                if t is None:
                    continue
                try:
                    ts = start_aw + dt.timedelta(seconds=float(t))
                except Exception:
                    continue
                aligned_samples.append({"ts": ts, "mgdl": float(val)})

        # 4) Upsert activité + stats
        athlete_id = act.get("athlete", {}).get("id")
        if athlete_id is None:
            print("⚠️ athlete_id manquant, arrêt.")
            return

        # Prépare les samples pour les stats (priorité à la série alignée)
        if len(aligned_samples) >= 2:
            samples = aligned_samples
        else:
            samples = select_window(graph_db, start_aw, end_aw, buffer_min=0)

        stats = compute_stats(
            samples,
            activity_start=start_aw,
            activity_end=end_aw,
            start_value_hint=_first_valid(g_vals),
            end_value_hint=_last_valid(g_vals),
        )

        activity_obj = upsert_activity_record(
            db=db,
            user_id=user_id,
            athlete_id=athlete_id,
            strava_activity=act,
            glucose_stats=stats,
            summary_block=None,
        )

        # 4b) Calcul difficulté + niveau (pour les runs)
        score, level = compute_difficulty_and_level(
            distance_m=activity_obj.distance,
            elevation_m=activity_obj.total_elevation_gain,
            sport=activity_obj.sport,
        )
        if score is not None and level is not None:
            activity_obj.difficulty_score = score
            activity_obj.level = level
            db.add(activity_obj)
            db.commit()

        # 5) Enregistrer les points détaillés
        try:
            n_points = save_activity_stream_points(db=db, activity=activity_obj, streams=streams or {})
            print(f"💾 {n_points} points activity_stream_points enregistrés.")
        except Exception as e:
            print("⚠️ Erreur save_activity_stream_points :", e)

        # 5b) Calcul des pics VAM (optionnel)
        try:
            compute_and_store_vam_peaks(db=db, activity=activity_obj, user_id=user_id)
        except Exception as e:
            print("⚠️ Erreur compute_and_store_vam_peaks :", e)

        # 5c) Calcul des agrégats zone × pente
        try:
            compute_and_store_zone_slope_aggs(db=db, activity=activity_obj, user_id=user_id)
        except Exception as e:
            print("⚠️ Erreur compute_and_store_zone_slope_aggs :", e)

        try:
            update_runner_profile_monthly_from_activity(
                db=db,
                activity=activity_obj,
                stats=stats,
            )
        except Exception as e:
            print("⚠️ Erreur update_runner_profile_monthly_from_activity :", e)

        # 6) Sections dynamiques selon le type d'activité
        # -----------------------------------------------------------------
        settings = db.query(UserSettings).filter(UserSettings.user_id == user_id).one_or_none()
        if settings is None:
            class _S:
                desc_enable_auto_block = True
            settings = _S()

        auto_block_enabled = getattr(settings, "desc_enable_auto_block", True)
        full_block = ""

        if auto_block_enabled:
            time_stream_full = streams.get("time", {}).get("data") or []
            hr_stream_full = streams.get("heartrate", {}).get("data") or []
            altitude_stream = streams.get("altitude", {}).get("data") or []
            distance_stream = streams.get("distance", {}).get("data") or []
            cadence_stream = streams.get("cadence", {}).get("data") or []
            longest_climb = _compute_longest_climb_summary(
                time_stream_full,
                distance_stream,
                altitude_stream,
            )
            descent_summaries = _compute_descent_summaries(
                time_stream_full,
                distance_stream,
                altitude_stream,
            )
            km_highlights = compute_km_highlights_from_streams(
                time_stream_full,
                distance_stream,
                altitude_stream,
            )

            best_gain_windows = _compute_best_gain_windows(time_stream_full, altitude_stream, [60, 300, 600, 900, 1800, 3600])
            best_drop_windows = _compute_best_drop_windows(time_stream_full, altitude_stream, [600, 900, 1800, 3600])
            cadence_buckets = _compute_cadence_buckets(time_stream_full, cadence_stream)

            def _last_valid_value(seq):
                if not seq:
                    return None
                for val in reversed(seq):
                    if val is None:
                        continue
                    try:
                        return float(val)
                    except (TypeError, ValueError):
                        continue
                return None

            total_distance_m = activity_obj.distance or act.get("distance") or _last_valid_value(distance_stream)
            moving_time_sec = act.get("moving_time") or activity_obj.elapsed_time or _last_valid_value(time_stream_full)
            total_gain_m = activity_obj.total_elevation_gain or act.get("total_elevation_gain")

            overall_pace_sec = None
            avg_speed_kmh = None
            if total_distance_m and moving_time_sec and float(total_distance_m) > 0:
                overall_pace_sec = (float(moving_time_sec) / float(total_distance_m)) * 1000.0
                if moving_time_sec and float(moving_time_sec) > 0:
                    avg_speed_kmh = (float(total_distance_m) / float(moving_time_sec)) * 3.6
            overall_pace_str = _format_pace(overall_pace_sec)

            sport_norm = (activity_obj.sport or "").lower()
            raw_activity_type = (activity_obj.activity_type or act.get("type") or "").strip().lower()
            strength_sport_aliases = {
                "crossfit",
                "muscu",
                "musculation",
                "strengthtraining",
                "strength_training",
                "weighttraining",
                "weightsession",
            }
            has_significant_climb = False
            if total_distance_m and total_gain_m and float(total_distance_m) > 0:
                has_significant_climb = (float(total_gain_m) / float(total_distance_m)) >= 0.02

            def _format_speed_kmh(speed_kmh: float | None) -> str | None:
                if speed_kmh is None or speed_kmh <= 0:
                    return None
                return f"{speed_kmh:.1f} km/h"

            blocks_ordered = []
            if stats and stats.get("block"):
                blocks_ordered.append(stats["block"])

            best_pace_windows = {}
            pace_windows_needed: list[int] = []
            if sport_norm == "run":
                pace_windows_needed = [15, 60, 300, 600]
            elif sport_norm == "ride":
                pace_windows_needed = [900, 1800, 3600]
            elif sport_norm in {"ski_alpine", "ski_nordic", "ski_rando"}:
                pace_windows_needed = [60, 300]
            if pace_windows_needed:
                max_speed_cap = _max_speed_cap_for_sport(sport_norm)
                best_pace_windows = _compute_best_pace_windows(
                    time_stream_full,
                    distance_stream,
                    pace_windows_needed,
                    max_speed_mps=max_speed_cap,
                )

            if sport_norm == "run":
                run_lines = []
                gain_labels = {60: "1′", 300: "5′", 600: "10′"}
                climb_line = None
                if longest_climb:
                    climb_parts = _build_longest_climb_parts(longest_climb, sport_norm)
                    if len(climb_parts) > 1:
                        climb_line = "⛰️ Montée la plus longue : " + " | ".join(climb_parts)

                if has_significant_climb:
                    dplus_parts = []
                    for window in [60, 300, 600]:
                        data = best_gain_windows.get(window)
                        if not data:
                            continue
                        gain = round(data.get("gain_m", 0.0))
                        vam = round(data.get("vam_m_per_h", 0.0))
                        if gain <= 0 or vam <= 0:
                            continue
                        dplus_parts.append(f"{gain_labels[window]} : +{gain} m ({vam} m/h)")
                    if dplus_parts:
                        run_lines.append("⛰️ D+ max : " + " | ".join(dplus_parts))

                pace_labels = {15: "15s", 60: "1′", 300: "5′", 600: "10′"}
                pace_parts = []
                for window in [15, 60, 300, 600]:
                    data = best_pace_windows.get(window)
                    if not data:
                        continue
                    pace_str = _format_pace(data.get("pace_sec_per_km"))
                    if pace_str:
                        pace_parts.append(f"{pace_labels[window]} : {pace_str}")
                if pace_parts:
                    run_lines.append("⚡ Allures max : " + " | ".join(pace_parts))

                if climb_line:
                    run_lines.append(climb_line)

                if run_lines:
                    blocks_ordered.append("\n".join(run_lines))

            elif sport_norm in {"ski_alpine", "ski_nordic", "ski_rando"}:
                ski_lines = []
                climb_line = None
                if sport_norm in {"ski_nordic", "ski_rando"} and total_gain_m and float(total_gain_m) > 0:
                    ski_lines.append(f"⛰️ D+ total : {round(float(total_gain_m))} m")

                if sport_norm == "ski_alpine":
                    longest_descent = descent_summaries.get("longest_descent")
                    descent_count = int(descent_summaries.get("count") or 0)
                    if descent_count > 0:
                        ski_lines.append(f"🎿 Descentes détectées : {descent_count}")
                    if longest_descent:
                        descent_parts = [f"{(longest_descent.get('distance_m', 0.0) / 1000.0):.1f} km"]
                        net_vertical = round(longest_descent.get("net_vertical_m", 0.0))
                        if net_vertical > 0:
                            descent_parts.append(f"D- {net_vertical} m")
                        speed_str = _format_speed_kmh(longest_descent.get("avg_speed_kmh"))
                        if speed_str:
                            descent_parts.append(speed_str)
                        if len(descent_parts) > 1:
                            ski_lines.append("📏 Descente la plus longue : " + " | ".join(descent_parts))
                elif longest_climb:
                    climb_parts = _build_longest_climb_parts(longest_climb, sport_norm)
                    if len(climb_parts) > 1:
                        climb_line = "⛰️ Montée la plus longue : " + " | ".join(climb_parts)

                dminus_labels = {600: "10′", 1800: "30′", 3600: "1 h"}
                dminus_parts = []
                for window, label in dminus_labels.items():
                    data = best_drop_windows.get(window)
                    if not data:
                        continue
                    drop = round(data.get("drop_m", 0.0))
                    if drop <= 0:
                        continue
                    dminus_parts.append(f"{label} : -{drop} m")
                if dminus_parts:
                    ski_lines.append("🎿 D- max : " + " | ".join(dminus_parts))

                vam_labels = {300: "5′", 900: "15′", 1800: "30′"}
                vam_parts = []
                for window, label in vam_labels.items():
                    data = best_gain_windows.get(window)
                    if not data:
                        continue
                    vam = round(data.get("vam_m_per_h", 0.0))
                    if vam <= 0:
                        continue
                    vam_parts.append(f"{label} : {vam} m/h")
                if vam_parts:
                    ski_lines.append("⛰️ VAM max : " + " | ".join(vam_parts))

                speed_labels = {60: "1′", 300: "5′"}
                speed_parts = []
                for window, label in speed_labels.items():
                    data = best_pace_windows.get(window)
                    if not data:
                        continue
                    pace_sec = data.get("pace_sec_per_km")
                    if pace_sec and pace_sec > 0:
                        speed_kmh = 3600.0 / pace_sec
                        speed_parts.append(f"{label} : {speed_kmh:.1f} km/h")
                if speed_parts:
                    ski_lines.append("⚡ Vitesse moy : " + " | ".join(speed_parts))

                if climb_line:
                    ski_lines.append(climb_line)
                if ski_lines:
                    blocks_ordered.append("\n".join(ski_lines))

            elif sport_norm == "ride":
                ride_lines = []
                dplus_labels = {300: "5′", 900: "15′", 1800: "30′", 3600: "1 h"}
                ride_icon = "🚵" if raw_activity_type in {"mountainbike", "mtb"} else "🚴"
                climb_line = None
                if longest_climb:
                    climb_parts = _build_longest_climb_parts(longest_climb, sport_norm)
                    if len(climb_parts) > 1:
                        climb_line = f"{ride_icon} Montée la plus longue : " + " | ".join(climb_parts)

                dplus_parts = []
                for window, label in dplus_labels.items():
                    data = best_gain_windows.get(window)
                    if not data:
                        continue
                    gain = round(data.get("gain_m", 0.0))
                    vam = round(data.get("vam_m_per_h", 0.0))
                    if gain <= 0 or vam <= 0:
                        continue
                    dplus_parts.append(f"{label} : +{gain} m ({vam} m/h)")
                if dplus_parts:
                    ride_lines.append("⛰️ D+ max : " + " | ".join(dplus_parts))

                speed_parts = []
                speed_labels = {900: "15′", 1800: "30′", 3600: "1 h"}
                for window in [900, 1800, 3600]:
                    data = best_pace_windows.get(window)
                    if not data:
                        continue
                    pace_sec = data.get("pace_sec_per_km")
                    if pace_sec and pace_sec > 0:
                        speed_kmh = 3600.0 / pace_sec
                        speed_parts.append(f"{speed_labels[window]} : {speed_kmh:.1f} km/h")
                if speed_parts:
                    ride_lines.append("⚡ Vitesse moy : " + " | ".join(speed_parts))

                if climb_line:
                    ride_lines.append(climb_line)

                avg_cad, max_cad = _compute_time_weighted_avg_and_max(time_stream_full, cadence_stream)
                cadence_parts = []
                if avg_cad:
                    cadence_parts.append(f"moy {round(avg_cad)} rpm")
                if max_cad:
                    cadence_parts.append(f"max {round(max_cad)} rpm")
                if cadence_parts:
                    ride_lines.append("🔁 Cadence vélo : " + " | ".join(cadence_parts))

                power_stream = streams.get("watts", {}).get("data") or []
                power_windows = _compute_avg_value_windows(time_stream_full, power_stream, [300, 900, 1800])
                if power_windows:
                    label_map = {300: "5′", 900: "15′", 1800: "30′"}
                    pw_parts = []
                    for window in [300, 900, 1800]:
                        avg_power = power_windows.get(window)
                        if avg_power is None:
                            continue
                        pw_parts.append(f"{label_map[window]} : {round(avg_power)} W")
                    if pw_parts:
                        ride_lines.append("⚡ Puissance moy : " + " | ".join(pw_parts))

                if ride_lines:
                    blocks_ordered.append("\n".join(ride_lines))

            elif sport_norm in strength_sport_aliases or raw_activity_type in strength_sport_aliases:
                user = db.query(User).get(user_id)
                fc_max = compute_user_fc_max(user)
                hr_zones = compute_hr_zones(
                    samples=samples,
                    activity_start=start_aw,
                    time_stream=time_stream_full,
                    hr_stream=hr_stream_full,
                    fc_max=fc_max,
                )
                if hr_zones:
                    zone_lines = []
                    for zone in hr_zones:
                        duration_sec = float(zone.get("duration_sec") or 0.0)
                        gly_avg = zone.get("gly_avg")
                        gly_label = f"{round(gly_avg)} mg/dL" if gly_avg is not None else "n/a"
                        zone_lines.append(
                            f"{zone['name']} : {_format_duration(duration_sec)} | Gly moy : {gly_label}"
                        )
                    if zone_lines:
                        blocks_ordered.append("\n".join(zone_lines))

            elif stats and stats.get("block"):
                # Autres sports : on ne garde que la glycémie si disponible
                pass

            if blocks_ordered:
                app_base_url = _get_app_base_url()
                activity_link = (
                    f"{app_base_url}/ui/user/{user_id}/activity/{activity_obj.id}"
                    if activity_obj.id is not None
                    else None
                )
                if activity_link:
                    blocks_ordered.append(f"Voir l'analyse complète : {activity_link}")
                blocks_ordered.append(
                    f"Pour tous les fans de data —> Join us : {app_base_url}/"
                )
                full_block = normalize_summary_block_layout("\n".join(blocks_ordered))

        # 7) Mise à jour Strava + persistance du block (optionnel)
        if auto_block_enabled and full_block and update_strava_description and cli is not None and activity_id is not None:
            new_desc = merge_desc(act.get("description") or "", full_block)
            await cli.update_activity_description(activity_id, new_desc)
            strava_description_updated = True
        elif update_strava_description and activity_id is not None:
            print(
                f"[STRAVA] activity_id={activity_id} user_id={user_id} -> aucun export description "
                f"(auto_block={auto_block_enabled}, full_block={'yes' if full_block else 'no'})."
            )

        activity_obj.glucose_summary_block = full_block or None
        db.add(activity_obj)
        db.commit()

        purged_count = purge_old_user_activities(db, user_id=user_id)
        if purged_count:
            logger.info("[RETENTION] %s activités live purgées pour user_id=%s", purged_count, user_id)

        final_glucose_coverage = glucose_graph_has_activity_coverage(
            graph_db,
            start_aw,
            end_aw,
            max_delta_sec=CGM_MATCH_MAX_DELTA_SEC,
        )

        return {
            "status": "ok",
            "activity_id": activity_id,
            "stored_activity_id": activity_obj.id,
            "user_id": user_id,
            "needs_live_fetch": needs_live_fetch,
            "live_fetch_reason": live_fetch_reason,
            "live_fetch_source": live_fetch_source,
            "live_fetch_points_count": live_fetch_points_count,
            "has_glucose_coverage": final_glucose_coverage,
            "strava_description_updated": strava_description_updated,
            "full_block_built": bool(full_block),
            "trigger_source": trigger_source,
        }

    finally:
        db.close()


async def _perform_strava_activity_enrichment(
    activity_id: int,
    *,
    user_id: int,
    trigger_source: str,
) -> dict:
    cli = StravaClient(user_id=user_id)

    try:
        act = await cli.get_activity(activity_id)
        streams = await cli.get_streams(activity_id)
    except Exception as exc:
        return {
            "status": "deferred",
            "activity_id": activity_id,
            "user_id": user_id,
            "reason": "strava_fetch_error",
            "error": str(exc),
            "retryable": True,
            "trigger_source": trigger_source,
        }

    try:
        result = await process_activity_core(
            act=act,
            streams=streams,
            user_id=user_id,
            activity_id=activity_id,
            cli=cli,
            update_strava_description=True,
            trigger_source=trigger_source,
        )
    except Exception as exc:
        return {
            "status": "deferred",
            "activity_id": activity_id,
            "user_id": user_id,
            "reason": "strava_update_error",
            "error": str(exc),
            "retryable": True,
            "trigger_source": trigger_source,
        }

    live_fetch_reason = result.get("live_fetch_reason")
    full_block_built = bool(result.get("full_block_built"))
    retryable = _is_retryable_enrichment_reason(live_fetch_reason)

    if full_block_built and result.get("strava_description_updated"):
        result["status"] = "succeeded"
        result["retryable"] = False
        result["reason"] = None
        return result

    if full_block_built:
        result["status"] = "completed_without_update"
        result["retryable"] = False
        result["reason"] = "strava_update_skipped"
        return result

    if retryable:
        result["status"] = "deferred"
        result["retryable"] = True
        result["reason"] = live_fetch_reason
        return result

    result["status"] = "completed_without_update"
    result["retryable"] = False
    result["reason"] = live_fetch_reason or "no_glucose_coverage"
    return result


async def _process_enrichment_job(job_id: int, *, trigger_source: str | None = None) -> dict:
    db = SessionLocal()
    job = db.query(ActivityEnrichmentJob).get(job_id)
    if job is None:
        db.close()
        return {"status": "missing", "reason": "job_not_found", "retryable": False}

    user_id = int(job.user_id)
    activity_id = int(job.strava_activity_id)
    effective_trigger = (trigger_source or job.trigger_source or "job")[:32]

    now = dt.datetime.utcnow()
    if job.next_retry_at and job.next_retry_at > now and job.status in {"pending", "retry"}:
        retry_at = job.next_retry_at
        result = _attach_enrichment_job_snapshot({
            "status": "deferred",
            "reason": "retry_not_due",
            "retryable": True,
            "retry_at": retry_at,
        }, job)
        db.close()
        return result

    if not _acquire_activity_enrichment_lock(user_id, activity_id):
        _schedule_enrichment_retry(
            db,
            job=job,
            reason="activity_busy",
            trigger_source=effective_trigger,
        )
        db.commit()
        retry_at = job.next_retry_at
        result = _attach_enrichment_job_snapshot({
            "status": "deferred",
            "reason": "activity_busy",
            "retryable": True,
            "retry_at": retry_at,
        }, job)
        db.close()
        return result

    try:
        job.status = "processing"
        job.trigger_source = effective_trigger
        job.attempts = int(job.attempts or 0) + 1
        job.locked_at = now
        job.started_at = now
        job.completed_at = None
        job.next_retry_at = None
        db.commit()
        db.close()

        result = await _perform_strava_activity_enrichment(
            activity_id,
            user_id=user_id,
            trigger_source=effective_trigger,
        )

        db = SessionLocal()
        job = db.query(ActivityEnrichmentJob).get(job_id)
        if job is None:
            return result

        if result.get("status") == "succeeded":
            _mark_enrichment_job_success(job, trigger_source=effective_trigger)
        elif result.get("retryable"):
            _schedule_enrichment_retry(
                db,
                job=job,
                reason=result.get("reason") or "unknown_retryable_error",
                last_error=result.get("error"),
                trigger_source=effective_trigger,
            )
        else:
            _mark_enrichment_job_failed(
                job,
                reason=result.get("reason") or "enrichment_failed",
                last_error=result.get("error"),
                trigger_source=effective_trigger,
            )
        db.commit()
        db.refresh(job)
        return _attach_enrichment_job_snapshot(result, job)
    finally:
        _release_activity_enrichment_lock(user_id, activity_id)
        try:
            db.close()
        except Exception:
            pass


async def request_activity_enrichment(
    activity_id: int,
    *,
    user_id: int,
    trigger_source: str,
    immediate: bool = True,
) -> dict:
    db = SessionLocal()
    try:
        job = _get_or_create_enrichment_job(db, user_id, activity_id)
        job.status = "pending"
        job.trigger_source = (trigger_source or "manual")[:32]
        job.next_retry_at = dt.datetime.utcnow()
        job.last_reason = None
        job.last_error = None
        job.completed_at = None
        db.commit()
        job_id = int(job.id)
    finally:
        db.close()

    if not immediate:
        return {"status": "queued", "job_id": job_id}

    return await _process_enrichment_job(job_id, trigger_source=trigger_source)


async def process_pending_enrichment_jobs_once(limit: int = 10) -> int:
    db = SessionLocal()
    try:
        now = dt.datetime.utcnow()
        jobs = (
            db.query(ActivityEnrichmentJob)
            .filter(
                ActivityEnrichmentJob.status.in_(("pending", "retry")),
                (
                    (ActivityEnrichmentJob.next_retry_at == None)
                    | (ActivityEnrichmentJob.next_retry_at <= now)
                ),
            )
            .order_by(
                ActivityEnrichmentJob.next_retry_at.is_(None).desc(),
                ActivityEnrichmentJob.next_retry_at.asc(),
                ActivityEnrichmentJob.updated_at.asc(),
            )
            .limit(limit)
            .all()
        )
        job_ids = [int(job.id) for job in jobs]
    finally:
        db.close()

    processed = 0
    for job_id in job_ids:
        await _process_enrichment_job(job_id, trigger_source="retry_worker")
        processed += 1
    return processed


def run_enrichment_retry_loop():
    logger.info(
        "[ENRICHMENT] Thread de reprise lancé (scan toutes les %ss).",
        ENRICHMENT_WORKER_POLL_SECONDS,
    )
    while True:
        try:
            processed = asyncio.run(process_pending_enrichment_jobs_once())
            if processed:
                logger.info("[ENRICHMENT] %s job(s) d'enrichissement traité(s) par le worker.", processed)
        except Exception:
            logger.exception("Erreur dans la boucle de reprise des enrichissements.")
        time.sleep(ENRICHMENT_WORKER_POLL_SECONDS)



# -----------------------------------------------------------------------------
# Orchestrateur : enrichir une activité Strava (wrapper autour du core)
# -----------------------------------------------------------------------------
async def enrich_activity(activity_id: int, user_id: int = 1):
    return await _perform_strava_activity_enrichment(
        activity_id,
        user_id=user_id,
        trigger_source="direct",
    )

# -----------------------------------------------------------------------------
# Outils pour lire un GPX et le transformer en act + streams façon Strava
# -----------------------------------------------------------------------------
def _haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def parse_gpx_to_act_and_streams(filepath: str, user_id: int = 1) -> tuple[dict, dict]:
    tree = ET.parse(filepath)
    root = tree.getroot()

    ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}

    lats, lons, eles, times, hrs, cads = [], [], [], [], [], []

    for trkpt in root.findall(".//gpx:trkpt", ns):
        lat = float(trkpt.get("lat"))
        lon = float(trkpt.get("lon"))
        ele_node = trkpt.find("gpx:ele", ns)
        ele = float(ele_node.text) if ele_node is not None else None
        time_node = trkpt.find("gpx:time", ns)

        if time_node is None:
            raise ValueError("GPX sans timestamps <time> : impossible de calculer le temps / allure.")

        ts = dt.datetime.fromisoformat(time_node.text.replace("Z", "+00:00"))

        # Extensions : HR + cadence si dispo
        hr_val = None
        cad_val = None
        ext = trkpt.find("gpx:extensions", ns)
        if ext is not None:
            for e in ext.iter():
                tag = e.tag.lower()
                if tag.endswith("hr"):
                    try:
                        hr_val = float(e.text)
                    except Exception:
                        pass
                if tag.endswith("cad"):
                    try:
                        cad_val = float(e.text)
                    except Exception:
                        pass

        lats.append(lat)
        lons.append(lon)
        eles.append(ele)
        times.append(ts)
        hrs.append(hr_val)
        cads.append(cad_val)

    if not times:
        raise ValueError("GPX vide ou sans points valides.")

    # ---------- Time stream ----------
    start_ts = times[0]
    time_stream = [int((t - start_ts).total_seconds()) for t in times]
    elapsed = time_stream[-1]

    # ---------- Distance + latlng + velocity_smooth + D+ ----------
    cum_dist = 0.0
    latlng = []
    dist_stream = []
    vel_stream = []
    total_gain = 0.0

    prev_t = None
    prev_lat = None
    prev_lon = None
    prev_ele = None

    for i in range(len(lats)):
        lat = lats[i]
        lon = lons[i]
        ele = eles[i]
        t = times[i]

        if i > 0:
            dt_s = (t - prev_t).total_seconds() if prev_t is not None else 0.0
            d_m = _haversine_m(prev_lat, prev_lon, lat, lon)
            cum_dist += d_m

            if dt_s > 0:
                v = d_m / dt_s
            else:
                v = 0.0

            # D+ (on ne compte que les montées)
            if ele is not None and prev_ele is not None:
                delta_ele = ele - prev_ele
                if delta_ele > 0:
                    total_gain += delta_ele
        else:
            dt_s = 0.0
            d_m = 0.0
            v = 0.0

        latlng.append([lat, lon])
        dist_stream.append(cum_dist)
        vel_stream.append(v)

        prev_t = t
        prev_lat = lat
        prev_lon = lon
        prev_ele = ele

    # ---------- FC moyenne si dispo ----------
    hr_values = [h for h in hrs if h is not None]
    avg_hr = sum(hr_values) / len(hr_values) if hr_values else None

    # ---------- ID factice + nom lisible ----------
    # Les imports locaux utilisent un identifiant négatif, réservé aux activités
    # qui ne viennent pas de Strava. Leur rétention est calculée depuis l'import.
    fake_id = -(int(start_ts.timestamp()) * 100 + int(user_id))

    filename = os.path.basename(filepath)
    base_name = os.path.splitext(filename)[0]
    pretty_name = f"{start_ts.date()} – {base_name}"

    # Activité "façon Strava"
    act = {
        "id": fake_id,
        "name": pretty_name,
        "start_date": start_ts.replace(tzinfo=dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        "elapsed_time": int(elapsed),
        "moving_time": int(elapsed),
        "distance": float(cum_dist),
        "total_elevation_gain": float(total_gain),
        "average_heartrate": float(avg_hr) if avg_hr is not None else None,
        "type": "Run",         # comme Strava
        "sport_type": "Run",   # pour que ton code normalise le sport
        "athlete": {"id": user_id},
        "description": "",
    }

    # ---------- Streams façon Strava ----------
    streams = {
        "time": {"data": time_stream},
        "latlng": {"data": latlng},
        "altitude": {"data": eles},
        "distance": {"data": dist_stream},
        "velocity_smooth": {"data": vel_stream},
    }

    if hr_values:
        streams["heartrate"] = {"data": [h if h is not None else None for h in hrs]}

    if any(c is not None for c in cads):
        streams["cadence"] = {"data": [c if c is not None else None for c in cads]}

    return act, streams





# -----------------------------------------------------------------------------
# Orchestrateur : enrichir une activité à partir d'un fichier GPX
# -----------------------------------------------------------------------------
async def enrich_activity_from_gpx(filepath: str, user_id: int = 1):
    act, streams = parse_gpx_to_act_and_streams(filepath, user_id=user_id)

    return await process_activity_core(
        act=act,
        streams=streams,
        user_id=user_id,
        activity_id=None,                  # pas d'ID Strava
        cli=None,                          # pas de client Strava
        update_strava_description=False,   # on ne met pas à jour une activité Strava
        fetch_glucose=False,               # import historique : profil sportif uniquement
    )


def _fit_record_value(record, field_name: str):
    """Lit un champ FIT, quel que soit le format renvoyé par fitparse."""
    value = record.get_value(field_name)
    return value.value if hasattr(value, "value") else value


def parse_fit_to_act_and_streams(filepath: str, user_id: int = 1) -> tuple[dict, dict]:
    """Transforme un export FIT en activité et streams compatibles Strava."""
    if FitFile is None:
        raise ValueError("La lecture des fichiers FIT n'est pas disponible sur ce serveur.")

    fit = FitFile(filepath)
    session = {}
    points = []
    for message in fit.get_messages():
        if message.name == "session" and not session:
            session = {field.name: field.value for field in message}
        elif message.name == "record":
            timestamp = _fit_record_value(message, "timestamp")
            if isinstance(timestamp, dt.datetime):
                points.append({
                    "timestamp": timestamp,
                    "lat": _fit_record_value(message, "position_lat"),
                    "lon": _fit_record_value(message, "position_long"),
                    "altitude": _fit_record_value(message, "enhanced_altitude") or _fit_record_value(message, "altitude"),
                    "distance": _fit_record_value(message, "distance"),
                    "heartrate": _fit_record_value(message, "heart_rate"),
                    "cadence": _fit_record_value(message, "cadence"),
                    "speed": _fit_record_value(message, "enhanced_speed") or _fit_record_value(message, "speed"),
                })

    if not points:
        raise ValueError("Fichier FIT vide ou sans enregistrements horodatés.")

    points.sort(key=lambda point: point["timestamp"])
    start_ts = points[0]["timestamp"]
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=dt.timezone.utc)
    else:
        start_ts = start_ts.astimezone(dt.timezone.utc)

    time_stream, latlng, altitude, distance, heartrate, cadence, speed = [], [], [], [], [], [], []
    cumulative_distance = 0.0
    total_gain = 0.0
    previous_altitude = None
    previous_latlng = None
    for point in points:
        timestamp = point["timestamp"]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=dt.timezone.utc)
        else:
            timestamp = timestamp.astimezone(dt.timezone.utc)
        time_stream.append(max(0, int((timestamp - start_ts).total_seconds())))

        lat = point["lat"]
        lon = point["lon"]
        current_latlng = None
        if lat is not None and lon is not None:
            # Les coordonnées FIT sont exprimées en semicircles.
            current_latlng = [float(lat) * 180.0 / (2 ** 31), float(lon) * 180.0 / (2 ** 31)]
        latlng.append(current_latlng)

        point_distance = point["distance"]
        if point_distance is not None:
            cumulative_distance = float(point_distance)
        elif previous_latlng and current_latlng:
            cumulative_distance += _haversine_m(*previous_latlng, *current_latlng)
        distance.append(cumulative_distance)
        previous_latlng = current_latlng or previous_latlng

        current_altitude = float(point["altitude"]) if point["altitude"] is not None else None
        altitude.append(current_altitude)
        if current_altitude is not None and previous_altitude is not None:
            total_gain += max(0.0, current_altitude - previous_altitude)
        if current_altitude is not None:
            previous_altitude = current_altitude

        heartrate.append(float(point["heartrate"]) if point["heartrate"] is not None else None)
        cadence.append(float(point["cadence"]) if point["cadence"] is not None else None)
        speed.append(float(point["speed"]) if point["speed"] is not None else None)

    elapsed = int(session.get("total_elapsed_time") or time_stream[-1])
    average_heartrate = session.get("avg_heart_rate")
    if average_heartrate is None:
        values = [value for value in heartrate if value is not None]
        average_heartrate = sum(values) / len(values) if values else None
    with open(filepath, "rb") as fit_source:
        imported_id = -int(hashlib.sha256(fit_source.read()).hexdigest()[:15], 16)
    base_name = os.path.splitext(os.path.basename(filepath))[0]
    sport = session.get("sport") or "running"

    act = {
        "id": imported_id,
        "name": f"{start_ts.date()} – {base_name}",
        "start_date": start_ts.isoformat().replace("+00:00", "Z"),
        "elapsed_time": elapsed,
        "moving_time": int(session.get("total_timer_time") or elapsed),
        "distance": float(session.get("total_distance") or cumulative_distance),
        "total_elevation_gain": float(session.get("total_ascent") or total_gain),
        "average_heartrate": float(average_heartrate) if average_heartrate is not None else None,
        "type": str(sport),
        "sport_type": str(sport),
        "athlete": {"id": user_id},
        "description": "",
    }
    streams = {
        "time": {"data": time_stream},
        "latlng": {"data": latlng},
        "altitude": {"data": altitude},
        "distance": {"data": distance},
        "velocity_smooth": {"data": speed},
    }
    if any(value is not None for value in heartrate):
        streams["heartrate"] = {"data": heartrate}
    if any(value is not None for value in cadence):
        streams["cadence"] = {"data": cadence}
    return act, streams


async def enrich_activity_from_fit(filepath: str, user_id: int = 1):
    act, streams = parse_fit_to_act_and_streams(filepath, user_id=user_id)
    return await process_activity_core(
        act=act,
        streams=streams,
        user_id=user_id,
        activity_id=None,
        cli=None,
        update_strava_description=False,
        fetch_glucose=False,
    )


# -----------------------------------------------------------------------------
# Routes DEBUG
# -----------------------------------------------------------------------------

@app.get("/debug/last-activity")
async def debug_last_activity(
    user_id: int = Query(2, ge=1),
    page_size: int = Query(1, ge=1, le=10),
):
    """
    Retourne les dernières activités visibles par le token de l'utilisateur donné.
    Par défaut user_id=2 (puisque c'est celui qu'on a lié à Strava chez toi).
    """
    cli = StravaClient(user_id=user_id)
    acts = await cli.list_activities(per_page=page_size)
    return [
        {"id": a["id"], "name": a.get("name"), "start_date": a.get("start_date")}
        for a in acts
    ]

#-------------------------------------------------------------------------------
# Route DEBUG : enrichir une activité Strava via LibreLinkUp (Node.js)
#-------------------------------------------------------------------------------
@app.get("/debug/enrich/{activity_id}")
async def debug_enrich(activity_id: int):
    """
    Version de debug : lit LibreLinkUp via Node.js, calcule un résumé simple
    et met à jour la description Strava.
    """
    cli = StravaClient(user_id=1)

    # 1️⃣ Lecture de l’activité Strava
    act = await cli.get_activity(activity_id)
    start_time = datetime.fromisoformat(act["start_date"].replace("Z", "+00:00"))
    elapsed_sec = act.get("elapsed_time", 0)
    end_time = start_time + timedelta(seconds=elapsed_sec)

    # 2️⃣ Lecture LibreLinkUp via le point d'entrée protégé : pas d'appel
    # simultané ni de contournement du cooldown Cloudflare.
    try:
        glucose_data, libre_reason = fetch_libre_points_guarded(
            user_id=1,
            context="debug_enrich",
        )
    except Exception as e:
        return {"error": f"Erreur lecture LibreLinkUp : {e}"}

    if libre_reason:
        return {
            "status": "libre_unavailable",
            "reason": libre_reason,
            "activity_id": activity_id,
        }

    # 3️⃣ Filtrage des points pendant la durée de l’activité (+/- 5 min)
    points = []
    for p in glucose_data:
        ts = p["ts"]
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        if start_time - timedelta(minutes=5) <= ts <= end_time + timedelta(minutes=5):
            points.append(p)

    if not points:
        return {
            "status": "no_glucose_data",
            "message": "Aucune donnée glycémie trouvée dans la fenêtre temporelle.",
            "activity_id": activity_id,
        }

    # 4️⃣ Calcul de stats simples
    values = [p["mgdl"] for p in points if p.get("mgdl") is not None]
    if not values:
        return {"status": "no_valid_values", "activity_id": activity_id}

    moy = sum(values) / len(values)
    mini = min(values)
    maxi = max(values)
    pct_in_range = sum(1 for v in values if 70 <= v <= 180) / len(values) * 100
    hypos = sum(1 for v in values if v < 70)
    hypers = sum(1 for v in values if v > 180)

    # 5️⃣ Visuel simple
    blocs_vert = int(pct_in_range // 10)
    blocs_total = 10
    visuel = "🟩" * blocs_vert + "⬜" * (blocs_total - blocs_vert)

    resume = (
        f"🔬 Glycémie : Moy {moy:.0f} mg/dL | 70–180 : {pct_in_range:.0f}%\n"
        f"{visuel}\n"
        f"Min : {mini:.0f} | Max : {maxi:.0f}\n"
        f"Hypos : {hypos} | Hypers : {hypers}\n"
        f"— résumé auto"
    )

    # 6️⃣ Mise à jour Strava
    try:
        await cli.update_activity_description(activity_id, resume)
    except Exception as e:
        return {"status": "strava_error", "error": str(e), "resume": resume}

    print(f"✅ Activité {activity_id} enrichie et mise à jour sur Strava.")

    return {
        "status": "ok",
        "activity_id": activity_id,
        "resume": resume,
        "points_count": len(points),
    }


@app.get("/debug/db-activities/{user_id}")
def debug_db_activities(
    user_id: int,
    limit: int = Query(10, ge=1, le=100),
):
    """
    Liste les dernières activités enregistrées en base pour un user donné.
    Utile pour vérifier que l'enregistrement fonctionne bien.
    """
    db = SessionLocal()
    try:
        qs = (
            db.query(Activity)
            .filter(Activity.user_id == user_id)
            .order_by(Activity.start_date.desc())
            .limit(limit)
        )
        activities = qs.all()

        return [
            {
                "id": a.id,
                "strava_activity_id": a.strava_activity_id,
                "start_date": a.start_date.isoformat() if a.start_date else None,
                "elapsed_time": a.elapsed_time,
                "average_heartrate": a.average_heartrate,
                "avg_glucose": a.avg_glucose,
                "min_glucose": a.min_glucose,
                "max_glucose": a.max_glucose,
                "time_in_range_percent": a.time_in_range_percent,
                "hypo_count": a.hypo_count,
                "hyper_count": a.hyper_count,
                "last_synced_at": a.last_synced_at.isoformat() if a.last_synced_at else None,
            }
            for a in activities
        ]
    finally:
        db.close()

#-----------------------------------------------------------------------------
# --- Helper : lecture glycémie depuis la base ---
#-----------------------------------------------------------------------------
CGM_MATCH_MAX_DELTA_SEC = int(os.getenv("CGM_MATCH_MAX_DELTA_SEC", "900") or "900")
CGM_MATCH_MARGIN_MIN = max(10, math.ceil(CGM_MATCH_MAX_DELTA_SEC / 60))


def load_glucose_graph_from_db(db, user_id: int, start: dt.datetime, end: dt.datetime, margin_min: int = 10):
    """
    Lit les points de glycémie autour d'une activité.
    Renvoie des dicts avec ts **UTC AWARE** (tzinfo=UTC) pour éviter tout mélange.
    """
    def to_utc_aware(d: dt.datetime | None) -> dt.datetime | None:
        if d is None:
            return None
        if d.tzinfo is None:
            return d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)

    pad = dt.timedelta(minutes=margin_min)

    # Tolérance : si start/end sont naïfs, on les rend AWARE UTC avant de construire le filtre
    start_f = to_utc_aware(start)
    end_f   = to_utc_aware(end)

    rows = (
        db.query(GlucosePoint)
        .filter(
            GlucosePoint.user_id == user_id,
            GlucosePoint.ts >= (start_f - pad),
            GlucosePoint.ts <= (end_f + pad),
        )
        .order_by(GlucosePoint.ts.asc())
        .all()
    )

    out = []
    for r in rows:
        if r.mgdl is None or r.ts is None:
            continue
        ts_aw = to_utc_aware(r.ts)
        out.append({
            "ts": ts_aw,                 # <- AWARE UTC
            "mgdl": float(r.mgdl),
            "trend": r.trend,
            "source": r.source,
        })
    return out


def glucose_graph_has_activity_coverage(
    graph: list,
    start: dt.datetime,
    end: dt.datetime,
    *,
    max_delta_sec: int = CGM_MATCH_MAX_DELTA_SEC,
    min_points: int = 2,
) -> bool:
    if not graph or not start or not end:
        return False

    valid = []
    for point in graph:
        ts = point.get("ts")
        mgdl = point.get("mgdl")
        if ts is None or mgdl is None:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.timezone.utc)
        else:
            ts = ts.astimezone(dt.timezone.utc)
        valid.append(ts)

    if len(valid) < min_points:
        return False

    start_utc = start if start.tzinfo is not None else start.replace(tzinfo=dt.timezone.utc)
    end_utc = end if end.tzinfo is not None else end.replace(tzinfo=dt.timezone.utc)
    window_pad = dt.timedelta(seconds=max_delta_sec)
    in_window = [ts for ts in sorted(valid) if start_utc - window_pad <= ts <= end_utc + window_pad]
    if len(in_window) < min_points:
        return False

    start_gap = min(abs((ts - start_utc).total_seconds()) for ts in in_window)
    end_gap = min(abs((ts - end_utc).total_seconds()) for ts in in_window)
    return start_gap <= max_delta_sec and end_gap <= max_delta_sec

# -----------------------------------------------------------------------------
# UI : Enregistrer les identifiants LibreLinkUp pour un utilisateur
# -----------------------------------------------------------------------------
@app.post("/ui/user/{user_id}/libre/credentials", response_class=HTMLResponse)
def ui_set_libre_credentials(
    request: Request,
    user_id: int,
    email: str = Form(...),
    password: str = Form(...),
    region: str = Form("fr"),
):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    test_status = "error"
    test_msg = ""
    final_status = "error"
    final_msg = ""

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        existing_cred = (
            db.query(LibreCredentials)
            .filter(LibreCredentials.user_id == user_id)
            .first()
        )

        client_version = (
            existing_cred.client_version if existing_cred and existing_cred.client_version
            else os.getenv("LIBRE_CLIENT_VERSION", "4.16.0")
        )

        try:
            test_status, test_msg = test_libre_credentials_guarded(
                email=email,
                password=password,
                region=region or "fr",
                client_version=client_version,
                user_id=user_id,
                context="credentials_test",
            )
        except Exception as e:
            test_status = "error"
            test_msg = f"Erreur de vérification LibreLinkUp : {e}"

        auth_error = is_libre_auth_error_message(test_msg, level=test_status)
        should_persist_credentials = test_status != "error" or not auth_error

        if should_persist_credentials:
            if existing_cred:
                cred = existing_cred
                cred.email = email
                cred.password_encrypted = encrypt_secret(password)
                cred.region = region
                cred.client_version = client_version
            else:
                cred = LibreCredentials(
                    user_id=user_id,
                    email=email,
                    password_encrypted=encrypt_secret(password),
                    region=region,
                    client_version=client_version,
                )
                db.add(cred)
            if get_active_glucose_source(user) is None:
                set_active_glucose_source(user, "abbott")
            if test_status == "error":
                clear_libre_disabled_state(user_id)
            db.commit()
            db.refresh(cred)

            if test_status == "error":
                final_status = "warn"
                final_msg = (
                    f"{test_msg} Les nouveaux identifiants ont ete enregistres et seront reessayes automatiquement."
                )
                set_libre_status_flag(user_id, final_status, final_msg)
            else:
                final_status = test_status
                final_msg = test_msg
        else:
            final_status = test_status
            final_msg = test_msg

    finally:
        db.close()

    status = final_status or test_status or "error"
    message = final_msg or test_msg
    params = f"?libre_status={status}"
    if message:
        params += f"&libre_msg={quote_plus(message)}"

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile{params}#libre",
        status_code=302,
    )


@app.post("/ui/user/{user_id}/libre/test")
def ui_test_libre_credentials(request: Request, user_id: int):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            msg = quote_plus("Utilisateur introuvable.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?libre_status=error&libre_msg={msg}#libre",
                status_code=303,
            )
        result = test_provider_connection(user, "abbott")
    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile?libre_status={result.status}&libre_msg={quote_plus(result.message)}#libre",
        status_code=303,
    )




# -----------------------------------------------------------------------------
# MINI INTERFACE WEB
# -----------------------------------------------------------------------------



@app.get("/ui", response_class=HTMLResponse)
def ui_home(request: Request):
    guard = _guard_admin(request)
    if guard:
        return guard

    user_page_size = _safe_positive_int(request.query_params.get("user_page_size"), 10)
    if user_page_size not in {10, 50, 100}:
        user_page_size = 10
    user_page = _safe_positive_int(request.query_params.get("user_page"), 1)
    strava_filter = _normalize_connection_filter(request.query_params.get("strava_filter"))
    libre_filter = _normalize_connection_filter(request.query_params.get("libre_filter"))
    dexcom_filter = _normalize_connection_filter(request.query_params.get("dexcom_filter"))
    carelink_filter = _normalize_connection_filter(request.query_params.get("carelink_filter"))
    nightscout_filter = _normalize_connection_filter(request.query_params.get("nightscout_filter"))
    activity_page = _safe_positive_int(request.query_params.get("activity_page"), 1)
    activity_page_size = 5
    plan_page = _safe_positive_int(request.query_params.get("plan_page"), 1)
    plan_page_size = 10

    recent_activities = []
    enrichment_dashboard = {}
    total_filtered_users = 0
    total_activity_count = 0
    user_pagination = {}
    activity_pagination = {}
    course_plan_usage = {"total": 0, "last_30_days": 0, "unique_users": 0}
    course_plan_downloads = []
    course_plan_pagination = {}
    signup_trend = []
    signup_trend_max = 1
    plan_download_trend = []
    plan_download_trend_max = 1
    login_trend = []
    login_trend_max = 1
    admin_status = request.query_params.get("admin_status")
    admin_message = request.query_params.get("admin_msg")

    db = SessionLocal()
    try:
        filtered_users = _collect_admin_user_rows(
            db,
            strava_filter=strava_filter,
            libre_filter=libre_filter,
            dexcom_filter=dexcom_filter,
            carelink_filter=carelink_filter,
            nightscout_filter=nightscout_filter,
        )
        enrichment_dashboard = _collect_enrichment_admin_rows(db)
        usage_since = dt.datetime.utcnow() - dt.timedelta(days=30)
        course_plan_usage = {
            "total": db.query(CoursePlanDownload.id).count(),
            "last_30_days": db.query(CoursePlanDownload.id).filter(CoursePlanDownload.downloaded_at >= usage_since).count(),
            "unique_users": db.query(func.count(func.distinct(CoursePlanDownload.user_id))).scalar() or 0,
        }
        course_plan_total_pages = max(1, math.ceil(course_plan_usage["total"] / plan_page_size))
        plan_page = min(plan_page, course_plan_total_pages)
        course_plan_downloads = (
            db.query(CoursePlanDownload)
            .order_by(CoursePlanDownload.downloaded_at.desc(), CoursePlanDownload.id.desc())
            .offset((plan_page - 1) * plan_page_size)
            .limit(plan_page_size)
            .all()
        )
        course_plan_pagination = {
            "page": plan_page,
            "total_pages": course_plan_total_pages,
            "has_prev": plan_page > 1,
            "has_next": plan_page < course_plan_total_pages,
            "prev_url": _build_url_with_query(request, plan_page=plan_page - 1) if plan_page > 1 else None,
            "next_url": _build_url_with_query(request, plan_page=plan_page + 1) if plan_page < course_plan_total_pages else None,
        }
        signup_start = dt.datetime.utcnow().date() - dt.timedelta(days=29)
        trend_start_at = dt.datetime.combine(signup_start, dt.time.min)

        def _daily_count_trend(rows, *, unique_users: bool = False):
            counts = {signup_start + dt.timedelta(days=offset): set() if unique_users else 0 for offset in range(30)}
            for row in rows:
                occurred_at = row[0]
                if not occurred_at or occurred_at.date() not in counts:
                    continue
                if unique_users:
                    counts[occurred_at.date()].add(row[1])
                else:
                    counts[occurred_at.date()] += 1
            return [
                {"label": day.strftime("%d/%m"), "count": len(count) if unique_users else count}
                for day, count in counts.items()
            ]

        signup_trend = _daily_count_trend(
            db.query(User.created_at).filter(User.created_at >= trend_start_at).all()
        )
        signup_trend_max = max((point["count"] for point in signup_trend), default=1) or 1
        plan_download_trend = _daily_count_trend(
            db.query(CoursePlanDownload.downloaded_at, CoursePlanDownload.user_id)
            .filter(CoursePlanDownload.downloaded_at >= trend_start_at)
            .all(),
            unique_users=True,
        )
        plan_download_trend_max = max((point["count"] for point in plan_download_trend), default=1) or 1
        login_trend = _daily_count_trend(
            db.query(UserLoginEvent.logged_at, UserLoginEvent.user_id)
            .filter(UserLoginEvent.logged_at >= trend_start_at)
            .all(),
            unique_users=True,
        )
        login_trend_max = max((point["count"] for point in login_trend), default=1) or 1

        total_filtered_users = len(filtered_users)
        user_total_pages = max(1, math.ceil(total_filtered_users / user_page_size))
        user_page = min(user_page, user_total_pages)
        user_offset = (user_page - 1) * user_page_size
        ui_users = filtered_users[user_offset:user_offset + user_page_size]
        user_has_prev = user_page > 1
        user_has_next = user_offset + user_page_size < total_filtered_users
        page_numbers = {1, 2, user_total_pages - 1, user_total_pages, user_page - 1, user_page, user_page + 1}
        page_numbers = sorted(page for page in page_numbers if 1 <= page <= user_total_pages)
        page_links = []
        previous_page_number = 0
        for page_number in page_numbers:
            if previous_page_number and page_number > previous_page_number + 1:
                page_links.append(None)
            page_links.append(
                {
                    "number": page_number,
                    "url": _build_url_with_query(request, user_page=page_number),
                    "current": page_number == user_page,
                }
            )
            previous_page_number = page_number
        user_pagination = {
            "page": user_page,
            "page_size": user_page_size,
            "total": total_filtered_users,
            "total_pages": user_total_pages,
            "start_index": user_offset + 1 if total_filtered_users else 0,
            "end_index": min(user_offset + user_page_size, total_filtered_users),
            "has_prev": user_has_prev,
            "has_next": user_has_next,
            "first_url": _build_url_with_query(request, user_page=1),
            "last_url": _build_url_with_query(request, user_page=user_total_pages),
            "prev_url": _build_url_with_query(request, user_page=user_page - 1) if user_has_prev else None,
            "next_url": _build_url_with_query(request, user_page=user_page + 1) if user_has_next else None,
            "page_links": page_links,
        }

        activity_offset = (activity_page - 1) * activity_page_size
        recent_activities_rows = (
            db.query(Activity, User)
            .join(User, Activity.user_id == User.id)
            .order_by(Activity.start_date.desc())
            .offset(activity_offset)
            .limit(activity_page_size + 1)
            .all()
        )

        visible_recent_activities = recent_activities_rows[:activity_page_size]

        recent_activities = []
        for act, owner in visible_recent_activities:
            recent_activities.append({
                "id": act.id,
                "user_id": owner.id,
                "user_email": owner.email,
                "name": act.name or f"Activité {act.id}",
                "date_str": act.start_date.strftime("%Y-%m-%d %H:%M") if act.start_date else "n/a",
                "distance_km": round((act.distance or 0) / 1000.0, 1) if act.distance else None,
                "duration_str": _format_duration(act.elapsed_time) if act.elapsed_time else None,
                "sport": act.sport or act.activity_type or "—",
                "dplus": int(round(act.total_elevation_gain)) if act.total_elevation_gain is not None else None,
                "summary_block": normalize_summary_block_layout(act.glucose_summary_block or ""),
            })

        activity_has_prev = activity_page > 1
        activity_has_next = len(recent_activities_rows) > activity_page_size
        visible_activity_total = (
            activity_offset + len(visible_recent_activities) + (1 if activity_has_next else 0)
        )
        activity_pagination = {
            "page": activity_page,
            "page_size": activity_page_size,
            "total": visible_activity_total if recent_activities else 0,
            "start_index": activity_offset + 1 if recent_activities else 0,
            "end_index": activity_offset + len(visible_recent_activities),
            "has_prev": activity_has_prev,
            "has_next": activity_has_next,
            "prev_url": _build_url_with_query(request, activity_page=activity_page - 1) if activity_has_prev else None,
            "next_url": _build_url_with_query(request, activity_page=activity_page + 1) if activity_has_next else None,
        }
    finally:
        db.close()

    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "users": ui_users,
            "recent_activities": recent_activities,
            "current_user_id": _get_session_user_id(request),
            "user_pagination": user_pagination,
            "activity_pagination": activity_pagination,
            "filters": {
                "strava": strava_filter,
                "libre": libre_filter,
                "dexcom": dexcom_filter,
                "carelink": carelink_filter,
                "nightscout": nightscout_filter,
            },
            "total_filtered_users": total_filtered_users,
            "enrichment_dashboard": enrichment_dashboard,
            "admin_status": admin_status,
            "admin_message": admin_message,
            "course_plan_usage": course_plan_usage,
            "course_plan_downloads": course_plan_downloads,
            "course_plan_pagination": course_plan_pagination,
            "signup_trend": signup_trend,
            "signup_trend_max": signup_trend_max,
            "plan_download_trend": plan_download_trend,
            "plan_download_trend_max": plan_download_trend_max,
            "login_trend": login_trend,
            "login_trend_max": login_trend_max,
        },
    )


@app.post("/admin/enrichment-jobs/run")
def admin_run_enrichment_jobs(request: Request):
    guard = _guard_admin(request)
    if guard:
        return guard

    try:
        processed = asyncio.run(process_pending_enrichment_jobs_once())
    except Exception as exc:
        return RedirectResponse(
            url="/ui?admin_status=error&admin_msg="
            + quote_plus(f"Erreur lors de l'exécution de la file: {exc}"),
            status_code=303,
        )

    return RedirectResponse(
        url="/ui?admin_status=ok&admin_msg="
        + quote_plus(f"{processed} job(s) d'enrichissement traité(s)."),
        status_code=303,
    )


@app.get("/admin/course-plan-downloads", response_class=HTMLResponse)
def admin_course_plan_downloads(request: Request):
    guard = _guard_admin(request)
    if guard:
        return guard

    page_size = 50
    page = _safe_positive_int(request.query_params.get("page"), 1)
    db = SessionLocal()
    try:
        total = db.query(CoursePlanDownload.id).count()
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        downloads = (
            db.query(CoursePlanDownload)
            .order_by(CoursePlanDownload.downloaded_at.desc(), CoursePlanDownload.id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        unique_users = db.query(func.count(func.distinct(CoursePlanDownload.user_id))).scalar() or 0
    finally:
        db.close()

    return templates.TemplateResponse(
        "admin_course_plan_downloads.html",
        {
            "request": request,
            "downloads": downloads,
            "total": total,
            "unique_users": unique_users,
            "page": page,
            "total_pages": total_pages,
        },
    )


@app.post("/admin/enrichment-jobs/{job_id}/retry")
def admin_retry_enrichment_job(request: Request, job_id: int):
    guard = _guard_admin(request)
    if guard:
        return guard

    db = SessionLocal()
    try:
        job = db.query(ActivityEnrichmentJob).filter(ActivityEnrichmentJob.id == int(job_id)).one_or_none()
        if job is None:
            return RedirectResponse(
                url="/ui?admin_status=error&admin_msg="
                + quote_plus(f"Job #{job_id} introuvable."),
                status_code=303,
            )
        job.status = "pending"
        job.attempts = 0
        job.trigger_source = "admin_retry"
        job.next_retry_at = dt.datetime.utcnow()
        job.last_reason = "manual_admin_retry"
        job.last_error = None
        job.locked_at = None
        job.started_at = None
        job.completed_at = None
        db.commit()
    finally:
        db.close()

    try:
        result = asyncio.run(_process_enrichment_job(int(job_id), trigger_source="admin_retry"))
    except Exception as exc:
        return RedirectResponse(
            url="/ui?admin_status=error&admin_msg="
            + quote_plus(f"Retry du job #{job_id} échoué: {exc}"),
            status_code=303,
        )

    status = result.get("status") or "unknown"
    reason = result.get("reason") or "ok"
    return RedirectResponse(
        url="/ui?admin_status=ok&admin_msg="
        + quote_plus(f"Job #{job_id} relancé ({status}, reason={reason})."),
        status_code=303,
    )


@app.post("/admin/users/send-email")
def admin_send_email(
    request: Request,
    subject: str = Form(""),
    body: str = Form(""),
    strava_filter: str = Form("all"),
    libre_filter: str = Form("all"),
    dexcom_filter: str = Form("all"),
    carelink_filter: str = Form("all"),
):
    guard = _guard_admin(request)
    if guard:
        return guard

    subject = (subject or "").strip()
    body = (body or "").strip()
    strava_filter = _normalize_connection_filter(strava_filter)
    libre_filter = _normalize_connection_filter(libre_filter)
    dexcom_filter = _normalize_connection_filter(dexcom_filter)
    carelink_filter = _normalize_connection_filter(carelink_filter)

    if not subject or not body:
        return RedirectResponse(
            url=(
                f"/ui?strava_filter={strava_filter}&libre_filter={libre_filter}&dexcom_filter={dexcom_filter}"
                f"&carelink_filter={carelink_filter}"
                "&admin_status=error&admin_msg="
                + quote_plus("Sujet et contenu de l'email requis.")
            ),
            status_code=303,
        )

    db = SessionLocal()
    try:
        rows = _collect_admin_user_rows(
            db,
            strava_filter=strava_filter,
            libre_filter=libre_filter,
            dexcom_filter=dexcom_filter,
            carelink_filter=carelink_filter,
        )
        recipients = [row["email"] for row in rows if row.get("email")]
        sent_count = _send_plain_email(recipients=recipients, subject=subject, body=body)
    except Exception as exc:  # noqa: BLE001
        return RedirectResponse(
            url=(
                f"/ui?strava_filter={strava_filter}&libre_filter={libre_filter}&dexcom_filter={dexcom_filter}"
                f"&carelink_filter={carelink_filter}"
                "&admin_status=error&admin_msg="
                + quote_plus(f"Envoi impossible : {exc}")
            ),
            status_code=303,
        )
    finally:
        db.close()

    return RedirectResponse(
        url=(
            f"/ui?strava_filter={strava_filter}&libre_filter={libre_filter}&dexcom_filter={dexcom_filter}"
            f"&carelink_filter={carelink_filter}"
            "&admin_status=ok&admin_msg="
            + quote_plus(f"Email envoyé à {sent_count} utilisateur(s).")
        ),
        status_code=303,
    )


@app.post("/admin/users/{user_id}/plan-credits")
def admin_add_plan_credits(
    request: Request,
    user_id: int,
    credits: int = Form(...),
    return_to: str = Form("/ui"),
):
    """Add complementary plan credits to one account and retain an admin audit record."""
    guard = _guard_admin(request)
    if guard:
        return guard
    safe_return_to = return_to if (return_to or "").startswith("/ui") else "/ui"
    separator = "&" if "?" in safe_return_to else "?"
    if credits < 1 or credits > 100:
        return RedirectResponse(
            url=safe_return_to + separator + "admin_status=error&admin_msg=" + quote_plus("Le nombre de crédits doit être compris entre 1 et 100."),
            status_code=303,
        )
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.id == user_id).one_or_none()
        if not user:
            message = "Utilisateur introuvable."
            status = "error"
        else:
            wallet = _get_plan_credit_wallet(db, user.id)
            wallet.credits += credits
            db.add(PlanPaymentAttempt(
                user_id=user.id,
                user_email=user.email,
                course_name="Crédits ajoutés par l’administrateur",
                plan_payload=json.dumps({"product": "admin_credit", "credits": credits}, ensure_ascii=False),
                amount_cents=0,
                currency="eur",
                status="admin_credit",
            ))
            db.commit()
            message = f"{credits} crédit(s) ajouté(s) à {user.email}. Nouveau solde : {wallet.credits}."
            status = "ok"
    except Exception:
        db.rollback()
        logger.exception("[PAYMENT] Ajout manuel de crédits impossible user=%s", user_id)
        message = "Impossible d’ajouter les crédits."
        status = "error"
    finally:
        db.close()
    return RedirectResponse(
        url=safe_return_to + separator + f"admin_status={status}&admin_msg=" + quote_plus(message),
        status_code=303,
    )


@app.post("/admin/users/bulk-delete")
async def admin_bulk_delete_users(request: Request):
    guard = _guard_admin(request)
    if guard:
        return guard

    form = await request.form()
    raw_ids = form.getlist("selected_user_ids")
    selected_user_ids = sorted({int(value) for value in raw_ids if str(value).isdigit()})
    user_page = _safe_positive_int(form.get("user_page"), 1)
    user_page_size = _safe_positive_int(form.get("user_page_size"), 10)
    if user_page_size not in {10, 50, 100}:
        user_page_size = 10
    strava_filter = _normalize_connection_filter(form.get("strava_filter"))
    libre_filter = _normalize_connection_filter(form.get("libre_filter"))
    dexcom_filter = _normalize_connection_filter(form.get("dexcom_filter"))
    carelink_filter = _normalize_connection_filter(form.get("carelink_filter"))
    activity_page = _safe_positive_int(form.get("activity_page"), 1)

    if not selected_user_ids:
        return RedirectResponse(
            url=(
                f"/ui?user_page={user_page}&user_page_size={user_page_size}"
                f"&strava_filter={strava_filter}&libre_filter={libre_filter}&dexcom_filter={dexcom_filter}"
                f"&carelink_filter={carelink_filter}"
                f"&activity_page={activity_page}&admin_status=warn&admin_msg="
                + quote_plus("Aucun utilisateur sélectionné.")
            ),
            status_code=303,
        )

    current_admin_id = _get_session_user_id(request)
    db = SessionLocal()
    try:
        users = db.query(User).filter(User.id.in_(selected_user_ids)).order_by(User.id.asc()).all()
        deleted_count = 0
        skipped_admin = False
        for user in users:
            if current_admin_id is not None and user.id == current_admin_id:
                skipped_admin = True
                continue
            _delete_user_account_data(db, user)
            deleted_count += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    message = f"{deleted_count} utilisateur(s) supprimé(s)."
    status = "ok"
    if skipped_admin:
        message += " Le compte admin courant a été ignoré."
        status = "warn" if deleted_count == 0 else "ok"

    return RedirectResponse(
        url=(
            f"/ui?user_page={user_page}&user_page_size={user_page_size}"
            f"&strava_filter={strava_filter}&libre_filter={libre_filter}&dexcom_filter={dexcom_filter}"
            f"&carelink_filter={carelink_filter}"
            f"&activity_page={activity_page}&admin_status={status}&admin_msg="
            + quote_plus(message)
        ),
        status_code=303,
    )


@app.get("/admin/glucose-dashboard", response_class=HTMLResponse)
def admin_glucose_dashboard(request: Request):
    session_user_id = _get_session_user_id(request)
    if session_user_id != 1:
        raise HTTPException(status_code=403, detail="Forbidden")

    now = dt.datetime.utcnow()
    start_dt = now - dt.timedelta(days=183)

    db = SessionLocal()
    try:
        total_users = db.query(User.id).count()
        total_activities = db.query(Activity.id).count()

        user_ids_from_glucose = {
            uid for (uid,) in db.query(GlucosePoint.user_id).distinct()
        }
        user_ids_from_acts = {
            uid
            for (uid,) in db.query(Activity.user_id)
            .filter(Activity.avg_glucose.isnot(None))
            .distinct()
        }
        users_with_glucose = len(user_ids_from_glucose | user_ids_from_acts)

        activities_with_glucose = (
            db.query(Activity.id)
            .filter(
                Activity.start_date >= start_dt,
                Activity.avg_glucose.isnot(None),
            )
            .count()
        )

        glucose_points_count = (
            db.query(GlucosePoint.id)
            .filter(GlucosePoint.ts >= start_dt, GlucosePoint.ts <= now)
            .count()
        )

        analyzed_seconds = (
            db.query(func.coalesce(func.sum(Activity.elapsed_time), 0))
            .filter(
                Activity.start_date >= start_dt,
                Activity.avg_glucose.isnot(None),
                Activity.elapsed_time.isnot(None),
            )
            .scalar()
            or 0
        )
        analyzed_hours = round(float(analyzed_seconds) / 3600.0, 1)

        dialect_name = db.get_bind().dialect.name
        if dialect_name == "postgresql":
            week_expr = func.to_char(
                func.date_trunc("week", Activity.start_date),
                "IYYY-\"W\"IW",
            )
        else:
            week_expr = func.strftime("%Y-%W", Activity.start_date)

        weekly_rows = (
            db.query(
                week_expr.label("week"),
                func.count(Activity.id).label("count"),
            )
            .filter(
                Activity.start_date >= start_dt,
                Activity.start_date <= now,
                Activity.avg_glucose.isnot(None),
            )
            .group_by("week")
            .order_by("week")
            .all()
        )
        weekly_data = []
        for week_key, count in weekly_rows:
            if week_key:
                parts = week_key.split("-")
                if len(parts) == 2:
                    week_label = f"{parts[0]}-W{parts[1]}"
                else:
                    week_label = week_key
            else:
                week_label = "n/a"
            weekly_data.append({"week": week_label, "count": int(count)})

        zones_order = ["Zone 1", "Zone 2", "Zone 3", "Zone 4", "Zone 5"]
        zone_rows = (
            db.query(
                ActivityStreamPoint.hr_zone.label("zone"),
                func.avg(ActivityStreamPoint.glucose_mgdl).label("avg_glucose"),
                func.count(ActivityStreamPoint.id).label("points"),
            )
            .join(Activity, ActivityStreamPoint.activity_id == Activity.id)
            .filter(
                Activity.start_date >= start_dt,
                Activity.start_date <= now,
                ActivityStreamPoint.glucose_mgdl.isnot(None),
                ActivityStreamPoint.hr_zone.isnot(None),
            )
            .group_by(ActivityStreamPoint.hr_zone)
            .all()
        )
        zone_map = {
            z: {
                "avg_glucose": float(avg) if avg is not None else None,
                "points": int(points),
            }
            for z, avg, points in zone_rows
        }
        hr_zone_data = []
        for zone in zones_order:
            entry = zone_map.get(zone, {"avg_glucose": None, "points": 0})
            hr_zone_data.append({"zone": zone, **entry})

        acts_in_window = (
            db.query(Activity)
            .filter(
                Activity.start_date >= start_dt,
                Activity.start_date <= now,
                Activity.avg_glucose.isnot(None),
            )
            .all()
        )
        act_info = {}
        for act in acts_in_window:
            act_info[act.id] = {
                "user_id": act.user_id,
                "start_date": act.start_date,
                "sport": act.sport or act.activity_type or "—",
                "elapsed_time": act.elapsed_time,
                "distance": act.distance,
                "dplus": act.total_elevation_gain,
                "avg_glucose": act.avg_glucose,
                "tir_percent": act.time_in_range_percent,
                "min_glucose": act.min_glucose,
                "max_glucose": act.max_glucose,
                "hypo_count": act.hypo_count,
                "hyper_count": act.hyper_count,
            }

        zone_counts_rows = (
            db.query(
                ActivityStreamPoint.activity_id,
                ActivityStreamPoint.hr_zone,
                func.count(ActivityStreamPoint.id),
            )
            .join(Activity, ActivityStreamPoint.activity_id == Activity.id)
            .filter(
                Activity.start_date >= start_dt,
                Activity.start_date <= now,
                ActivityStreamPoint.hr_zone.isnot(None),
            )
            .group_by(ActivityStreamPoint.activity_id, ActivityStreamPoint.hr_zone)
            .all()
        )
        zone_counts_by_act = {}
        for act_id, zone, cnt in zone_counts_rows:
            if act_id not in act_info:
                continue
            zone_counts_by_act.setdefault(act_id, {})[zone] = int(cnt)

        profile_by_act = {}
        for act_id, zone_counts in zone_counts_by_act.items():
            total_zone = sum(zone_counts.values())
            if total_zone <= 0:
                continue
            z1 = zone_counts.get("Zone 1", 0)
            z2 = zone_counts.get("Zone 2", 0)
            z3 = zone_counts.get("Zone 3", 0)
            z4 = zone_counts.get("Zone 4", 0)
            z5 = zone_counts.get("Zone 5", 0)
            if (z5 / total_zone) >= 0.25:
                profile_by_act[act_id] = "fractionne"
            elif (z4 / total_zone) >= 0.20:
                profile_by_act[act_id] = "seuil"
            elif ((z1 + z2 + z3) / total_zone) >= 0.70:
                profile_by_act[act_id] = "endurance"
            else:
                profile_by_act[act_id] = "endurance"

        glucose_points_count_by_act = {}
        first_point_by_act = {}
        last_point_by_act = {}
        zone_stats = {
            zone: {"count": 0, "sum": 0.0, "sumsq": 0.0, "in_range": 0}
            for zone in zones_order
        }
        profile_bins = {
            "endurance": {},
            "seuil": {},
            "fractionne": {},
        }
        profile_event_bins = {
            "endurance": {},
            "seuil": {},
            "fractionne": {},
        }
        profile_tir_bins = {
            "endurance": {},
            "seuil": {},
            "fractionne": {},
        }

        glucose_points_rows = (
            db.query(
                ActivityStreamPoint.activity_id,
                ActivityStreamPoint.idx,
                ActivityStreamPoint.elapsed_time,
                ActivityStreamPoint.glucose_mgdl,
                ActivityStreamPoint.hr_zone,
            )
            .join(Activity, ActivityStreamPoint.activity_id == Activity.id)
            .filter(
                Activity.start_date >= start_dt,
                Activity.start_date <= now,
                ActivityStreamPoint.glucose_mgdl.isnot(None),
            )
            .all()
        )
        for act_id, idx, elapsed_time, glucose, hr_zone in glucose_points_rows:
            if act_id not in act_info:
                continue
            g_val = float(glucose)
            glucose_points_count_by_act[act_id] = (
                glucose_points_count_by_act.get(act_id, 0) + 1
            )

            first = first_point_by_act.get(act_id)
            if first is None or idx < first["idx"]:
                first_point_by_act[act_id] = {"idx": idx, "glucose": g_val}
            last = last_point_by_act.get(act_id)
            if last is None or idx > last["idx"]:
                last_point_by_act[act_id] = {"idx": idx, "glucose": g_val}

            if hr_zone in zone_stats:
                zs = zone_stats[hr_zone]
                zs["count"] += 1
                zs["sum"] += g_val
                zs["sumsq"] += g_val * g_val
                if 70 <= g_val <= 180:
                    zs["in_range"] += 1

            profile = profile_by_act.get(act_id)
            if profile and elapsed_time is not None:
                minute = int(elapsed_time / 60)
                if 0 <= minute <= 120:
                    bucket = int(minute / 5) * 5
                    acc = profile_bins[profile].setdefault(
                        bucket, {"sum": 0.0, "count": 0}
                    )
                    acc["sum"] += g_val
                    acc["count"] += 1

                bucket10 = int(minute / 10) * 10
                if 0 <= bucket10 <= 120:
                    ev = profile_event_bins[profile].setdefault(
                        bucket10, {"total": 0, "hypo": 0, "hyper": 0}
                    )
                    ev["total"] += 1
                    if g_val < 70:
                        ev["hypo"] += 1
                    elif g_val > 180:
                        ev["hyper"] += 1
                    tir = profile_tir_bins[profile].setdefault(
                        bucket10, {"total": 0, "in_range": 0}
                    )
                    tir["total"] += 1
                    if 70 <= g_val <= 180:
                        tir["in_range"] += 1

        tir_by_zone = []
        hr_zone_table = []
        for zone in zones_order:
            zs = zone_stats[zone]
            count = zs["count"]
            avg = (zs["sum"] / count) if count else None
            var = (zs["sumsq"] / count - (avg * avg)) if count and avg is not None else None
            stddev = (var ** 0.5) if var is not None and var >= 0 else None
            tir = (zs["in_range"] / count * 100.0) if count else None
            hr_zone_table.append(
                {
                    "zone": zone,
                    "duration_min": count,
                    "avg_glucose": round(avg, 1) if avg is not None else None,
                    "tir_percent": round(tir, 1) if tir is not None else None,
                    "stddev": round(stddev, 1) if stddev is not None else None,
                }
            )
            tir_by_zone.append(
                {
                    "zone": zone,
                    "tir_percent": round(tir, 1) if tir is not None else None,
                }
            )

        duration_points = []
        coverage_by_duration = []
        duration_bins_cov = [
            ("<30", 0, 30),
            ("30-60", 30, 60),
            ("60-90", 60, 90),
            ("90-120", 90, 120),
            ("120-180", 120, 180),
            (">180", 180, None),
        ]
        cov_acc = {label: {"sum": 0.0, "count": 0} for label, _, _ in duration_bins_cov}

        duration_bins = [
            ("<45", 0, 45),
            ("45-75", 45, 75),
            ("75-120", 75, 120),
            ("120-180", 120, 180),
            (">180", 180, None),
        ]
        bucket_acc = {
            label: {"count": 0, "sum_glucose": 0.0, "sum_tir": 0.0, "sum_drift": 0.0, "sum_hypos_per_h": 0.0}
            for label, _, _ in duration_bins
        }

        drift_scatter = []
        drift_values = []
        drift_by_zone = {zone: {"sum": 0.0, "count": 0} for zone in zones_order}

        profile_rules = {
            "endurance": {"sum_glucose": 0.0, "sum_tir": 0.0, "sum_amp": 0.0, "sum_hypos_per_h": 0.0, "count": 0},
            "seuil": {"sum_glucose": 0.0, "sum_tir": 0.0, "sum_amp": 0.0, "sum_hypos_per_h": 0.0, "count": 0},
            "fractionne": {"sum_glucose": 0.0, "sum_tir": 0.0, "sum_amp": 0.0, "sum_hypos_per_h": 0.0, "count": 0},
        }

        heatmap_data = {zone: {} for zone in zones_order}
        heatmap_bins = duration_bins

        for act_id, info in act_info.items():
            elapsed = info["elapsed_time"] or 0
            duration_min = (elapsed / 60.0) if elapsed else None
            points_count = glucose_points_count_by_act.get(act_id, 0)

            if duration_min and duration_min > 0:
                duration_points.append({"x": round(duration_min, 1), "y": points_count})
                coverage_pct = min(100.0, (points_count / duration_min) * 100.0)
                for label, lo, hi in duration_bins_cov:
                    if (duration_min >= lo) and (hi is None or duration_min < hi):
                        cov_acc[label]["sum"] += coverage_pct
                        cov_acc[label]["count"] += 1
                        break

            first = first_point_by_act.get(act_id)
            last = last_point_by_act.get(act_id)
            drift = None
            if first and last:
                drift = last["glucose"] - first["glucose"]
                drift_values.append(drift)

            if duration_min and drift is not None:
                drift_scatter.append({"x": round(duration_min, 1), "y": round(drift, 1)})

            for label, lo, hi in duration_bins:
                if duration_min is None:
                    continue
                if duration_min >= lo and (hi is None or duration_min < hi):
                    bucket = bucket_acc[label]
                    if info["avg_glucose"] is not None:
                        bucket["sum_glucose"] += float(info["avg_glucose"])
                    if info["tir_percent"] is not None:
                        bucket["sum_tir"] += float(info["tir_percent"])
                    if drift is not None:
                        bucket["sum_drift"] += float(drift)
                    hypos_per_h = None
                    if elapsed and info["hypo_count"] is not None and elapsed > 0:
                        hypos_per_h = float(info["hypo_count"]) / (elapsed / 3600.0)
                    if hypos_per_h is not None:
                        bucket["sum_hypos_per_h"] += hypos_per_h
                    bucket["count"] += 1
                    break

            zone_counts = zone_counts_by_act.get(act_id, {})
            total_zone = sum(zone_counts.values())
            dominant_zone = None
            if total_zone > 0:
                dominant_zone = max(zone_counts, key=zone_counts.get)
                if dominant_zone in drift_by_zone and drift is not None:
                    drift_by_zone[dominant_zone]["sum"] += drift
                    drift_by_zone[dominant_zone]["count"] += 1

                profile = profile_by_act.get(act_id)
                if profile:
                    prof = profile_rules[profile]
                    if info["avg_glucose"] is not None:
                        prof["sum_glucose"] += float(info["avg_glucose"])
                    if info["tir_percent"] is not None:
                        prof["sum_tir"] += float(info["tir_percent"])
                    if info["min_glucose"] is not None and info["max_glucose"] is not None:
                        prof["sum_amp"] += float(info["max_glucose"]) - float(info["min_glucose"])
                    if elapsed and info["hypo_count"] is not None and elapsed > 0:
                        prof["sum_hypos_per_h"] += float(info["hypo_count"]) / (elapsed / 3600.0)
                    prof["count"] += 1

                for label, lo, hi in heatmap_bins:
                    if duration_min is None:
                        continue
                    if duration_min >= lo and (hi is None or duration_min < hi):
                        key = label
                        cell = heatmap_data[dominant_zone].setdefault(key, {"sum": 0.0, "count": 0})
                        if drift is not None:
                            cell["sum"] += drift
                            cell["count"] += 1
                        break

        coverage_by_duration = []
        for label, _, _ in duration_bins_cov:
            acc = cov_acc[label]
            avg_cov = (acc["sum"] / acc["count"]) if acc["count"] else None
            coverage_by_duration.append({"label": label, "avg_coverage": round(avg_cov, 1) if avg_cov is not None else None})

        duration_buckets = []
        for label, _, _ in duration_bins:
            acc = bucket_acc[label]
            count = acc["count"]
            duration_buckets.append(
                {
                    "label": label,
                    "avg_glucose": round(acc["sum_glucose"] / count, 1) if count else None,
                    "avg_tir": round(acc["sum_tir"] / count, 1) if count else None,
                    "avg_drift": round(acc["sum_drift"] / count, 1) if count else None,
                    "avg_hypos_per_h": round(acc["sum_hypos_per_h"] / count, 2) if count else None,
                }
            )

        profile_data = []
        for label in ["endurance", "seuil", "fractionne"]:
            acc = profile_rules[label]
            count = acc["count"]
            profile_data.append(
                {
                    "profile": label,
                    "count": count,
                    "avg_glucose": round(acc["sum_glucose"] / count, 1) if count else None,
                    "avg_tir": round(acc["sum_tir"] / count, 1) if count else None,
                    "avg_amp": round(acc["sum_amp"] / count, 1) if count else None,
                    "avg_hypos_per_h": round(acc["sum_hypos_per_h"] / count, 2) if count else None,
                }
            )

        drift_by_zone_list = []
        for zone in zones_order:
            dz = drift_by_zone[zone]
            avg_drift = (dz["sum"] / dz["count"]) if dz["count"] else None
            drift_by_zone_list.append({"zone": zone, "avg_drift": round(avg_drift, 1) if avg_drift is not None else None})

        drift_hist = []
        if drift_values:
            bins = [-100, -80, -60, -40, -20, 0, 20, 40, 60, 80, 100]
            counts = [0 for _ in range(len(bins) + 1)]
            for val in drift_values:
                placed = False
                for i in range(len(bins) - 1):
                    if bins[i] <= val < bins[i + 1]:
                        counts[i + 1] += 1
                        placed = True
                        break
                if not placed:
                    if val < bins[0]:
                        counts[0] += 1
                    else:
                        counts[-1] += 1
            labels = [f"<{bins[0]}"] + [f"{bins[i]}:{bins[i+1]}" for i in range(len(bins) - 1)] + [f">{bins[-1]}"]
            for label, count in zip(labels, counts):
                drift_hist.append({"label": label, "count": count})

        profile_glucose_series = {}
        for profile in ["endurance", "seuil", "fractionne"]:
            series = []
            for minute in range(0, 121, 5):
                acc = profile_bins[profile].get(minute)
                if acc and acc["count"]:
                    avg_val = acc["sum"] / acc["count"]
                    series.append({"minute": minute, "avg_glucose": round(avg_val, 1)})
                else:
                    series.append({"minute": minute, "avg_glucose": None})
            profile_glucose_series[profile] = series

        profile_event_series = {}
        for profile in ["endurance", "seuil", "fractionne"]:
            series = []
            for minute in range(0, 121, 10):
                acc = profile_event_bins[profile].get(minute)
                if acc and acc["total"]:
                    hypo_pct = acc["hypo"] / acc["total"] * 100.0
                    hyper_pct = acc["hyper"] / acc["total"] * 100.0
                    series.append(
                        {
                            "minute": minute,
                            "hypo_pct": round(hypo_pct, 1),
                            "hyper_pct": round(hyper_pct, 1),
                        }
                    )
                else:
                    series.append(
                        {"minute": minute, "hypo_pct": None, "hyper_pct": None}
                    )
            profile_event_series[profile] = series

        profile_tir_series = {}
        for profile in ["endurance", "seuil", "fractionne"]:
            series = []
            for minute in range(0, 121, 10):
                acc = profile_tir_bins[profile].get(minute)
                if acc and acc["total"]:
                    tir_pct = acc["in_range"] / acc["total"] * 100.0
                    series.append({"minute": minute, "tir_pct": round(tir_pct, 1)})
                else:
                    series.append({"minute": minute, "tir_pct": None})
            profile_tir_series[profile] = series

        recent_acts_rows = (
            db.query(Activity)
            .filter(
                Activity.start_date >= start_dt,
                Activity.avg_glucose.isnot(None),
            )
            .order_by(Activity.start_date.desc())
            .limit(50)
            .all()
        )
        recent_acts = []
        for act in recent_acts_rows:
            recent_acts.append(
                {
                    "start_date": act.start_date.strftime("%Y-%m-%d %H:%M")
                    if act.start_date
                    else "n/a",
                    "user_id": act.user_id,
                    "activity_id": act.id,
                    "sport": act.sport or act.activity_type or "—",
                    "elapsed_str": _format_duration(act.elapsed_time)
                    if act.elapsed_time
                    else "—",
                    "distance_km": round((act.distance or 0) / 1000.0, 1)
                    if act.distance
                    else None,
                    "dplus_m": int(round(act.total_elevation_gain))
                    if act.total_elevation_gain is not None
                    else None,
                    "avg_glucose": round(float(act.avg_glucose), 1)
                    if act.avg_glucose is not None
                    else None,
                    "tir_percent": round(float(act.time_in_range_percent), 1)
                    if act.time_in_range_percent is not None
                    else None,
                    "min_glucose": round(float(act.min_glucose), 1)
                    if act.min_glucose is not None
                    else None,
                    "max_glucose": round(float(act.max_glucose), 1)
                    if act.max_glucose is not None
                    else None,
                    "hypo_count": act.hypo_count,
                    "hyper_count": act.hyper_count,
                }
            )
    finally:
        db.close()

    return templates.TemplateResponse(
        "glucose_dashboard.html",
        {
            "request": request,
            "current_user_id": session_user_id,
            "kpis": {
                "total_users": total_users,
                "users_with_glucose": users_with_glucose,
                "total_activities": total_activities,
                "activities_with_glucose": activities_with_glucose,
                "glucose_points_count": glucose_points_count,
                "analyzed_hours": analyzed_hours,
            },
            "weekly_data": weekly_data,
            "hr_zone_data": hr_zone_data,
            "tir_by_zone": tir_by_zone,
            "duration_points": duration_points,
            "coverage_by_duration": coverage_by_duration,
            "duration_buckets": duration_buckets,
            "profile_data": profile_data,
            "profile_glucose_series": profile_glucose_series,
            "profile_event_series": profile_event_series,
            "profile_tir_series": profile_tir_series,
            "drift_scatter": drift_scatter,
            "drift_by_zone": drift_by_zone_list,
            "drift_hist": drift_hist,
            "hr_zone_table": hr_zone_table,
            "heatmap_data": heatmap_data,
            "heatmap_bins": [label for label, _, _ in heatmap_bins],
            "recent_acts": recent_acts,
            "period_start": start_dt.strftime("%Y-%m-%d"),
            "period_end": now.strftime("%Y-%m-%d"),
        },
    )

@app.get("/", response_class=HTMLResponse)
def home_redirect(request: Request):
    """
    Page d’accueil publique : toujours la landing / login,
    même si une session est active (le header offre un lien déconnexion).
    """
    return _render_login_page(request)


def _seo_cta_url(request: Request, course_id: str | None = None) -> str:
    user_id = request.session.get("user_id")
    if user_id:
        suffix = "#simulation" if course_id else ""
        return f"/ui/user/{user_id}{suffix}"
    return "/ui/signup"


SEO_EVENT_REGISTRY = {
    "festival-des-templiers": {"name": "Festival des Templiers", "year": 2026, "location": "Millau, Aveyron", "date_label": "16–18 octobre 2026", "prefixes": (), "ids": set(TEMPLIERS_2026_DEPARTURES)},
    "saintelyon": {"name": "SaintéLyon", "year": 2026, "location": "Saint-Étienne et Lyon", "date_label": "édition 2026", "prefixes": ("saintelyon-",), "ids": set()},
    "ecotrail-paris": {"name": "EcoTrail Paris", "year": 2027, "location": "Île-de-France et Paris", "date_label": "20–21 mars 2027", "prefixes": ("ecotrail-paris-",), "ids": set()},
    "maxi-race-annecy": {"name": "MaXi-Race du lac d’Annecy", "year": 2027, "location": "Annecy, Haute-Savoie", "date_label": "édition 2027", "prefixes": ("maxi-race-",), "ids": set()},
    "vvx": {"name": "Volvic Volcanic Experience", "year": 2027, "location": "Volvic, Puy-de-Dôme", "date_label": "6–8 mai 2027", "prefixes": ("vvx-",), "ids": set()},
    "utmb-mont-blanc": {"name": "UTMB Mont-Blanc", "year": 2026, "location": "Chamonix-Mont-Blanc", "date_label": "édition 2026", "prefixes": (), "ids": {"utmb-2026", "ccc-2026", "occ-2026", "tds-2026", "mcc-2026", "etc-2026"}},
    "grand-raid-pyrenees": {"name": "Grand Raid des Pyrénées", "year": 2026, "location": "Pyrénées", "date_label": "édition 2026", "prefixes": ("grp-",), "ids": set()},
    "grand-raid-reunion": {"name": "Grand Raid de la Réunion", "year": 2026, "location": "La Réunion", "date_label": "édition 2026", "prefixes": (), "ids": {"diagonale-des-fous-2026", "trail-de-bourbon-2026"}},
}


def _seo_course_event(course: dict | None) -> dict | None:
    if not course:
        return None
    course_id = str(course.get("id") or "")
    declared_name = str(course.get("event_name") or "")
    for slug, event in SEO_EVENT_REGISTRY.items():
        if course_id in event["ids"] or any(course_id.startswith(prefix) for prefix in event["prefixes"]) or declared_name == event["name"]:
            return {"slug": slug, **{key: value for key, value in event.items() if key not in {"prefixes", "ids"}}}
    return None


def _seo_event_courses(event_slug: str) -> list[dict]:
    courses = [
        course for course in (_seo_course_payload(str(row["id"])) for row in _load_official_course_catalog())
        if course and (event := _seo_course_event(course)) and event["slug"] == event_slug
    ]
    courses.sort(key=lambda item: float(item.get("distance_km") or 0), reverse=True)
    return courses


def _seo_course_start_date(course: dict) -> str | None:
    departure = TEMPLIERS_2026_DEPARTURES.get(str(course.get("id") or ""))
    if not departure:
        return course.get("event_start_date")
    label = departure[0].lower()
    day = "16" if label.startswith("vendredi") else "17" if label.startswith("samedi") else "18"
    start = next((point for point in course.get("points") or [] if point.get("type") == "start"), {})
    match = re.search(r"(\d{1,2}):(\d{2})", str(start.get("fastest_label") or ""))
    time_part = f"T{int(match.group(1)):02d}:{match.group(2)}:00+02:00" if match else ""
    return f"2026-10-{day}{time_part}"


def _seo_page_context(request: Request, *, title: str, description: str, path: str, **extra) -> dict:
    base_url = _get_app_base_url()
    canonical_url = f"{base_url}{path}"
    page_kind = extra.get("page_kind", "website")
    schema_type = "Article" if page_kind == "guide" else "WebPage"
    breadcrumb_rows = extra.get("breadcrumbs") or [("Accueil", "/"), (title, path)]
    schema = {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": schema_type,
                "headline": title,
                "name": title,
                "description": description,
                "url": canonical_url,
                "inLanguage": "fr-FR",
            },
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {"@type": "ListItem", "position": position, "name": name, "item": base_url + item_path}
                    for position, (name, item_path) in enumerate(breadcrumb_rows, start=1)
                ],
            },
        ],
    }
    course = extra.get("course")
    seo_image_url = f"{base_url}/static/logo.png"
    if page_kind == "course" and isinstance(course, dict):
        seo_image_url = f"{base_url}/courses/{course['slug']}/og.png"
        event_schema = {
            "@type": "SportsEvent",
            "name": course.get("name"),
            "description": description,
            "url": canonical_url,
            "inLanguage": "fr-FR",
            "sport": "Trail running",
            "eventStatus": "https://schema.org/EventScheduled",
            "organizer": {"@type": "Organization", "name": course.get("event_name") or _course_event_name(str(course.get("id") or ""))},
            "image": seo_image_url,
        }
        start_date = _seo_course_start_date(course)
        if start_date:
            event_schema["startDate"] = start_date
        location = course.get("start_location")
        if location:
            event_schema["location"] = {"@type": "Place", "name": location, "address": {"@type": "PostalAddress", "addressCountry": "FR"}}
        schema["@graph"].append(event_schema)
    event_courses = extra.get("event_courses") or []
    if event_courses:
        schema["@graph"].append({
            "@type": "ItemList",
            "name": title,
            "itemListElement": [
                {"@type": "ListItem", "position": index, "name": item["name"], "url": f"{base_url}/courses/{item['slug']}"}
                for index, item in enumerate(event_courses, start=1)
            ],
        })
    faq_items = extra.get("faq_items") or []
    if faq_items:
        schema["@graph"].append({
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": str(question),
                    "acceptedAnswer": {"@type": "Answer", "text": str(answer)},
                }
                for question, answer in faq_items
            ],
        })
    return {
        "request": request,
        "seo_title": title,
        "seo_description": description,
        "seo_keywords": extra.get("seo_keywords", ""),
        "canonical_url": canonical_url,
        "seo_image_url": seo_image_url,
        "seo_og_type": "article" if page_kind in {"guide", "course"} else "website",
        "seo_robots": "index,follow",
        "schema_json": json.dumps(schema, ensure_ascii=False),
        "guide_links": [
            {"slug": slug, "title": guide["title"]}
            for slug, guide in SEO_GUIDES.items()
        ],
        "cta_url": _seo_cta_url(request, extra.get("course_id")),
        **extra,
    }


@app.get("/guides", response_class=HTMLResponse)
def seo_guides_index(request: Request):
    return templates.TemplateResponse("seo_index.html", _seo_page_context(
        request,
        title="Guides trail, glycémie et diabète de type 1 pendant le sport",
        description="Guides pratiques sur le plan de course trail, le pacing, les allures selon la pente et la gestion de la glycémie pendant le sport avec un diabète de type 1.",
        path="/guides",
        page_kind="guides",
        guides=[{"slug": slug, **guide} for slug, guide in SEO_GUIDES.items()],
    ))


@app.get("/guides/{slug}", response_class=HTMLResponse)
def seo_guide_detail(request: Request, slug: str):
    guide = SEO_GUIDES.get(slug)
    if not guide:
        raise HTTPException(status_code=404, detail="Guide introuvable.")
    related_courses = [
        _seo_course_payload(course_id) for course_id in guide.get("course_ids", [])
    ]
    return templates.TemplateResponse("seo_guide.html", _seo_page_context(
        request,
        title=guide["title"],
        description=guide["description"],
        path=f"/guides/{slug}",
        page_kind="guide",
        guide={"slug": slug, **guide},
        related_courses=[course for course in related_courses if course],
    ))


@app.get("/courses", response_class=HTMLResponse)
def seo_courses_index(request: Request):
    courses = []
    for item in _load_official_course_catalog():
        course = _seo_course_payload(str(item["id"]))
        if course:
            courses.append(course)
    return templates.TemplateResponse("seo_index.html", _seo_page_context(
        request,
        title="Courses trail : profils GPX, ravitos et plan de course",
        description="Prépare un plan de course trail à partir du profil GPX, des ravitaillements, des barrières et de tes allures selon la pente.",
        path="/courses",
        page_kind="courses",
        courses=courses,
        event_hubs=[
            {
                "name": f"{event['name']} {event['year']}",
                "slug": event_slug,
                "description": f"Toutes les courses : parcours GPX, ravitaillements, pacing, nutrition et plans de course.",
                "count": len(_seo_event_courses(event_slug)),
            }
            for event_slug, event in SEO_EVENT_REGISTRY.items()
            if _seo_event_courses(event_slug)
        ],
    ))


@app.get("/courses/{slug}", response_class=HTMLResponse)
def seo_course_detail(request: Request, slug: str):
    if slug in SEO_EVENT_REGISTRY:
        event = {"slug": slug, **{key: value for key, value in SEO_EVENT_REGISTRY[slug].items() if key not in {"prefixes", "ids"}}}
        event_courses = _seo_event_courses(slug)
        return templates.TemplateResponse("seo_event.html", _seo_page_context(
            request,
            title=f"{event['name']} {event['year']} : parcours, courses et plans de course",
            description=f"Prépare toutes les courses de {event['name']} {event['year']} : parcours GPX, dénivelés, ravitaillements, pacing, nutrition et plans de course personnalisés.",
            seo_keywords=f"{event['name']} {event['year']}, parcours, GPX, ravitaillements, plan de course, pacing",
            path=f"/courses/{slug}",
            page_kind="event",
            event=event,
            event_courses=event_courses,
            breadcrumbs=[("Accueil", "/"), ("Courses", "/courses"), (event["name"], f"/courses/{slug}")],
        ))
    course_id = next(
        (str(item["id"]) for item in _load_official_course_catalog() if _seo_course_slug(item["id"]) == slug),
        None,
    )
    course = _seo_course_payload(course_id) if course_id else None
    if not course:
        raise HTTPException(status_code=404, detail="Course introuvable.")
    description = (
        f"Préparer {course['name']} : plan de course, pacing, nutrition et ravitaillements. "
        + (f"Profil GPX, allures selon la pente et barrières horaires. {course.get('distance_km')} km · D+ {course.get('elevation_gain_m')} m."
           if course.get("route_available") else
           (f"Anticipe la sortie du GPX officiel en enrichissant ton historique d’entraînement. Format annoncé : {course.get('distance_km')} km."
            if course.get("distance_km") is not None else
            "Anticipe la sortie du GPX officiel en enrichissant ton historique d’entraînement. Distance et dénivelé à confirmer."))
    )
    related = [
        item for item in (_seo_course_payload(str(row["id"])) for row in _load_official_course_catalog())
        if item and item["id"] != course["id"]
    ][:3]
    event = _seo_course_event(course)
    return templates.TemplateResponse("seo_course.html", _seo_page_context(
        request,
        title=(f"{course['name']} : parcours GPX, carte et points de passage" if course.get("route_available") else f"{course['name']} : plan de course, pacing et nutrition"),
        description=description,
        seo_keywords=(f"{course['name']}, parcours, parcours GPX, carte, points de passage, profil altimétrique, ravitaillements, plan de course, pacing" if course.get("route_available") else f"{course['name']}, plan de course, pacing, nutrition, GPX"),
        path=f"/courses/{course['slug']}",
        page_kind="course",
        course=course,
        course_id=course["id"],
        faq_items=course["editorial"]["faq"],
        related_courses=related,
        event=event,
        breadcrumbs=[("Accueil", "/"), ("Courses", "/courses")]
        + ([(event["name"], f"/courses/{event['slug']}")] if event else [])
        + [(course["short_name"], f"/courses/{course['slug']}")],
    ))


SEO_COURSE_TOPICS = {
    "parcours": {
        "label": "Parcours GPX",
        "title": "parcours GPX et profil altimétrique",
        "description": "Découvre le parcours GPX, la distance, le dénivelé, le profil altimétrique et le découpage de la course.",
        "requires_gpx": True,
    },
    "carte": {
        "label": "Carte",
        "title": "carte interactive du parcours",
        "description": "Explore la carte interactive de la course, sa trace GPX et la position des principaux points du parcours.",
        "requires_gpx": True,
    },
    "points-de-passage": {
        "label": "Points de passage",
        "title": "points de passage, ravitaillements et contrôles",
        "description": "Retrouve les points de passage kilométriques, ravitaillements, contrôles et barrières horaires du parcours.",
        "requires_gpx": True,
        "requires_points": True,
    },
    "nutrition-alimentation": {
        "label": "Nutrition et glucides",
        "title": "nutrition, alimentation et glucides",
        "description": "Prépare une stratégie de nutrition, d’alimentation, d’hydratation et de glucides cohérente avec la durée et le pacing de la course.",
        "requires_gpx": False,
    },
    "assistance-ravitaillements": {
        "label": "Assistance et ravitaillements",
        "title": "assistance, ravitaillements et autonomie",
        "description": "Organise les ravitaillements, l’assistance, le contenu du sac et l’autonomie nécessaire entre les points de passage.",
        "requires_gpx": False,
    },
    "plan-entrainement": {
        "label": "Plan d’entraînement",
        "title": "plan d’entraînement et préparation trail",
        "description": "Construis un plan d’entraînement adapté à la distance, au dénivelé, au terrain et à ton historique personnel.",
        "requires_gpx": False,
    },
}


def _seo_social_card_png(course: dict) -> bytes:
    """Generate a lightweight, cacheable course-specific Open Graph image."""
    width, height = 1200, 630
    seed = int(hashlib.sha256(str(course.get("id") or "course").encode()).hexdigest()[:6], 16)
    accent = (210 + seed % 35, 65 + (seed >> 4) % 35, 35 + (seed >> 8) % 25)
    pixels = [bytearray([245, 241, 233] * width) for _ in range(height)]
    for y in range(70, 92):
        for x in range(70, 1130):
            pixels[y][x * 3:x * 3 + 3] = bytes(accent)
    profile = course.get("map_profile") or []
    elevations = [float(point.get("elevation_m") or 0) for point in profile]
    if len(elevations) >= 2:
        low, high = min(elevations), max(elevations)
        span = max(1.0, high - low)
        coords = [
            (70 + round(index / (len(elevations) - 1) * 1060), 540 - round((value - low) / span * 330))
            for index, value in enumerate(elevations)
        ]
        for (x0, y0), (x1, y1) in zip(coords, coords[1:]):
            steps = max(abs(x1 - x0), abs(y1 - y0), 1)
            for step in range(steps + 1):
                x = round(x0 + (x1 - x0) * step / steps)
                y = round(y0 + (y1 - y0) * step / steps)
                for thickness in range(-3, 4):
                    yy = y + thickness
                    if 0 <= x < width and 0 <= yy < height:
                        pixels[yy][x * 3:x * 3 + 3] = bytes(accent)
    raw = b"".join(b"\x00" + bytes(row) for row in pixels)
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw, 8)) + chunk(b"IEND", b"")


@app.get("/courses/{slug}/og.png", response_class=Response)
def seo_course_social_image(slug: str):
    course_id = next((str(item["id"]) for item in _load_official_course_catalog() if _seo_course_slug(item["id"]) == slug), None)
    course = _seo_course_payload(course_id) if course_id else None
    if not course:
        raise HTTPException(status_code=404, detail="Course introuvable.")
    return Response(_seo_social_card_png(course), media_type="image/png", headers={"Cache-Control": "public, max-age=86400"})


@app.get("/courses/{slug}/{topic}", response_class=HTMLResponse)
def seo_course_topic(request: Request, slug: str, topic: str):
    topic_data = SEO_COURSE_TOPICS.get(topic)
    if not topic_data:
        raise HTTPException(status_code=404, detail="Page de course introuvable.")
    course_id = next(
        (str(item["id"]) for item in _load_official_course_catalog() if _seo_course_slug(item["id"]) == slug),
        None,
    )
    course = _seo_course_payload(course_id) if course_id else None
    if (
        not course
        or (topic_data.get("requires_gpx") and not course.get("route_available"))
        or (topic_data.get("requires_points") and not course.get("points"))
    ):
        raise HTTPException(status_code=404, detail="Trace GPX indisponible.")
    title = f"{course['name']} : {topic_data['title']}"
    course_metrics = f"Format annoncé : {course.get('distance_label') or (str(course['distance_km']) + ' km')}." if course.get("distance_km") is not None else "Distance et dénivelé 2027 à confirmer."
    if course.get("distance_km") is not None and course.get("elevation_gain_m") is not None:
        course_metrics = f"{course['distance_km']} km · D+ {course['elevation_gain_m']} m."
    description = f"{topic_data['description']} {course_metrics}"
    event = _seo_course_event(course)
    return templates.TemplateResponse("seo_course_topic.html", _seo_page_context(
        request,
        title=title,
        description=description,
        seo_keywords=f"{course['name']}, {topic_data['label']}, nutrition trail, alimentation, glucides, assistance, ravitaillements, plan d’entraînement, pacing",
        path=f"/courses/{course['slug']}/{topic}",
        page_kind="course",
        course=course,
        topic=topic,
        topic_data=topic_data,
        course_topics=SEO_COURSE_TOPICS,
        course_id=course["id"],
        faq_items=course["editorial"]["faq"],
        event=event,
        seo_robots="index,follow" if course.get("route_available") else "noindex,follow",
        breadcrumbs=[("Accueil", "/"), ("Courses", "/courses")]
        + ([(event["name"], f"/courses/{event['slug']}")] if event else [])
        + [(course["short_name"], f"/courses/{course['slug']}"), (topic_data["label"], f"/courses/{course['slug']}/{topic}")],
    ))


@app.get("/demo/utmb-3d", response_class=HTMLResponse)
@app.get("/demo/parcours-3d", response_class=HTMLResponse)
def course_3d_demo(request: Request, course_id: str | None = Query(default=None)):
    """Banc d'essai public Cesium + relief MapTiler pour les GPX officiels."""
    available_courses = [
        item for item in _load_official_course_catalog()
        if item.get("route_available")
    ]
    available_course_ids = {str(item["id"]) for item in available_courses}
    selected_course_id = str(course_id or "utmb-2026")
    if selected_course_id not in available_course_ids:
        selected_course_id = "utmb-2026" if "utmb-2026" in available_course_ids else next(iter(available_course_ids), "")
    course = _seo_course_payload(selected_course_id)
    if not course or len(course.get("map_profile_3d") or []) < 2:
        raise HTTPException(status_code=404, detail="Trace GPX indisponible.")
    return templates.TemplateResponse(
        "utmb_3d_demo.html",
        {
            "request": request,
            "course": course,
            "course_options": sorted(available_courses, key=lambda item: str(item.get("name") or "")),
            "maptiler_api_key": settings.MAPTILER_API_KEY or "",
        },
    )


@app.get("/robots.txt", response_class=Response)
def seo_robots(request: Request):
    return Response(
        content=f"User-agent: *\nAllow: /\nSitemap: {_get_app_base_url()}/sitemap.xml\n",
        media_type="text/plain",
    )


@app.get("/sitemap.xml", response_class=Response)
def seo_sitemap(request: Request):
    base_url = _get_app_base_url()
    paths = ["/", "/guides", "/courses"]
    paths += [
        f"/courses/{event_slug}"
        for event_slug in SEO_EVENT_REGISTRY
        if _seo_event_courses(event_slug)
    ]
    paths += [f"/guides/{slug}" for slug in SEO_GUIDES]
    sitemap_courses = [
        course
        for item in _load_official_course_catalog()
        if (course := _seo_course_payload(str(item["id"])))
    ]
    paths += [f"/courses/{course['slug']}" for course in sitemap_courses]
    paths += [
        f"/courses/{course['slug']}/{topic}"
        for course in sitemap_courses
        for topic, topic_data in SEO_COURSE_TOPICS.items()
        if course.get("route_available")
        and (not topic_data.get("requires_points") or course.get("points"))
    ]
    template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
    content_paths = [os.path.join(os.path.dirname(__file__), "main.py")]
    content_paths += [os.path.join(template_dir, name) for name in os.listdir(template_dir) if name.startswith("seo_")]
    content_paths += [os.path.join(OFFICIAL_COURSES_DIR, name) for name in os.listdir(OFFICIAL_COURSES_DIR) if name.endswith(".json")]
    last_modified = max(os.path.getmtime(item) for item in content_paths if os.path.exists(item))
    lastmod = dt.datetime.fromtimestamp(last_modified, tz=dt.timezone.utc).date().isoformat()
    urls = "".join(f"<url><loc>{escape(base_url + path)}</loc><lastmod>{lastmod}</lastmod></url>" for path in dict.fromkeys(paths))
    return Response(content=f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>', media_type="application/xml")


def _support_form_context(request: Request) -> dict:
    """Préremplit l'adresse de contact du compte connecté."""
    prefill_email = ""
    user_id = request.session.get("user_id")
    if user_id:
        db = SessionLocal()
        try:
            user = db.get(User, int(user_id))
            if user:
                prefill_email = user.email or ""
        finally:
            db.close()
    return {
        "request": request,
        "support_status": request.query_params.get("status", ""),
        "prefill_email": prefill_email,
    }


@app.get("/conditions-utilisation", response_class=HTMLResponse)
def ui_terms_of_use(request: Request):
    return templates.TemplateResponse("terms.html", {"request": request})


@app.get("/aide", response_class=HTMLResponse)
def ui_help(request: Request):
    return templates.TemplateResponse("help.html", {"request": request})


@app.get("/assistance", response_class=HTMLResponse)
def ui_support_form(request: Request):
    guard = _guard_user_route(request)
    if guard:
        return guard
    return templates.TemplateResponse("support.html", _support_form_context(request))


@app.post("/assistance", response_class=HTMLResponse)
def ui_support_submit(
    request: Request,
    email: str = Form(""),
    category: str = Form(""),
    subject: str = Form(""),
    message: str = Form(""),
    name: str = Form(""),
    website: str = Form(""),
):
    """Transmet une demande d'assistance à la boîte configurée de l'équipe."""
    guard = _guard_user_route(request)
    if guard:
        return guard

    user_id = _get_session_user_id(request)
    db = SessionLocal()
    try:
        user = db.get(User, user_id)
        account_email = (user.email if user else "").strip().lower()
    finally:
        db.close()
    if not account_email:
        request.session.clear()
        return RedirectResponse(url="/ui/login", status_code=303)

    # Champ invisible : les robots le renseignent souvent, les utilisateurs non.
    if website.strip():
        return RedirectResponse(url="/assistance?status=sent", status_code=303)

    # L'adresse de réponse est celle du compte authentifié, pas une valeur libre du formulaire.
    clean_email = account_email
    clean_name = re.sub(r"\s+", " ", name.strip())[:120]
    clean_category = category.strip().lower()
    clean_subject = re.sub(r"[\r\n]+", " ", subject.strip())[:180]
    clean_message = message.strip()[:6000]
    allowed_categories = {"compte", "strava", "course", "plan", "donnees", "autre"}

    if (
        not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", clean_email)
        or clean_category not in allowed_categories
        or len(clean_subject) < 3
        or len(clean_message) < 10
    ):
        return RedirectResponse(url="/assistance?status=invalid", status_code=303)

    support_recipient = settings.SUPPORT_EMAIL or settings.SMTP_FROM_EMAIL or settings.SMTP_USER
    if not support_recipient:
        logger.error("[SUPPORT] SUPPORT_EMAIL / SMTP_FROM_EMAIL non configuré")
        return RedirectResponse(url="/assistance?status=unavailable", status_code=303)

    page = request.headers.get("referer", "")[:500]
    body = (
        "Nouvelle demande d'assistance — Running Data Plan\n\n"
        f"Catégorie : {clean_category}\n"
        f"Nom : {clean_name or 'Non renseigné'}\n"
        f"Email de réponse : {clean_email}\n"
        f"Page d'origine : {page or 'Non renseignée'}\n\n"
        "Message :\n"
        f"{clean_message}\n"
    )
    try:
        _send_plain_email(
            recipients=[support_recipient],
            subject=f"[Assistance] {clean_category} — {clean_subject}",
            body=body,
            include_login_footer=False,
        )
    except Exception:
        logger.exception("[SUPPORT] Impossible d'envoyer la demande d'assistance")
        return RedirectResponse(url="/assistance?status=unavailable", status_code=303)

    return RedirectResponse(url="/assistance?status=sent", status_code=303)


@app.get("/logout")
def ui_logout(request: Request):
    """
    Déconnecte l'utilisateur courant en vidant la session, puis renvoie vers la page de connexion.
    """
    request.session.clear()
    return RedirectResponse(url="/ui/login", status_code=303)

@app.post("/ui/enrich-last", response_class=HTMLResponse)
async def ui_enrich_last(request: Request, user_id: int = Form(...)):
    """
    Action depuis l'interface web :
    - récupère la dernière activité Strava pour ce user
    - lance enrich_activity dessus
    - affiche un petit message de résultat
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    cli = StravaClient(user_id=user_id)
    try:
        acts = await cli.list_activities(per_page=1)
    except Exception as e:
        return HTMLResponse(
            f"<h1>Erreur Strava</h1><p>{e}</p><p><a href='/ui'>Retour</a></p>",
            status_code=500,
        )

    if not acts:
        return HTMLResponse(
            "<h1>Aucune activité</h1><p>Aucune activité trouvée pour ce compte Strava.</p>"
            "<p><a href='/ui'>Retour</a></p>",
            status_code=200,
        )
    activity = acts[0]
    activity_id = activity["id"]
    name = activity.get("name", "")
    start_date = activity.get("start_date", "")

    try:
        result = await request_activity_enrichment(
            int(activity_id),
            user_id=user_id,
            trigger_source="manual_ui",
            immediate=True,
        )
        if result.get("status") == "succeeded":
            msg = "Activité enrichie avec succès 🎉"
        elif result.get("status") == "deferred":
            msg = "Enrichissement différé : une reprise automatique est programmée."
        else:
            msg = "Activité traitée, mais aucun enrichissement Strava n'a été publié."
    except Exception as e:
        msg = f"Erreur lors de l'enrichissement : {e}"

    return templates.TemplateResponse(
        "enrich_last.html",
        {
            "request": request,
            "user_id": user_id,
            "activity_id": activity_id,
            "name": name,
            "start_date": start_date,
            "message": msg,
        },
    )


# --------------------------------------------------------------------------
# Formulaire d'inscription (UI)
# --------------------------------------------------------------------------

@app.get("/ui/signup", response_class=HTMLResponse)
def ui_signup_form(request: Request):
    """
    Formulaire d'inscription utilisateur (UI).
    """
    return templates.TemplateResponse(
        "signup.html",
        {"request": request}
    )


@app.post("/ui/signup", response_class=HTMLResponse)
def ui_signup(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    first_name: str = Form(""),
    last_name: str = Form(""),
    location: str = Form(""),
):
    """
    Traite le formulaire de signup :
    - crée un utilisateur en base avec prénom, nom, localisation
    - redirige vers son dashboard
    """
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.email == email).first()
        if existing:
            html = """
            <html>
              <head>
                <meta charset="utf-8">
                <title>Création de compte - Erreur</title>
              </head>
              <body>
                <h1>Créer un compte</h1>
                <p style="color:red;">Un utilisateur avec cet email existe déjà.</p>
                <a href="/ui/signup">Réessayer</a> · <a href="/ui/login">Se connecter</a>
              </body>
            </html>
            """
            return HTMLResponse(content=html, status_code=400)

        password_hash = pwd_context.hash(password)
        user = User(
            email=email,
            password_hash=password_hash,
            first_name=first_name or None,
            last_name=last_name or None,
            location=location or None,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    finally:
        db.close()

    # 👉 Après inscription, on passe par une page "welcome" qui propose Strava
    request.session["user_id"] = int(user.id)
    return RedirectResponse(url=f"/ui/user/{user.id}/welcome", status_code=302)


@app.get("/ui/user/{user_id}/welcome", response_class=HTMLResponse)
def ui_user_welcome(user_id: int, request: Request):
    """
    Écran d'accueil juste après inscription :
    - propose de connecter Strava
    - ou de passer et aller au dashboard
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        has_strava = bool(user.strava_tokens)

    finally:
        db.close()

    return templates.TemplateResponse(
        "welcome_strava.html",
        {
            "request": request,
            "user": user,
            "has_strava": has_strava,
        },
    )


# -----------------------------------------------------------------------------
# UI : Profil utilisateur
# -----------------------------------------------------------------------------

from app.models import User, UserSettings  # make sure this import exists

@app.get("/ui/user/{user_id}/profile", response_class=HTMLResponse)
def ui_user_profile(user_id: int, request: Request):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        _maybe_refresh_glucose_for_page_view(db, user, page_name="profile")

        # Statuts connexions
        has_strava = bool(user.strava_tokens)
        has_libre = user.libre_credentials is not None
        dexcom_record = _get_dexcom_share_record(user.dexcom_tokens)
        has_dexcom = dexcom_record is not None
        carelink_record = user.carelink_credentials
        has_carelink = bool(carelink_record and carelink_record.username)
        nightscout_record = user.nightscout_credentials
        has_nightscout = bool(nightscout_record and nightscout_record.base_url)

        libre_email = user.libre_credentials.email if user.libre_credentials else ""
        libre_region = user.libre_credentials.region if user.libre_credentials else ""
        dexcom_username = dexcom_record.share_username if dexcom_record else ""
        dexcom_region = dexcom_record.share_region if dexcom_record else settings.DEXCOM_SHARE_REGION_DEFAULT
        carelink_username = carelink_record.username if carelink_record else ""
        carelink_region = carelink_record.region if carelink_record else "EU"
        nightscout_url = nightscout_record.base_url if nightscout_record else ""
        nightscout_token_configured = bool(nightscout_record and nightscout_record.read_token_encrypted)
        carelink_status = request.query_params.get("carelink_status") or (carelink_record.status if carelink_record else None)
        carelink_status_message = request.query_params.get("carelink_msg") or (carelink_record.error_message if carelink_record else None)
        carelink_last_sync_at = carelink_record.last_sync_at if carelink_record else None
        nightscout_status = request.query_params.get("nightscout_status")
        nightscout_status_message = request.query_params.get("nightscout_msg") or (
            nightscout_record.last_error_message if nightscout_record and not request.query_params.get("nightscout_status") else None
        )

        strava_athlete_id = user.strava_tokens[0].athlete_id if user.strava_tokens else None
        glucose_provider = get_active_glucose_source(user) or ""
        glucose_source_active_label = get_glucose_source_label(glucose_provider)

        # ✅ EAGER: lire les préférences et créer un bool simple
        user_settings = db.query(UserSettings).filter(UserSettings.user_id == user_id).one_or_none()
        if user_settings and user_settings.desc_enable_auto_block is not None:
            auto_block_enabled = bool(user_settings.desc_enable_auto_block)
        else:
            auto_block_enabled = True
        share_show_club_logo = bool(
            user_settings.share_show_club_logo
        ) if user_settings and user_settings.share_show_club_logo is not None else False

        club_options = get_available_clubs()
        selected_club = build_club_payload(user.club_slug)

        libre_status = request.query_params.get("libre_status")
        libre_status_message = request.query_params.get("libre_msg")
        if libre_status is None and has_libre:
            status_flag = get_last_libre_status(user_id)
            if status_flag:
                libre_status, libre_status_message = status_flag

        dexcom_status = request.query_params.get("dexcom_status")
        dexcom_status_message = request.query_params.get("dexcom_msg")
        if dexcom_status is None and has_dexcom:
            status_flag = get_last_dexcom_status(user_id)
            if status_flag:
                dexcom_status, dexcom_status_message = status_flag

        plan_payment_enabled = _payment_pilot_allowed(user_id)
        plan_credits = _get_plan_credit_wallet(db, user_id).credits if plan_payment_enabled else 0
        has_purchased_plan = _has_purchased_individual_plan(db, user_id) if plan_payment_enabled else False
        recent_plan_downloads = (
            db.query(CoursePlanDownload)
            .filter(CoursePlanDownload.user_id == user_id)
            .order_by(CoursePlanDownload.downloaded_at.desc(), CoursePlanDownload.id.desc())
            .limit(4)
            .all()
        ) if plan_payment_enabled else []

        # On rend la page en passant des primitives (pas d’accès lazy après fermeture)
        ctx = {
            "request": request,
            "user": user,
            "has_strava": has_strava,
            "has_libre": has_libre,
            "has_dexcom": has_dexcom,
            "has_carelink": has_carelink,
            "has_nightscout": has_nightscout,
            "libre_email": libre_email,
            "libre_region": libre_region,
            "dexcom_username": dexcom_username,
            "dexcom_region": dexcom_region,
            "carelink_username": carelink_username,
            "carelink_region": carelink_region,
            "carelink_last_sync_at": carelink_last_sync_at,
            "nightscout_url": nightscout_url,
            "nightscout_token_configured": nightscout_token_configured,
            "strava_athlete_id": strava_athlete_id,
            "glucose_provider": glucose_provider,
            "glucose_source_active_label": glucose_source_active_label,
            "auto_block_enabled": auto_block_enabled,
            "club_options": club_options,
            "selected_club": selected_club,
            "share_show_club_logo": share_show_club_logo,
            "libre_status": libre_status,
            "libre_status_message": libre_status_message,
            "dexcom_status": dexcom_status,
            "dexcom_status_message": dexcom_status_message,
            "carelink_status": carelink_status,
            "carelink_status_message": carelink_status_message,
            "nightscout_status": nightscout_status,
            "nightscout_status_message": nightscout_status_message,
            "plan_payment_enabled": plan_payment_enabled,
            "plan_credits": plan_credits,
            "has_purchased_plan": has_purchased_plan,
            "recent_plan_downloads": recent_plan_downloads,
        }
        return templates.TemplateResponse("user_profile.html", ctx)

    finally:
        db.close()


@app.post("/ui/user/{user_id}/profile", response_class=HTMLResponse)
def ui_user_profile_update(
    request: Request,
    user_id: int,
    first_name: str = Form(""),
    last_name: str = Form(""),
    location: str = Form(""),
    birthdate: str = Form(""),       # "YYYY-MM-DD" ou vide
    sex: str = Form(""),
    max_heartrate: str = Form(""),   # on parse en int si non vide
    height_cm: str = Form(""),       # pareil en float
    weight_kg: str = Form(""),
    is_pro: bool = Form(False),      # checkbox pro
    club_slug: str = Form(""),
    glucose_provider: str = Form(""),      # rétro-compat UI historique
    profile_image: UploadFile | None = File(None),  # 👈 fichier uploadé

    desc_enable_auto_block: str | None = Form(None),
    share_show_club_logo: str | None = Form(None),
):
    """
    Traite le formulaire de profil utilisateur :
    - met à jour les infos de base
    - met à jour les infos physiologiques
    - met à jour le statut is_pro
    - met à jour la préférence de source CGM (cgm_source)
    - gère l'upload de la photo de profil (stockée dans static/avatars)
    - met à jour les préférences de description Strava (gly/VAM/pace/cadence)
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        # -------- Profil : champs texte simples --------
        user.first_name = first_name or None
        user.last_name  = last_name or None
        user.location   = location or None
        user.sex        = sex or None

        # -------- Date de naissance --------
        if birthdate:
            try:
                user.birthdate = dt.date.fromisoformat(birthdate)
            except ValueError:
                pass
        else:
            user.birthdate = None

        # -------- FC max --------
        if max_heartrate:
            try:
                user.max_heartrate = int(max_heartrate)
            except ValueError:
                pass
        else:
            user.max_heartrate = None

        # -------- Taille --------
        if height_cm:
            try:
                user.height_cm = float(height_cm)
            except ValueError:
                pass
        else:
            user.height_cm = None

        # -------- Poids --------
        if weight_kg:
            try:
                user.weight_kg = float(weight_kg)
            except ValueError:
                pass
        else:
            user.weight_kg = None

        # -------- Abonnement pro --------
        user.is_pro = bool(is_pro)

        # -------- Club --------
        club_slug_value = (club_slug or "").strip().lower()
        user.club_slug = club_slug_value if get_club_by_slug(club_slug_value, include_inactive=False) else None

        # -------- Source CGM --------
        provider_val = (glucose_provider or "").strip().lower()
        if provider_val in ("abbott", "dexcom", "medtronic_carelink", "nightscout"):
            set_active_glucose_source(user, provider_val)

        # -------- Upload avatar --------
        if profile_image and profile_image.filename:
            valid_exts = {".jpg", ".jpeg", ".png", ".gif", ".webp"}
            _, ext = os.path.splitext(profile_image.filename)
            ext = ext.lower()
            if ext in valid_exts:
                avatars_dir = os.path.join("static", "avatars")
                os.makedirs(avatars_dir, exist_ok=True)
                filename = f"user_{user_id}{ext}"
                file_path = os.path.join(avatars_dir, filename)
                with open(file_path, "wb") as buffer:
                    shutil.copyfileobj(profile_image.file, buffer)
                user.profile_image_url = f"/static/avatars/{filename}"
            else:
                print(f"[PROFILE] Extension de fichier non supportée : {ext}")

        # -------- NEW : upsert UserSettings --------
        settings = db.query(UserSettings).filter(UserSettings.user_id == user_id).one_or_none()
        if settings is None:
            settings = UserSettings(user_id=user_id)
            db.add(settings)

        settings.desc_enable_auto_block = bool(desc_enable_auto_block)
        settings.share_show_club_logo = bool(share_show_club_logo)

        db.commit()
        db.refresh(user)

    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile",
        status_code=302,
    )



@app.get("/ui/user/{user_id}/runner-profile", response_class=HTMLResponse)
def ui_runner_profile(
    request: Request,
    user_id: int,
    sport: str = Query("run"),
    period: str = Query("all"),           # "all" ou "last_12_months"
    tab: str = Query("ascent"),         # "ascent", "vam", "pace", ...
    db: Session = Depends(get_db),
):
    """
    Page 'profil coureur' :
    - tab=overview  : profil cardio × pente × VAM × allure × cadence
    """

    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    # 1) Récupérer l'utilisateur
    user = db.query(models.User).get(user_id)
    if user is None:
        return HTMLResponse(status_code=404, content="User not found")

    # 2) Déterminer la période
    now_utc = dt.datetime.utcnow()

    date_from = None
    date_to = None

    period_windows = {
        "last_12_months": 365,
        "last_6_months": 183,
        "last_3_months": 92,
        "last_1_month": 31,
    }
    days_window = period_windows.get(period)
    if days_window:
        date_from = now_utc - dt.timedelta(days=days_window)
        # date_to reste None => jusqu’à maintenant

    # Les totaux sportifs sont archivés séparément des streams détaillés. Ce
    # rattrapage léger protège également les activités créées avant ce système.
    activities_to_archive = (
        db.query(models.Activity)
        .filter(models.Activity.user_id == user_id)
        .filter(sport_column_condition(models.Activity.sport, sport))
        .all()
    )
    archived_any = False
    for activity_to_archive in activities_to_archive:
        archived_any = ensure_activity_meta_contribution(db, activity_to_archive) or archived_any
    if archived_any:
        db.commit()
    archived_training_summary = get_archived_training_summary(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )

    # Migration progressive des anciennes activités : six sorties sont
    # corrigées à chaque ouverture, sans bloquer longtemps le tableau de bord.
    legacy_signed_vam_ids = (
        db.query(models.Activity.id)
        .join(
            models.ActivityStreamPoint,
            models.ActivityStreamPoint.activity_id == models.Activity.id,
        )
        .filter(models.Activity.user_id == user_id)
        .filter(sport_column_condition(models.Activity.sport, sport))
        .filter(models.ActivityStreamPoint.slope_percent < -1)
        .filter(models.ActivityStreamPoint.velocity.isnot(None))
        .filter(func.abs(models.ActivityStreamPoint.velocity * models.ActivityStreamPoint.slope_percent * 36.0) <= 4000)
        .filter(
            (models.ActivityStreamPoint.vertical_speed_m_per_h.is_(None))
            | (models.ActivityStreamPoint.vertical_speed_m_per_h >= 0)
        )
        .distinct()
        .limit(6)
        .all()
    )
    for (legacy_activity_id,) in legacy_signed_vam_ids:
        legacy_activity = db.query(models.Activity).get(legacy_activity_id)
        if legacy_activity is None:
            continue
        try:
            updated_points = backfill_signed_vertical_speed_for_activity(db, legacy_activity)
            if updated_points:
                compute_and_store_zone_slope_aggs(db, legacy_activity, user_id)
                update_runner_profile_monthly_from_activity(db=db, activity=legacy_activity)
        except Exception:
            db.rollback()
            logger.exception(
                "[RUNNER_PROFILE][signed_vam_backfill] activity_id=%s",
                legacy_activity_id,
            )

    # 3) Profil coureur (zones × pente)
    profile_start = time.perf_counter()
    profile = get_cached_runner_profile(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )
    if not profile or not profile.get("zones"):
        logger.warning(
            "[RUNNER_PROFILE][cache_miss] user_id=%s sport=%s period=%s → recalcul complet",
            user_id,
            sport,
            period,
        )
        rebuilt_months = rebuild_runner_profile_range_from_contributions(
            db,
            user_id=user_id,
            sport=sport,
            date_from=date_from,
            date_to=date_to,
        )
        if rebuilt_months:
            profile = get_cached_runner_profile(
                db,
                user_id=user_id,
                sport=sport,
                date_from=date_from,
                date_to=date_to,
            )
        if profile and profile.get("zones"):
            logger.info(
                "[RUNNER_PROFILE][cache_rebuild] user_id=%s sport=%s rebuilt_months=%s",
                user_id,
                sport,
                rebuilt_months,
            )
        else:
            profile = build_runner_profile(
                db,
                user_id=user_id,
                sport=sport,
                date_from=date_from,
                date_to=date_to,
            )
    logger.info(
        "[RUNNER_PROFILE][timing] profile_lookup user_id=%s sport=%s took=%.3fs",
        user_id,
        sport,
        time.perf_counter() - profile_start,
    )

    # 4) Ordre des zones cardio + pentes (positives et négatives)
    hr_zone_names = [name for (name, _, _) in HR_ZONES]
    pace_lookup_by_slope = _build_pace_lookup_from_profile(profile, hr_zone_names)
    slopes_order = SLOPE_ORDER

    libre_connected = (
        db.query(LibreCredentials.id).filter(LibreCredentials.user_id == user_id).first() is not None
    )
    dexcom_connected = has_dexcom_share_credentials(
        db.query(DexcomToken).filter(DexcomToken.user_id == user_id).all()
    )
    carelink_connected = (
        db.query(CareLinkCredential.id).filter(CareLinkCredential.user_id == user_id).first() is not None
    )
    nightscout_connected = (
        db.query(NightscoutCredential.id).filter(NightscoutCredential.user_id == user_id).first() is not None
    )
    show_glucose_tabs = libre_connected or dexcom_connected or carelink_connected or nightscout_connected

    glucose_zone_summary = []
    glucose_chart_24h = []
    glucose_dashboard_metrics = {
        "has_data": False,
        "latest_mgdl": None,
        "latest_label": "—",
        "delta_mgdl": None,
        "trend_label": "Données insuffisantes",
        "trend_tone": "neutral",
        "average_mgdl": None,
        "variability_pct": None,
        "minimum_mgdl": None,
        "maximum_mgdl": None,
        "time_in_range_pct": None,
        "low_pct": None,
        "high_pct": None,
    }
    recent_glucose_activities = []
    glucose_activity_chart = {"labels": [], "start": [], "avg": [], "tir": []}
    glucose_activity_profile_radar = None

    if not show_glucose_tabs and tab in {"glucose", "glucose_activities"}:
        tab = "ascent"

    if show_glucose_tabs:
        glucose_start = time.perf_counter()
            # 5) Stats glycémie (temps passé dans les zones sur différentes fenêtres)
        glucose_zone_defs = [
            ("G1", "Zone 1", "Hypo", "< 70 mg/dL", None, 70),
            ("G2", "Zone 2", "Bas", "70–100 mg/dL", 70, 100),
            ("G3", "Zone 3", "Cible basse", "100–140 mg/dL", 100, 140),
            ("G4", "Zone 4", "Cible haute", "140–180 mg/dL", 140, 180),
            ("G5", "Zone 5", "Élevée", "> 180 mg/dL", 180, None),
        ]

        def _format_duration_local(sec: float) -> str:
            s = int(round(sec))
            if s <= 0:
                return "–"
            h = s // 3600
            m = (s % 3600) // 60
            if h > 0:
                return f"{h}h{m:02d}"
            if m > 0:
                return f"{m} min"
            return f"{s}s"

        def _compute_glucose_zones(duration_days: int):
            start_ts = now_utc - dt.timedelta(days=duration_days)
            points = (
                db.query(GlucosePoint)
                .filter(GlucosePoint.user_id == user_id)
                .filter(GlucosePoint.ts >= start_ts)
                .order_by(GlucosePoint.ts.asc())
                .all()
            )

            valid_points = [p for p in points if p.mgdl is not None and p.ts is not None]
            zone_time = {zid: 0.0 for (zid, *_rest) in glucose_zone_defs}

            def find_zone_id(glu: float | None) -> str | None:
                if glu is None:
                    return None
                for zid, _name, _desc, _range_label, zmin, zmax in glucose_zone_defs:
                    if (zmin is None or glu >= zmin) and (zmax is None or glu < zmax):
                        return zid
                return None

            for i in range(len(valid_points) - 1):
                curr = valid_points[i]
                nxt = valid_points[i + 1]
                dt_seconds = (nxt.ts - curr.ts).total_seconds()
                if dt_seconds <= 0:
                    continue
                zid = find_zone_id(curr.mgdl)
                if not zid:
                    continue
                zone_time[zid] += dt_seconds

            total = sum(zone_time.values())
            rows = []
            for zid, name, desc, range_label, _zmin, _zmax in glucose_zone_defs:
                t = zone_time.get(zid, 0.0)
                pct = round(t * 100.0 / total) if total > 0 else 0
                rows.append(
                    {
                        "id": zid,
                        "name": name,
                        "description": desc,
                        "range": range_label,
                        "time_sec": t,
                        "time_str": _format_duration_local(t),
                        "percent": pct,
                    }
                )

            avg_mgdl = None
            if valid_points:
                avg_mgdl = sum(float(p.mgdl) for p in valid_points) / len(valid_points)

            return {
                "rows": rows,
                "has_data": total > 0,
                "total_time_str": _format_duration_local(total),
                "avg_mgdl": avg_mgdl,
            }

        glucose_zone_summary = [
            {
                "key": "1d",
                "label": "Dernières 24 h",
                **_compute_glucose_zones(1),
            },
            {
                "key": "7d",
                "label": "7 derniers jours",
                **_compute_glucose_zones(7),
            },
            {
                "key": "14d",
                "label": "14 derniers jours",
                **_compute_glucose_zones(14),
            },
        ]

        # Série temporelle détaillée sur 24h pour affichage graphique
        points_24h = (
            db.query(GlucosePoint)
            .filter(GlucosePoint.user_id == user_id)
            .filter(GlucosePoint.ts >= now_utc - dt.timedelta(days=1))
            .order_by(GlucosePoint.ts.asc())
            .all()
        )

        def _ts_iso(ts: dt.datetime | None) -> str | None:
            if ts is None:
                return None
            aware = _safe_dt(ts)
            if aware.tzinfo is None:
                aware = aware.replace(tzinfo=dt.timezone.utc)
            return aware.isoformat()

        valid_points_24h = [p for p in points_24h if p.mgdl is not None and p.ts is not None]
        glucose_chart_24h = [
            {
                "ts": _ts_iso(p.ts),
                "mgdl": float(p.mgdl),
            }
            for p in valid_points_24h
        ]

        if glucose_chart_24h:
            values_24h = [float(point["mgdl"]) for point in glucose_chart_24h]
            latest_point = glucose_chart_24h[-1]
            latest_dt = _safe_dt(valid_points_24h[-1].ts) if valid_points_24h else now_utc
            freshness_minutes = max(0, int((_safe_dt(now_utc) - latest_dt).total_seconds() // 60))
            delta_mgdl = None
            trend_label = "Stable"
            trend_tone = "stable"
            if len(values_24h) >= 2:
                delta_mgdl = values_24h[-1] - values_24h[-2]
                if delta_mgdl >= 12:
                    trend_label, trend_tone = "En hausse", "up"
                elif delta_mgdl <= -12:
                    trend_label, trend_tone = "En baisse", "down"
            first_summary = glucose_zone_summary[0] if glucose_zone_summary else {}
            rows_by_id = {row["id"]: row for row in first_summary.get("rows", [])}
            time_in_range_pct = sum(rows_by_id.get(zone, {}).get("percent", 0) for zone in ("G2", "G3", "G4"))
            glucose_dashboard_metrics = {
                "has_data": True,
                "latest_mgdl": round(values_24h[-1]),
                "latest_label": "à l’instant" if freshness_minutes <= 5 else f"il y a {freshness_minutes} min",
                "delta_mgdl": round(delta_mgdl) if delta_mgdl is not None else None,
                "trend_label": trend_label,
                "trend_tone": trend_tone,
                "average_mgdl": round(statistics.mean(values_24h)),
                "variability_pct": round((statistics.pstdev(values_24h) / statistics.mean(values_24h)) * 100) if len(values_24h) > 1 and statistics.mean(values_24h) else None,
                "minimum_mgdl": round(min(values_24h)),
                "maximum_mgdl": round(max(values_24h)),
                "time_in_range_pct": time_in_range_pct,
                "low_pct": rows_by_id.get("G1", {}).get("percent", 0),
                "high_pct": rows_by_id.get("G5", {}).get("percent", 0),
            }

        # 6bis) Historique glycémie par activité (20 dernières activités avec données)
        recent_glucose_activities = []
        glucose_activity_chart = {
            "labels": [],
            "start": [],
            "avg": [],
            "tir": [],
        }
        def _format_activity_date(ts: dt.datetime | None, short: bool = False) -> str:
            if ts is None:
                return "?"
            aware = _safe_dt(ts)
            if aware.tzinfo is None:
                aware = aware.replace(tzinfo=dt.timezone.utc)
            return aware.strftime("%d/%m") if short else aware.strftime("%d %b %Y · %H:%M")

        glucose_activity_profile_radar = {
            "labels": ["Endurance", "Seuil", "Fractionné"],
            "values": [None, None, None],
            "counts": [0, 0, 0],
            "has_data": False,
        }

        cached_glucose_summary = get_cached_glucose_activity_summary(
            db,
            user_id=user_id,
            sport=sport,
            limit=20,
        )

        if cached_glucose_summary and cached_glucose_summary.get("activities"):
            cached_activities = cached_glucose_summary["activities"]
            profile_stats = cached_glucose_summary.get("profile_stats") or {}

            for entry in cached_activities:
                start_dt = entry.get("start_ts")
                start_label = _format_activity_date(start_dt)
                duration_sec = entry.get("duration_sec")
                recent_glucose_activities.append(
                    {
                        "id": entry.get("activity_id"),
                        "name": entry.get("name") or f"Activité {entry.get('activity_id')}",
                        "start_label": start_label,
                        "distance_km": entry.get("distance_km"),
                        "elevation_gain_m": entry.get("elevation_gain_m"),
                        "duration_str": _format_duration(duration_sec) if duration_sec else None,
                        "start_glucose": entry.get("start_mgdl"),
                        "avg_glucose": entry.get("avg_mgdl"),
                        "tir_percent": entry.get("tir_percent"),
                    }
                )

                glucose_activity_chart["labels"].append(_format_activity_date(start_dt, short=True))
                glucose_activity_chart["start"].append(entry.get("start_mgdl"))
                glucose_activity_chart["avg"].append(entry.get("avg_mgdl"))
                glucose_activity_chart["tir"].append(entry.get("tir_percent"))

            radar_labels = ["Endurance", "Seuil", "Fractionné"]
            radar_values = []
            radar_counts = []
            has_data = False
            order = [("endurance", "Endurance"), ("seuil", "Seuil"), ("fractionne", "Fractionné")]
            for key, label in order:
                stats = profile_stats.get(key) or {}
                count = int(stats.get("count") or 0)
                avg_val = None
                if count > 0:
                    avg_val = (stats.get("sum_avg_mgdl") or 0.0) / count
                    has_data = True
                radar_values.append(avg_val)
                radar_counts.append(count)
            glucose_activity_profile_radar = {
                "labels": radar_labels,
                "values": radar_values,
                "counts": radar_counts,
                "has_data": has_data,
            }
        else:
            sport = canonicalize_sport_label(sport)
            activities_with_glucose = (
                db.query(models.Activity)
                .filter(models.Activity.user_id == user_id)
                .filter(sport_column_condition(models.Activity.sport, sport))
                .order_by(models.Activity.start_date.desc())
                .limit(20)
                .all()
            )

            if activities_with_glucose:
                activity_ids = [a.id for a in activities_with_glucose]

                subq = (
                    db.query(
                        ActivityStreamPoint.activity_id.label("activity_id"),
                        func.min(ActivityStreamPoint.elapsed_time).label("min_elapsed"),
                    )
                    .filter(ActivityStreamPoint.activity_id.in_(activity_ids))
                    .filter(ActivityStreamPoint.glucose_mgdl.isnot(None))
                    .group_by(ActivityStreamPoint.activity_id)
                    .subquery()
                )

                start_points = {}
                if activity_ids:
                    rows = (
                        db.query(
                            ActivityStreamPoint.activity_id,
                            ActivityStreamPoint.glucose_mgdl,
                        )
                        .join(
                            subq,
                            and_(
                                ActivityStreamPoint.activity_id == subq.c.activity_id,
                                ActivityStreamPoint.elapsed_time == subq.c.min_elapsed,
                            ),
                        )
                        .all()
                    )
                    for row in rows:
                        if row.glucose_mgdl is not None:
                            start_points[row.activity_id] = float(row.glucose_mgdl)

                zone_mix: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
                if activity_ids:
                    zone_rows = (
                        db.query(
                            ActivityZoneSlopeAgg.activity_id,
                            ActivityZoneSlopeAgg.hr_zone,
                            func.sum(ActivityZoneSlopeAgg.duration_sec).label("duration_sec"),
                        )
                        .filter(ActivityZoneSlopeAgg.activity_id.in_(activity_ids))
                        .group_by(ActivityZoneSlopeAgg.activity_id, ActivityZoneSlopeAgg.hr_zone)
                        .all()
                    )

                    for row in zone_rows:
                        zone_mix[row.activity_id][row.hr_zone] += float(row.duration_sec or 0)

                radar_acc = {
                    "endurance": {"label": "Endurance", "sum": 0.0, "count": 0},
                    "seuil": {"label": "Seuil", "sum": 0.0, "count": 0},
                    "fractionne": {"label": "Fractionné", "sum": 0.0, "count": 0},
                }

                for activity in activities_with_glucose:
                    start_dt = _safe_dt(activity.start_date)
                    distance_km = float(activity.distance) / 1000.0 if activity.distance else None
                    elevation_gain = float(activity.total_elevation_gain) if activity.total_elevation_gain else None
                    recent_glucose_activities.append(
                        {
                            "id": activity.id,
                            "name": activity.name or f"Activité {activity.id}",
                            "start_label": _format_activity_date(start_dt),
                            "distance_km": distance_km,
                            "elevation_gain_m": elevation_gain,
                            "duration_str": _format_duration(activity.elapsed_time) if activity.elapsed_time else None,
                            "start_glucose": start_points.get(activity.id),
                            "avg_glucose": float(activity.avg_glucose) if activity.avg_glucose is not None else None,
                            "tir_percent": float(activity.time_in_range_percent) if activity.time_in_range_percent is not None else None,
                        }
                    )

                for activity in reversed(activities_with_glucose):
                    start_dt = _safe_dt(activity.start_date)
                    glucose_activity_chart["labels"].append(_format_activity_date(start_dt, short=True))
                    glucose_activity_chart["start"].append(start_points.get(activity.id))
                    glucose_activity_chart["avg"].append(
                        float(activity.avg_glucose) if activity.avg_glucose is not None else None
                    )
                    glucose_activity_chart["tir"].append(
                        float(activity.time_in_range_percent) if activity.time_in_range_percent is not None else None
                    )

                    avg_glucose = activity.avg_glucose
                    if avg_glucose is None:
                        continue
                    zone_distribution = zone_mix.get(activity.id) or {}
                    profile_key = _classify_activity_profile(zone_distribution)
                    if not profile_key:
                        continue
                    bucket = radar_acc.get(profile_key)
                    if not bucket:
                        continue
                    bucket["sum"] += float(avg_glucose)
                    bucket["count"] += 1

                radar_labels = [radar_acc[k]["label"] for k in ("endurance", "seuil", "fractionne")]
                radar_values = []
                radar_counts = []
                has_data = False
                for key in ("endurance", "seuil", "fractionne"):
                    bucket = radar_acc[key]
                    avg_val = None
                    if bucket["count"] > 0:
                        avg_val = bucket["sum"] / bucket["count"]
                        has_data = True
                    radar_values.append(avg_val)
                    radar_counts.append(bucket["count"])

                glucose_activity_profile_radar = {
                    "labels": radar_labels,
                    "values": radar_values,
                    "counts": radar_counts,
                    "has_data": has_data,
                }
        logger.info(
            "[RUNNER_PROFILE][timing] glucose_blocks user_id=%s sport=%s took=%.3fs",
            user_id,
            sport,
            time.perf_counter() - glucose_start,
        )
    else:
        logger.info(
            "[RUNNER_PROFILE][timing] glucose_blocks user_id=%s skipped (no CGM)",
            user_id,
        )

    # 6) D+ max sur fenêtres glissantes (lecture cache uniquement)
    dplus_start = time.perf_counter()
    best_dplus_windows = get_cached_dplus_windows(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )
    logger.info(
        "[RUNNER_PROFILE][timing] dplus_windows user_id=%s sport=%s took=%.3fs",
        user_id,
        sport,
        time.perf_counter() - dplus_start,
    )

    series_matrix = get_series_splits_matrix(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )
    distance_efforts = get_cached_distance_efforts(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )
    volume_weekly_summary = get_cached_volume_weekly_summary(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )
    distance_projections = compute_distance_projections(series_matrix, distance_efforts)

    drift_activities = (
        db.query(models.Activity)
        .filter(models.Activity.user_id == user_id)
        .filter(models.Activity.elapsed_time >= 2700)
        .filter(sport_column_condition(models.Activity.sport, sport))
        .order_by(models.Activity.start_date.desc())
        .limit(24)
        .all()
    )
    cardiac_drift_rows = []
    for drift_activity in drift_activities:
        drift_points = (
            db.query(models.ActivityStreamPoint)
            .filter(models.ActivityStreamPoint.activity_id == drift_activity.id)
            .order_by(models.ActivityStreamPoint.idx.asc())
            .all()
        )
        drift_metric = compute_terrain_adjusted_cardiac_drift(drift_points)
        if drift_metric.get("available"):
            cardiac_drift_rows.append({
                "activity_id": drift_activity.id,
                "name": drift_activity.name or "Sortie sans nom",
                "date": drift_activity.start_date,
                **drift_metric,
            })

    cardiac_drift_history = {"available": False, "rows": [], "count": 0}
    if cardiac_drift_rows:
        recent_rows = cardiac_drift_rows[:10]
        average = sum(row["percent"] for row in recent_rows) / len(recent_rows)
        stable_count = sum(1 for row in recent_rows if row["percent"] < 5)
        trend = None
        if len(recent_rows) >= 6:
            newest = sum(row["percent"] for row in recent_rows[:3]) / 3
            older = sum(row["percent"] for row in recent_rows[3:6]) / 3
            delta = newest - older
            trend = "En amélioration" if delta < -1 else ("À surveiller" if delta > 1 else "Stable")
        cardiac_drift_history = {
            "available": True,
            "rows": recent_rows,
            "count": len(cardiac_drift_rows),
            "average": round(average, 1),
            "stable_pct": round(stable_count / len(recent_rows) * 100),
            "trend": trend or "Première tendance",
        }

    # Le profil est aussi injecté dans un graphique JavaScript avec `tojson`.
    # Les filtres de période ajoutent des datetime Python dans `profile.period`,
    # qui ne sont pas sérialisables directement par Jinja.
    profile_for_template = dict(profile or {})
    profile_period = dict(profile_for_template.get("period") or {})
    for key in ("from", "to"):
        value = profile_period.get(key)
        if isinstance(value, (dt.datetime, dt.date)):
            profile_period[key] = value.isoformat()
    profile_for_template["period"] = profile_period

    # Synthèse lisible du profil : les métriques les plus utiles sont extraites
    # des mêmes cellules que les tableaux détaillés, avec leur couverture.
    profile_zones = profile_for_template.get("zones") or {}
    all_cells = [
        (zone_name, slope_id, cell)
        for zone_name, slopes in profile_zones.items()
        for slope_id, cell in (slopes or {}).items()
        if isinstance(cell, dict)
    ]
    coverage_seconds = sum(float(cell.get("duration_sec") or 0) for _, _, cell in all_cells)

    def _weighted_cell_value(zone_name: str, slope_ids: tuple[str, ...], value_key: str):
        values = []
        for slope_id in slope_ids:
            cell = (profile_zones.get(zone_name) or {}).get(slope_id) or {}
            value = cell.get(value_key)
            duration = float(cell.get("pace_duration_sec") or cell.get("duration_sec") or 0)
            if isinstance(value, (int, float)) and duration > 0:
                values.append((float(value), duration))
        total = sum(duration for _, duration in values)
        return (sum(value * duration for value, duration in values) / total, total) if total else (None, 0)

    endurance_pace, endurance_seconds = _weighted_cell_value(
        "Zone 2", ("Sneg0_5", "S0_5"), "avg_pace_s_per_km"
    )
    endurance_cadence, _ = _weighted_cell_value(
        "Zone 2", ("Sneg0_5", "S0_5"), "avg_cadence_spm"
    )
    climb_cells = [
        (slope_id, float(cell["avg_vam_m_per_h"]), float(cell.get("duration_sec") or 0))
        for zone_name, slope_id, cell in all_cells
        if zone_name in {"Zone 2", "Zone 3", "Zone 4"}
        and slope_id.startswith("S") and not slope_id.startswith("Sneg") and slope_id != "S0_5"
        and isinstance(cell.get("avg_vam_m_per_h"), (int, float))
        and float(cell.get("avg_vam_m_per_h") or 0) > 0
    ]
    descent_cells = [
        (slope_id, float(cell["avg_vam_m_per_h"]), float(cell.get("duration_sec") or 0))
        for zone_name, slope_id, cell in all_cells
        if zone_name in {"Zone 2", "Zone 3", "Zone 4"}
        and slope_id.startswith("Sneg")
        and isinstance(cell.get("avg_vam_m_per_h"), (int, float))
        and float(cell.get("avg_vam_m_per_h") or 0) < 0
    ]
    best_climb = max(climb_cells, key=lambda row: row[1], default=None)
    best_descent = min(descent_cells, key=lambda row: row[1], default=None)
    slope_labels = dict(SLOPE_ORDER)

    def _pace_label(seconds):
        if not seconds:
            return None
        rounded = int(round(seconds))
        return f"{rounded // 60}:{rounded % 60:02d}/km"

    volume_delta_pct = None
    volume_history = (volume_weekly_summary or {}).get("history") or []
    if len(volume_history) >= 2:
        previous_volume = float(volume_history[-2].get("weekly_km") or 0)
        latest_volume = float(volume_history[-1].get("weekly_km") or 0)
        if previous_volume > 0:
            volume_delta_pct = round((latest_volume - previous_volume) / previous_volume * 100)

    confidence = "élevée" if coverage_seconds >= 20 * 3600 else ("moyenne" if coverage_seconds >= 6 * 3600 else "à consolider")
    analytics_insights = []
    if best_climb:
        analytics_insights.append({"tone": "strength", "label": "Point fort", "text": f"Ton meilleur rendement vertical observé se situe sur {slope_labels.get(best_climb[0], best_climb[0])}, à {round(best_climb[1])} m/h."})
    if cardiac_drift_history.get("available"):
        drift_average = cardiac_drift_history["average"]
        analytics_insights.append({
            "tone": "positive" if drift_average < 5 else "watch",
            "label": "Endurance",
            "text": f"Ta dérive cardiaque récente est de {drift_average:+.1f} % : " + ("le rendement reste stable sur la durée." if drift_average < 5 else "la stabilité en fin d’effort reste un axe de travail."),
        })
    if volume_delta_pct is not None:
        analytics_insights.append({"tone": "neutral", "label": "Charge", "text": f"Ton volume récent évolue de {volume_delta_pct:+d} % par rapport au calcul précédent."})
    if not analytics_insights:
        analytics_insights.append({"tone": "neutral", "label": "Données", "text": "Continue à enregistrer tes sorties avec GPS et cardio pour faire émerger des tendances fiables."})

    runner_analytics = {
        "coverage_hours": round(coverage_seconds / 3600, 1),
        "training_hours": archived_training_summary["duration_hours"],
        "activities_count": archived_training_summary["activities_count"],
        "coverage_pct": round(min(100, coverage_seconds / (archived_training_summary["duration_hours"] * 3600) * 100)) if archived_training_summary["duration_hours"] > 0 else None,
        "confidence": confidence,
        "endurance_pace": _pace_label(endurance_pace),
        "endurance_hours": round(endurance_seconds / 3600, 1),
        "endurance_cadence": round(endurance_cadence * 2) if endurance_cadence else None,
        "best_climb_vam": round(best_climb[1]) if best_climb else None,
        "best_climb_slope": slope_labels.get(best_climb[0]) if best_climb else None,
        "best_climb_hours": round(best_climb[2] / 3600, 1) if best_climb else None,
        "best_descent_vam": round(best_descent[1]) if best_descent else None,
        "best_descent_slope": slope_labels.get(best_descent[0]) if best_descent else None,
        "volume_delta_pct": volume_delta_pct,
        "insights": analytics_insights[:3],
    }

    return templates.TemplateResponse(
        "runner_profile.html",
        {
            "request": request,
            "user": user,
            "hr_zones": hr_zone_names,
            "slopes_order": slopes_order,
            "profile": profile_for_template,
            "pace_lookup_by_slope": pace_lookup_by_slope,
            "sport": sport,
            "period": period,
            "tab": tab,
            "show_glucose_tabs": show_glucose_tabs,
            "glucose_zone_summary": glucose_zone_summary,
            "glucose_chart_24h": glucose_chart_24h,
            "glucose_dashboard_metrics": glucose_dashboard_metrics,
            "glucose_activity_chart": glucose_activity_chart,
            "glucose_activity_table": recent_glucose_activities,
            "glucose_activity_profile_radar": glucose_activity_profile_radar,
            "best_dplus_windows": best_dplus_windows,
            "series_matrix": series_matrix,
            "volume_weekly_summary": volume_weekly_summary,
            "distance_projections": distance_projections,
            "cardiac_drift_history": cardiac_drift_history,
            "runner_analytics": runner_analytics,
            "archived_training_summary": archived_training_summary,
        },
    )


@app.post("/ui/user/{user_id}/runner-profile/pace-projection", response_class=JSONResponse)
async def ui_runner_profile_pace_projection(
    request: Request,
    user_id: int,
    sport: str = Form("run"),
    period: str = Form("all"),
    course_id: str = Form(""),
    gpx_file: UploadFile | None = File(None),
    db: Session = Depends(get_db),
):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    official_course = None
    if (course_id or "").strip():
        loaded = _load_official_course(course_id)
        if not loaded:
            raise HTTPException(status_code=404, detail="Course officielle introuvable ou GPX non disponible.")
        official_course = loaded["course"]
        try:
            with open(loaded["route_path"], "rb") as handle:
                file_bytes = handle.read()
        except OSError:
            raise HTTPException(status_code=500, detail="Impossible de lire le GPX de cette course officielle.")
    else:
        if gpx_file is None:
            raise HTTPException(status_code=400, detail="Merci de fournir un fichier GPX ou de choisir une course officielle.")
        file_bytes = await gpx_file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Merci de fournir un fichier GPX.")

    try:
        dist_by_band, total_distance, km_segments, elevation_profile = _compute_slope_distribution_from_gpx(
            file_bytes,
            smoothing_radius_m=float((official_course or {}).get("gpx_elevation_smoothing_m") or 0.0),
        )
        dist_by_band, total_distance, km_segments, elevation_profile = _calibrate_gpx_projection_to_official_course(
            dist_by_band,
            total_distance,
            km_segments,
            elevation_profile,
            official_course,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    km_rows = []
    cumulative_m = 0.0
    for segment in km_segments:
        dist_m = segment.get("distance_m", 0.0)
        cumulative_m += dist_m
        km_rows.append(
            {
                "km_index": segment.get("km_index"),
                "distance_km": dist_m / 1000.0 if dist_m else 0.0,
                "cumulative_km": cumulative_m / 1000.0,
                "elevation_gain_m": segment.get("elevation_gain_m", 0.0),
                "elevation_loss_m": segment.get("elevation_loss_m", 0.0),
                "slope_distribution": [
                    {
                        "slope_id": slope_id,
                        "distance_km": distance_m / 1000.0,
                    }
                    for slope_id, distance_m in (segment.get("slope_dist") or {}).items()
                    if distance_m > 0
                ],
            }
        )

    return {
        "total_distance_km": total_distance / 1000.0,
        "slope_distribution": [
            {
                "slope_id": slope_id,
                "label": SLOPE_LABELS.get(slope_id, slope_id),
                "distance_km": distance_m / 1000.0,
            }
            for slope_id, distance_m in sorted(
                dist_by_band.items(),
                key=lambda item: SLOPE_ORDER_INDEX.get(item[0], 0),
            )
        ],
        "km_splits": km_rows,
        "elevation_profile": elevation_profile,
        "official_course": official_course,
    }


@app.post("/ui/user/{user_id}/course-plans", response_class=JSONResponse)
async def ui_save_course_plan(
    request: Request,
    user_id: int,
    payload: dict = Body(...),
    db: Session = Depends(get_db),
):
    """Create or update an autosaved race-plan draft owned by the current user."""
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    user = db.query(User).filter(User.id == user_id).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="Utilisateur introuvable.")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="Plan de course invalide.")
    course_plan = _upsert_course_plan_snapshot(db, user, payload)
    db.commit()
    db.refresh(course_plan)
    return {"plan": _serialize_course_plan(course_plan)}


@app.get("/ui/user/{user_id}/course-plans/{plan_id}", response_class=JSONResponse)
def ui_get_course_plan(request: Request, user_id: int, plan_id: str, db: Session = Depends(get_db)):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    course_plan = db.query(CoursePlan).filter(
        CoursePlan.id == plan_id,
        CoursePlan.user_id == user_id,
        CoursePlan.status != "archived",
    ).one_or_none()
    if not course_plan:
        raise HTTPException(status_code=404, detail="Plan de course introuvable.")
    return {"plan": _serialize_course_plan(course_plan)}


@app.post("/ui/user/{user_id}/course-plans/{plan_id}/archive", response_class=JSONResponse)
def ui_archive_course_plan(request: Request, user_id: int, plan_id: str, db: Session = Depends(get_db)):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    course_plan = db.query(CoursePlan).filter(
        CoursePlan.id == plan_id,
        CoursePlan.user_id == user_id,
    ).one_or_none()
    if not course_plan:
        raise HTTPException(status_code=404, detail="Plan de course introuvable.")
    course_plan.status = "archived"
    db.commit()
    return {"ok": True}


@app.post("/ui/user/{user_id}/course-plan/email")
async def ui_send_course_plan_email(
    request: Request,
    user_id: int,
    plan: dict = Body(...),
    db: Session = Depends(get_db),
):
    """Email the currently displayed, user-specific race plan as a PDF attachment."""
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    user = db.query(User).filter(User.id == user_id).first()
    if not user or not user.email:
        raise HTTPException(status_code=404, detail="Adresse e-mail du compte introuvable.")
    if not isinstance(plan, dict):
        raise HTTPException(status_code=422, detail="Plan de course invalide.")
    roadbook = plan.get("roadbook")
    if not isinstance(roadbook, list) or not roadbook:
        raise HTTPException(status_code=422, detail="Calcule une projection avant de l'envoyer.")
    if len(roadbook) > 120:
        raise HTTPException(status_code=422, detail="Le plan contient trop de points pour être envoyé.")

    course_name = str(plan.get("course_name") or "Course simulée").strip()[:120]
    payment_active = _payment_pilot_allowed(user_id)
    requested_plan_id = str(plan.get("course_plan_id") or "").strip()
    existing_course_plan = db.query(CoursePlan).filter(
        CoursePlan.id == requested_plan_id,
        CoursePlan.user_id == user.id,
    ).one_or_none() if requested_plan_id else None
    is_existing_purchased_plan = bool(existing_course_plan and existing_course_plan.status == "purchased")
    if payment_active and not is_existing_purchased_plan and plan.get("digital_content_consent") is not True:
        raise HTTPException(status_code=422, detail="Confirme la fourniture immédiate du plan numérique pour continuer.")
    if payment_active and not is_existing_purchased_plan:
        plan = {
            **plan,
            "digital_content_consent": True,
            "digital_content_consent_recorded_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
    course_plan = _upsert_course_plan_snapshot(db, user, plan)
    wallet = _get_plan_credit_wallet(db, user_id) if payment_active and not is_existing_purchased_plan else None
    if wallet is not None and wallet.credits < 1:
        raise HTTPException(status_code=402, detail="Aucun crédit disponible. Achète ce plan ou recharge 3 crédits.")
    delivery_attempt = None
    if payment_active and not is_existing_purchased_plan:
        delivery_attempt = PlanPaymentAttempt(
            user_id=user.id,
            course_plan_id=course_plan.id,
            user_email=user.email,
            course_name=course_name,
            plan_payload=json.dumps({"product": "credit_delivery", "plan": plan}, ensure_ascii=False, separators=(",", ":")),
            amount_cents=0,
            currency="eur",
            status="credit_delivery_processing",
        )
        db.add(delivery_attempt)
        db.commit()
        db.refresh(delivery_attempt)
        course_plan.payment_attempt_id = delivery_attempt.id
    try:
        pdf_data = _build_course_plan_pdf(user=user, plan=plan)
        roadbook_png = _build_course_plan_roadbook_png(plan=plan)
        if delivery_attempt is not None:
            _send_course_plan_admin_copy(
                attempt=delivery_attempt,
                user=user,
                pdf_data=pdf_data,
                roadbook_png=roadbook_png,
            )
            delivery_attempt.admin_sent_at = dt.datetime.utcnow()
            db.commit()
        _send_course_plan_email(
            to_email=user.email,
            recipient_name=user.first_name,
            course_name=course_name,
            pdf_data=pdf_data,
            roadbook_png=roadbook_png,
            plan=plan,
        )
        if wallet is not None:
            wallet.credits -= 1
        course_plan.status = "purchased"
        course_plan.purchased_at = course_plan.purchased_at or dt.datetime.utcnow()
        course_plan.last_downloaded_at = dt.datetime.utcnow()
        if delivery_attempt is not None:
            delivery_attempt.customer_sent_at = dt.datetime.utcnow()
            delivery_attempt.status = "delivered"
            delivery_attempt.last_error = None
        db.add(CoursePlanDownload(
            course_plan_id=course_plan.id,
            user_id=user.id,
            user_email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            course_name=course_name,
        ))
        db.commit()
    except RuntimeError as exc:
        if delivery_attempt is not None:
            delivery_attempt.status = "delivery_failed"
            delivery_attempt.last_error = str(exc)[:2000]
            db.commit()
        logger.warning("[COURSE PLAN] Envoi PDF indisponible pour user=%s : %s", user_id, exc)
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception:
        if delivery_attempt is not None:
            delivery_attempt.status = "delivery_failed"
            delivery_attempt.last_error = "Échec de génération ou d'envoi du plan."
            db.commit()
        logger.exception("[COURSE PLAN] Impossible d'envoyer le PDF à l'utilisateur %s", user_id)
        raise HTTPException(status_code=500, detail="Impossible de préparer ou d'envoyer le PDF.")
    safe_filename = re.sub(r"[^a-z0-9-]+", "-", course_name.lower()).strip("-") or "plan-course"
    return Response(
        content=pdf_data,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{safe_filename}-plan-de-course.pdf"',
            "X-Course-Plan-Message": "PDF envoyé par e-mail et prêt au téléchargement.",
        },
    )


@app.post("/ui/user/{user_id}/course-plan/checkout", response_class=JSONResponse)
async def ui_create_course_plan_checkout(
    request: Request,
    user_id: int,
    plan: dict = Body(...),
    product: str = Query("single_plan"),
    db: Session = Depends(get_db),
):
    """Create a Stripe Checkout session for one plan or a pack of plan credits."""
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    if not _payment_pilot_allowed(user_id):
        raise HTTPException(status_code=404, detail="Paiement indisponible pour ce compte.")
    product = (product or "").strip().lower()
    if product not in {"first_plan", "single_plan", "credit_pack"}:
        raise HTTPException(status_code=422, detail="Produit de plan invalide.")
    if product in {"first_plan", "single_plan"} and (not isinstance(plan, dict) or not isinstance(plan.get("roadbook"), list) or not plan.get("roadbook")):
        raise HTTPException(status_code=422, detail="Calcule une projection avant de tester le paiement.")
    if product in {"first_plan", "single_plan"} and plan.get("digital_content_consent") is not True:
        raise HTTPException(status_code=422, detail="Confirme la fourniture immédiate du plan numérique pour continuer.")
    if not (settings.PLAN_ADMIN_EMAIL or "").strip():
        raise HTTPException(status_code=503, detail="La boîte d’archivage des plans doit être configurée avant les paiements.")
    user = db.query(User).filter(User.id == user_id).one_or_none()
    if not user or not user.email:
        raise HTTPException(status_code=404, detail="Utilisateur introuvable.")
    course_plan = None
    if product in {"first_plan", "single_plan"}:
        course_plan = _upsert_course_plan_snapshot(db, user, plan)
    has_purchased_plan = _has_purchased_individual_plan(db, user.id)
    if product == "first_plan" and has_purchased_plan:
        raise HTTPException(status_code=409, detail="L’offre de premier plan a déjà été utilisée.")
    if product == "credit_pack" and not has_purchased_plan:
        raise HTTPException(status_code=403, detail="L’offre premier plan doit être utilisée avant d’acheter un pack.")
    course_name = str(plan.get("course_name") or ("Pack de 3 crédits" if product == "credit_pack" else "Course simulée")).strip()[:160]
    if product in {"first_plan", "single_plan"}:
        plan = {
            **plan,
            "digital_content_consent": True,
            "digital_content_consent_recorded_at": dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
    credit_quantity = 3 if product == "credit_pack" else 0
    amount_cents = 3000 if product == "credit_pack" else 490 if product == "first_plan" else 1490
    attempt = PlanPaymentAttempt(
        user_id=user.id,
        course_plan_id=course_plan.id if course_plan else None,
        user_email=user.email,
        course_name=course_name,
        plan_payload=json.dumps({"product": product, "credits": credit_quantity, "plan": plan if product in {"first_plan", "single_plan"} else {}}, ensure_ascii=False, separators=(",", ":")),
        amount_cents=amount_cents,
        currency="eur",
        status="creating_checkout",
    )
    db.add(attempt)
    db.commit()
    db.refresh(attempt)
    if course_plan is not None:
        course_plan.payment_attempt_id = attempt.id
        db.commit()
    try:
        stripe = _stripe_module()
        base_url = _get_app_base_url()
        price_id = (
            settings.STRIPE_PRICE_THREE_PLANS_ID if product == "credit_pack"
            else settings.STRIPE_PRICE_FIRST_PLAN_ID if product == "first_plan"
            else settings.STRIPE_PRICE_ONE_PLAN_ID
        )
        product_name = (
            "Pack de 3 crédits Running Data Plan" if product == "credit_pack"
            else "Offre premier plan Running Data Plan" if product == "first_plan"
            else "Plan de course Running Data Plan"
        )
        line_item = (
            {"price": price_id, "quantity": 1}
            if price_id
            else {
                "price_data": {
                    "currency": "eur",
                    "product_data": {"name": product_name},
                    "unit_amount": amount_cents,
                },
                "quantity": 1,
            }
        )
        checkout = stripe.checkout.Session.create(
            mode="payment",
            customer_email=user.email,
            line_items=[line_item],
            invoice_creation={
                "enabled": True,
                "invoice_data": {
                    "description": product_name,
                    "metadata": {"plan_payment_attempt_id": str(attempt.id), "product": product},
                },
            },
            metadata={"plan_payment_attempt_id": str(attempt.id), "user_id": str(user.id), "product": product},
            success_url=f"{base_url}/ui/user/{user.id}/course-plan/payment-success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{base_url}/ui/user/{user.id}",
        )
        attempt.stripe_checkout_session_id = checkout.id
        attempt.status = "pending_payment"
        db.commit()
        return {"checkout_url": checkout.url, "attempt_id": attempt.id}
    except Exception as exc:
        attempt.status = "checkout_failed"
        attempt.last_error = str(exc)[:2000]
        db.commit()
        logger.exception("[PAYMENT] Échec création Checkout user=%s attempt=%s", user_id, attempt.id)
        raise HTTPException(status_code=503, detail="Impossible de créer la session Stripe.") from exc


@app.get("/ui/user/{user_id}/course-plan/payment-success")
def ui_course_plan_payment_success(
    request: Request,
    user_id: int,
    session_id: str = Query(...),
    db: Session = Depends(get_db),
):
    """Fast path after Checkout; the webhook remains the authoritative delivery trigger."""
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    attempt = db.query(PlanPaymentAttempt).filter(
        PlanPaymentAttempt.stripe_checkout_session_id == session_id,
        PlanPaymentAttempt.user_id == user_id,
    ).one_or_none()
    if not attempt:
        raise HTTPException(status_code=404, detail="Session de paiement introuvable.")
    payment_result = "processing"
    try:
        fulfilled_attempt = _fulfill_paid_plan_attempt(db, session_id)
        payload = json.loads(fulfilled_attempt.plan_payload)
        if isinstance(payload, dict) and payload.get("product") == "credit_pack":
            payment_result = "credits"
        elif fulfilled_attempt.customer_sent_at:
            payment_result = "plan_sent"
    except RuntimeError as exc:
        logger.info("[PAYMENT] Retour Checkout en attente session=%s : %s", session_id, exc)
    except Exception:
        logger.exception("[PAYMENT] Livraison différée session=%s", session_id)
    plan_query = f"&plan={quote_plus(attempt.course_plan_id)}" if attempt.course_plan_id else ""
    return RedirectResponse(url=f"/ui/user/{user_id}?payment_plan={payment_result}{plan_query}", status_code=303)


@app.post("/webhooks/stripe", response_class=JSONResponse)
async def stripe_payment_webhook(request: Request, db: Session = Depends(get_db)):
    """Verified Stripe webhook. Safe to receive the same event more than once."""
    if not settings.STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Webhook Stripe non configuré.")
    payload = await request.body()
    signature = request.headers.get("Stripe-Signature", "")
    try:
        stripe = _stripe_module()
        event = stripe.Webhook.construct_event(payload, signature, settings.STRIPE_WEBHOOK_SECRET)
    except Exception as exc:
        logger.warning("[PAYMENT] Signature webhook Stripe invalide : %s", exc)
        raise HTTPException(status_code=400, detail="Webhook Stripe invalide.") from exc
    event_type = str(event["type"] or "")
    if event_type in {"checkout.session.completed", "checkout.session.async_payment_succeeded"}:
        checkout_session = event["data"]["object"]
        session_id = str(checkout_session["id"] or "")
        if session_id:
            try:
                _fulfill_paid_plan_attempt(db, session_id)
            except RuntimeError as exc:
                logger.info("[PAYMENT] Session Stripe non livrable pour l'instant %s : %s", session_id, exc)
            except Exception:
                logger.exception("[PAYMENT] Échec de livraison session=%s", session_id)
                # Stripe doit recevoir 200 : l'état delivery_failed est tracé et peut être renvoyé manuellement.
    return {"received": True}



# -----------------------------------------------------------------------------
# UI : Login
# -----------------------------------------------------------------------------

def _hash_reset_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _get_app_base_url() -> str:
    base_url = (settings.APP_BASE_URL or "").strip().rstrip("/")
    if not base_url:
        for candidate in (settings.STRAVA_REDIRECT_URI, settings.DEXCOM_REDIRECT_URI):
            raw = (candidate or "").strip()
            if not raw:
                continue
            parts = urlsplit(raw)
            if parts.scheme and parts.netloc:
                base_url = f"{parts.scheme}://{parts.netloc}"
                break
    if not base_url:
        base_url = "http://127.0.0.1:8000"
    return base_url


def _get_login_url() -> str:
    base_url = _get_app_base_url()
    return f"{base_url}/ui/login"


def _append_login_link_footer(body: str) -> str:
    clean_body = (body or "").rstrip()
    return (
        f"{clean_body}\n\n"
        "Retrouver Running Data Plan :\n"
        "https://www.runningdataplan.com/\n"
    )


def _send_reset_email(*, to_email: str, reset_url: str) -> None:
    if not settings.SMTP_HOST or not settings.SMTP_PORT:
        raise RuntimeError("SMTP settings missing (host/port).")
    if not settings.SMTP_USER or not settings.SMTP_PASS:
        raise RuntimeError("SMTP settings missing (user/pass).")

    from_name = settings.SMTP_FROM_NAME or "Running Data Plan"
    from_email = settings.SMTP_FROM_EMAIL or settings.SMTP_USER

    msg = EmailMessage()
    msg["Subject"] = "Réinitialisation du mot de passe"
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = to_email
    msg.set_content(_append_login_link_footer(
        "Bonjour,\n\n"
        "Vous avez demandé une réinitialisation de mot de passe.\n"
        f"Utilisez ce lien (valide 1 heure) :\n{reset_url}\n\n"
        "Si vous n'êtes pas à l'origine de cette demande, ignorez cet email.\n"
    ))

    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASS)
        server.send_message(msg)


def _send_plain_email(
    *,
    recipients: list[str],
    subject: str,
    body: str,
    include_login_footer: bool = True,
) -> int:
    if not settings.SMTP_HOST or not settings.SMTP_PORT:
        raise RuntimeError("SMTP settings missing (host/port).")
    if not settings.SMTP_USER or not settings.SMTP_PASS:
        raise RuntimeError("SMTP settings missing (user/pass).")

    clean_recipients = [email.strip() for email in recipients if email and email.strip()]
    if not clean_recipients:
        return 0

    from_name = settings.SMTP_FROM_NAME or "Running Data Plan"
    from_email = settings.SMTP_FROM_EMAIL or settings.SMTP_USER

    sent_count = 0
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASS)
        for to_email in clean_recipients:
            msg = EmailMessage()
            msg["Subject"] = subject
            msg["From"] = f"{from_name} <{from_email}>"
            msg["To"] = to_email
            msg.set_content(_append_login_link_footer(body) if include_login_footer else body)
            server.send_message(msg)
            sent_count += 1
    return sent_count


def _course_plan_pdf_value(value, fallback: str = "-") -> str:
    text = str(value or "").strip()
    return escape(text) if text else fallback


def _build_course_plan_pdf(*, user: User, plan: dict) -> bytes:
    """Build a compact, printable race-plan report from the current simulation."""
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, KeepTogether
        from reportlab.graphics.shapes import Drawing, Line, PolyLine, Polygon, Rect, String, Circle
    except ImportError as exc:
        raise RuntimeError("La génération PDF n'est pas encore installée sur le serveur. Lance `pip install -r requirements.txt` puis redémarre le serveur.") from exc
    buffer = BytesIO()
    document = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=15 * mm,
        leftMargin=15 * mm,
        topMargin=14 * mm,
        bottomMargin=15 * mm,
        title="Plan de course",
        author="Running Data Plan",
    )
    styles = getSampleStyleSheet()
    # Palette alignée avec l'interface : fond ivoire, encre profonde et un seul accent corail.
    ink = colors.HexColor("#121316")
    muted = colors.HexColor("#6f6b64")
    coral = colors.HexColor("#ff5a36")
    ivory = colors.HexColor("#f8f5ef")
    soft_ink = colors.HexColor("#efebe4")
    border = colors.HexColor("#dfdad0")

    title_style = ParagraphStyle(
        "CoursePlanTitle", parent=styles["Title"], fontName="Helvetica-Bold",
        fontSize=23, leading=27, textColor=ink, spaceAfter=5,
    )
    subtitle_style = ParagraphStyle(
        "CoursePlanSubtitle", parent=styles["Normal"], fontName="Helvetica",
        fontSize=9, leading=13, textColor=muted, spaceAfter=13,
    )
    section_style = ParagraphStyle(
        "CoursePlanSection", parent=styles["Heading2"], fontName="Helvetica-Bold",
        fontSize=13, leading=16, textColor=ink, spaceBefore=16, spaceAfter=7,
    )
    body_style = ParagraphStyle(
        "CoursePlanBody", parent=styles["BodyText"], fontName="Helvetica",
        fontSize=8.6, leading=12, textColor=colors.HexColor("#3f3c37"),
    )
    small_style = ParagraphStyle(
        "CoursePlanSmall", parent=body_style, fontSize=7.2, leading=9.5,
    )
    table_header_style = ParagraphStyle(
        "CoursePlanTableHeader", parent=small_style, fontName="Helvetica-Bold", textColor=colors.white,
    )
    table_header_light_style = ParagraphStyle(
        "CoursePlanTableHeaderLight", parent=small_style, fontName="Helvetica-Bold", textColor=muted,
        fontSize=6.6, leading=8, spaceAfter=0,
    )
    table_label_style = ParagraphStyle(
        "CoursePlanTableLabel", parent=small_style, fontName="Helvetica-Bold", textColor=muted,
        fontSize=6.6, leading=8, spaceAfter=2,
    )
    table_value_style = ParagraphStyle(
        "CoursePlanTableValue", parent=body_style, fontName="Helvetica-Bold", textColor=ink,
        fontSize=11, leading=13,
    )
    eyebrow_style = ParagraphStyle(
        "CoursePlanEyebrow", parent=small_style, fontName="Helvetica-Bold", textColor=coral,
        fontSize=6.8, leading=9, spaceAfter=3,
    )
    card_title_style = ParagraphStyle(
        "CoursePlanCardTitle", parent=body_style, fontName="Helvetica-Bold", textColor=ink,
        fontSize=11.5, leading=14, spaceAfter=3,
    )
    card_text_style = ParagraphStyle(
        "CoursePlanCardText", parent=small_style, textColor=muted, fontSize=7.5, leading=10,
    )
    hero_time_style = ParagraphStyle(
        "CoursePlanHeroTime", parent=title_style, fontName="Helvetica-Bold", textColor=ink,
        fontSize=28, leading=32, spaceAfter=4,
    )

    def metric_card(label: str, value: str, width: float) -> Table:
        card = Table([[
            Paragraph(str(label).upper(), table_label_style),
        ], [
            Paragraph(str(value), table_value_style),
        ]], colWidths=[width])
        card.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.white),
            ("BOX", (0, 0), (-1, -1), .5, border),
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 1),
            ("TOPPADDING", (0, 1), (-1, 1), 0),
            ("BOTTOMPADDING", (0, 1), (-1, 1), 8),
            ("LEFTPADDING", (0, 0), (-1, -1), 9),
            ("RIGHTPADDING", (0, 0), (-1, -1), 9),
        ]))
        return card

    def info_card(label: str, title: str, detail: str, width: float, *, accent: bool = False) -> Table:
        """Small, scannable PDF card used instead of dense data tables."""
        card = Table([[
            Paragraph(str(label).upper(), eyebrow_style),
        ], [
            Paragraph(str(title), card_title_style),
        ], [
            Paragraph(str(detail), card_text_style),
        ]], colWidths=[width])
        style = [
            ("BACKGROUND", (0, 0), (-1, -1), colors.white),
            ("BOX", (0, 0), (-1, -1), .55, border),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, 0), 9),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 0),
            ("TOPPADDING", (0, 1), (-1, 1), 1),
            ("BOTTOMPADDING", (0, 1), (-1, 1), 2),
            ("TOPPADDING", (0, 2), (-1, 2), 0),
            ("BOTTOMPADDING", (0, 2), (-1, 2), 9),
            ("LEFTPADDING", (0, 0), (-1, -1), 10),
            ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ]
        if accent:
            style.append(("LINEBEFORE", (0, 0), (0, -1), 3, coral))
        card.setStyle(TableStyle(style))
        return card

    def card_grid(cards: list, columns: int, card_width: float) -> Table:
        rows = []
        for start in range(0, len(cards), columns):
            row = cards[start:start + columns]
            if len(row) < columns:
                row.extend([""] * (columns - len(row)))
            rows.append(row)
        grid = Table(rows, colWidths=[card_width] * columns, hAlign="LEFT")
        grid.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("TOPPADDING", (0, 0), (-1, -1), 0),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ("LEFTPADDING", (0, 0), (-1, -1), 0),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]))
        return grid

    def clean_profile(raw_points) -> list[dict]:
        return [
            point for point in (raw_points or [])
            if isinstance(point, dict)
            and isinstance(point.get("distance_km"), (int, float))
            and isinstance(point.get("elevation_m"), (int, float))
        ][:900]

    def grade_color(grade: float) -> colors.Color:
        if grade <= -8:
            return colors.HexColor("#5bb9e6")
        if grade < -2:
            return colors.HexColor("#83cceb")
        if grade < 5:
            return colors.HexColor("#dfe68d")
        if grade < 12:
            return colors.HexColor("#f4c446")
        if grade < 20:
            return colors.HexColor("#f38b2d")
        return colors.HexColor("#e94c34")

    def elevation_drawing(raw_points, title: str, width: float = 180 * mm, height: float = 54 * mm, fueling_windows: list[dict] | None = None):
        points = clean_profile(raw_points)
        if len(points) < 2:
            return Paragraph("Profil indisponible pour ce tronçon.", small_style)
        drawing = Drawing(width, height)
        pad_x, pad_y = 7 * mm, 8 * mm
        values_x = [float(point["distance_km"]) for point in points]
        values_y = [float(point["elevation_m"]) for point in points]
        min_x, max_x = min(values_x), max(values_x)
        min_y, max_y = min(values_y), max(values_y)
        range_x, range_y = max(.01, max_x - min_x), max(1.0, max_y - min_y)
        drawing.add(Rect(0, 0, width, height, fillColor=ivory, strokeColor=border, strokeWidth=.5))
        drawing.add(String(pad_x, height - 4.5 * mm, title.upper(), fontName="Helvetica-Bold", fontSize=6.8, fillColor=muted))
        coords = []
        for point in points:
            x = pad_x + ((float(point["distance_km"]) - min_x) / range_x) * (width - 2 * pad_x)
            y = pad_y + ((float(point["elevation_m"]) - min_y) / range_y) * (height - pad_y - 9 * mm)
            coords.append((x, y, float(point.get("grade_percent") or 0)))
        base_y = pad_y
        for index in range(1, len(coords)):
            previous, current = coords[index - 1], coords[index]
            drawing.add(Polygon([previous[0], base_y, previous[0], previous[1], current[0], current[1], current[0], base_y], fillColor=grade_color(current[2]), strokeColor=None))
        drawing.add(PolyLine([coordinate for point in coords for coordinate in point[:2]], strokeColor=colors.HexColor("#45433f"), strokeWidth=.75, strokeLineJoin=1))
        for window in fueling_windows or []:
            if not isinstance(window, dict) or window.get("terrain") == "steady":
                continue
            try:
                marker_km = float(window.get("fromKm"))
            except (TypeError, ValueError):
                continue
            closest = min(points, key=lambda point: abs(float(point["distance_km"]) - marker_km))
            x = pad_x + ((float(closest["distance_km"]) - min_x) / range_x) * (width - 2 * pad_x)
            y = pad_y + ((float(closest["elevation_m"]) - min_y) / range_y) * (height - pad_y - 9 * mm)
            color = colors.HexColor("#f4c446") if window.get("terrain") == "climb" else colors.HexColor("#5bb9e6")
            drawing.add(Circle(x, y, 2.3, fillColor=color, strokeColor=colors.white, strokeWidth=.7))
        drawing.add(Line(pad_x, base_y, width - pad_x, base_y, strokeColor=border, strokeWidth=.4))
        drawing.add(String(pad_x, 2.5 * mm, f"{min_x:.1f} km", fontName="Helvetica", fontSize=6.5, fillColor=muted))
        drawing.add(String(width - pad_x - 23 * mm, 2.5 * mm, f"{max_x:.1f} km", fontName="Helvetica", fontSize=6.5, fillColor=muted))
        drawing.add(String(width - 27 * mm, height - 4.5 * mm, f"{min_y:.0f}-{max_y:.0f} m", fontName="Helvetica", fontSize=6.5, fillColor=muted))
        return drawing

    def route_map_drawing(raw_points, width: float = 180 * mm, height: float = 72 * mm):
        points = [
            point for point in clean_profile(raw_points)
            if isinstance(point.get("latitude"), (int, float)) and isinstance(point.get("longitude"), (int, float))
        ]
        if len(points) < 2:
            return Paragraph("Carte indisponible : coordonnées GPS absentes de la trace.", small_style)
        drawing = Drawing(width, height)
        pad = 7 * mm
        lats = [float(point["latitude"]) for point in points]
        lons = [float(point["longitude"]) for point in points]
        min_lat, max_lat, min_lon, max_lon = min(lats), max(lats), min(lons), max(lons)
        span_lat, span_lon = max(.00001, max_lat - min_lat), max(.00001, max_lon - min_lon)
        drawing.add(Rect(0, 0, width, height, fillColor=ivory, strokeColor=border, strokeWidth=.5))
        coords = [
            (pad + ((float(point["longitude"]) - min_lon) / span_lon) * (width - 2 * pad), pad + ((float(point["latitude"]) - min_lat) / span_lat) * (height - 2 * pad))
            for point in points
        ]
        flat_coords = [coordinate for point in coords for coordinate in point]
        drawing.add(PolyLine(flat_coords, strokeColor=coral, strokeWidth=1.35, strokeLineJoin=1))
        drawing.add(Circle(coords[0][0], coords[0][1], 1.8, fillColor=ink, strokeColor=colors.white, strokeWidth=.55))
        drawing.add(Circle(coords[-1][0], coords[-1][1], 1.8, fillColor=coral, strokeColor=colors.white, strokeWidth=.55))
        drawing.add(String(pad, height - 4.5 * mm, "TRACE GPS - DÉPART / ARRIVÉE", fontName="Helvetica-Bold", fontSize=6.8, fillColor=muted))
        return drawing

    first_name = (user.first_name or "").strip()
    runner_name = first_name or user.email
    course_name = _course_plan_pdf_value(plan.get("course_name"), "Course simulée")
    generated_at = dt.datetime.now().strftime("%d/%m/%Y à %H:%M")
    departure_date_value = str(plan.get("departure_date") or "").strip()
    try:
        departure_date_label = dt.date.fromisoformat(departure_date_value).strftime("%d/%m/%Y")
    except ValueError:
        departure_date_label = ""
    departure_label = " · ".join(part for part in (departure_date_label, _course_plan_pdf_value(plan.get("departure_time"), "")) if part) or "-"
    hero = Table([[
        Paragraph("RUNNING DATA PLAN · RACE PACK PERSONNALISÉ", eyebrow_style),
    ], [
        Paragraph(f"Plan de course - {course_name}", title_style),
    ], [
        Paragraph(_course_plan_pdf_value(plan.get("total_time")), hero_time_style),
    ], [
        Paragraph(
            f"Temps total estimé · préparé pour <b>{_course_plan_pdf_value(runner_name)}</b> · généré le {generated_at}",
            card_text_style,
        ),
    ]], colWidths=[180 * mm], hAlign="LEFT")
    hero.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.white),
        ("BOX", (0, 0), (-1, -1), .7, border),
        ("LINEBEFORE", (0, 0), (0, -1), 4, coral),
        ("TOPPADDING", (0, 0), (-1, 0), 13),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 0),
        ("TOPPADDING", (0, 1), (-1, 1), 1),
        ("BOTTOMPADDING", (0, 1), (-1, 1), 0),
        ("TOPPADDING", (0, 2), (-1, 2), 2),
        ("BOTTOMPADDING", (0, 2), (-1, 2), 0),
        ("TOPPADDING", (0, 3), (-1, 3), 0),
        ("BOTTOMPADDING", (0, 3), (-1, 3), 13),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
    ]))
    story = [hero]

    overview = [
        ["Temps total estimé", _course_plan_pdf_value(plan.get("total_time"))],
        ["Temps en mouvement", _course_plan_pdf_value(plan.get("moving_time"))],
        ["Parcours", _course_plan_pdf_value(plan.get("distance"))],
        ["Dénivelé", _course_plan_pdf_value(plan.get("elevation"))],
        ["Intensité visée", _course_plan_pdf_value(plan.get("zone"))],
        ["Départ", departure_label],
    ]
    overview_cards = [metric_card(label, value, 49 * mm) for label, value in overview]
    overview_table = Table([overview_cards[:3], overview_cards[3:]], colWidths=[55 * mm, 55 * mm, 55 * mm], hAlign="LEFT")
    overview_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
    ]))
    story.extend([Paragraph("Synthèse de la projection", section_style), overview_table])

    elevation_profile = clean_profile(plan.get("elevation_profile"))
    if elevation_profile:
        story.extend([
            Paragraph("Parcours et profil", section_style),
            elevation_drawing(elevation_profile, "Profil global - couleurs selon la pente", fueling_windows=plan.get("fueling_windows") or []),
            Paragraph("Le relief est rempli selon la pente : bleu pour les descentes, jaune pour le roulant, orange et corail pour les montées. Les pastilles signalent les repères nutrition terrain.", small_style),
        ])

    pacing = plan.get("pacing") if isinstance(plan.get("pacing"), list) else []
    if pacing:
        story.append(PageBreak())
        story.extend([
            Paragraph("Stratégie d'allure", section_style),
            Paragraph("Les allures ci-dessous sont celles utilisées pour la projection. Elles restent des repères à adapter au terrain, à la météo et à tes sensations.", body_style),
            Spacer(1, 7),
        ])
        pacing_cards = []
        for row in pacing[:12]:
            if isinstance(row, dict):
                pace = _course_plan_pdf_value(row.get("pace"))
                vam = _course_plan_pdf_value(row.get("vam"))
                pacing_cards.append(info_card(
                    _course_plan_pdf_value(row.get("slope")),
                    pace,
                    f"VAM de référence : {vam}",
                    84 * mm,
                    accent=True,
                ))
        if pacing_cards:
            story.append(card_grid(pacing_cards, 2, 84 * mm))

    nutrition = plan.get("nutrition") if isinstance(plan.get("nutrition"), dict) else {}
    story.append(PageBreak())
    story.append(Paragraph("Repères nutritionnels", section_style))
    nutrition_text = (
        f"Cible indicative : <b>{_course_plan_pdf_value(nutrition.get('carbs_rate'))} de glucides/h</b>"
        f" - {_course_plan_pdf_value(nutrition.get('calories_rate'))} d'apports planifiés/h"
        f" - dépense estimée {_course_plan_pdf_value(nutrition.get('expenditure_rate'))}/h."
    )
    story.extend([
        Paragraph(nutrition_text, body_style),
        Spacer(1, 4),
        Paragraph(
            "Ce plan reste générique : il est fondé sur le poids, le profil du parcours et l'intensité sélectionnée. "
            "Pour une stratégie précise, adaptée notamment à la tolérance digestive, à la santé ou au suivi glycémique, "
            "fais personnaliser le plan par un professionnel.",
            small_style,
        ),
    ])

    nutrition_totals = [
        ["Glucides à préparer", _course_plan_pdf_value(nutrition.get("total_carbs"))],
        ["Protéines à prévoir", _course_plan_pdf_value(nutrition.get("total_proteins"))],
        ["Lipides à prévoir", _course_plan_pdf_value(nutrition.get("total_fats"))],
        ["Apports énergétiques planifiés", _course_plan_pdf_value(nutrition.get("total_calories"))],
    ]
    nutrition_cards = [metric_card(label, value, 78 * mm) for label, value in nutrition_totals]
    nutrition_totals_table = Table([nutrition_cards[:2], nutrition_cards[2:]], colWidths=[82.5 * mm, 82.5 * mm], hAlign="LEFT")
    nutrition_totals_table.setStyle(TableStyle([
        ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
        ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 4.5),
    ]))
    story.extend([Spacer(1, 6), nutrition_totals_table])

    nutrition_stops = plan.get("nutrition_stops") if isinstance(plan.get("nutrition_stops"), list) else []
    if nutrition_stops:
        story.extend([
            Paragraph("Préparer chaque ravito", section_style),
            Paragraph("Lis chaque carte comme une consigne : ce que tu peux prendre sur place, ce qu'il faut emporter et la durée jusqu'au prochain point.", body_style),
            Spacer(1, 7),
        ])
        nutrition_cards = []
        for stop in nutrition_stops[:80]:
            if not isinstance(stop, dict):
                continue
            point = _course_plan_pdf_value(stop.get("point"))
            km = _course_plan_pdf_value(stop.get("km"))
            detail = (
                f"Jusqu'à {_course_plan_pdf_value(stop.get('destination'))} · {_course_plan_pdf_value(stop.get('duration'))}<br/>"
                f"<b>Sur place</b> {_course_plan_pdf_value(stop.get('at_aid'))} · "
                f"<b>À emporter</b> {_course_plan_pdf_value(stop.get('to_carry'))}<br/>"
                f"{_course_plan_pdf_value(stop.get('proteins'))} protéines · {_course_plan_pdf_value(stop.get('fats'))} lipides · {_course_plan_pdf_value(stop.get('calories'))}"
            )
            if stop.get("advice"):
                detail += f"<br/><font color=\"#6f6b64\">{_course_plan_pdf_value(stop.get('advice'))}</font>"
            nutrition_cards.append(info_card(f"RAVITO · {km}", point, detail, 84 * mm, accent=True))
        if nutrition_cards:
            story.append(card_grid(nutrition_cards, 2, 84 * mm))

    fueling_schedule = plan.get("fueling_schedule") if isinstance(plan.get("fueling_schedule"), list) else []
    if fueling_schedule:
        story.append(PageBreak())
        story.extend([
            Paragraph("Rythme des prises de glucides", section_style),
            Paragraph("Une prise toutes les 20 minutes. Les cartes donnent le moment, la position et le terrain à venir ; les quantités de ravito restent la référence à préparer.", body_style),
            Spacer(1, 6),
        ])
        schedule_cards = []
        for intake in fueling_schedule[:160]:
            if not isinstance(intake, dict):
                continue
            schedule_cards.append(info_card(
                f"{_course_plan_pdf_value(intake.get('moment'))} · {_course_plan_pdf_value(intake.get('km'))}",
                _course_plan_pdf_value(intake.get("carbs")),
                f"À venir : {_course_plan_pdf_value(intake.get('gain'))} · {_course_plan_pdf_value(intake.get('loss'))}<br/>{_course_plan_pdf_value(intake.get('advice'))}",
                56 * mm,
            ))
        if schedule_cards:
            story.append(card_grid(schedule_cards, 3, 56 * mm))

    roadbook = plan.get("roadbook") if isinstance(plan.get("roadbook"), list) else []
    if roadbook:
        story.append(PageBreak())
        story.append(Paragraph("Feuille de route", section_style))
        story.append(Paragraph("Tes passages estimés et les barrières importantes. Garde cette page accessible le jour de la course.", subtitle_style))
        roadbook_cards = []
        for point in roadbook[:120]:
            if not isinstance(point, dict):
                continue
            point_type = _course_plan_pdf_value(point.get("type"))
            detail = (
                f"KM {_course_plan_pdf_value(point.get('km'))} · {_course_plan_pdf_value(point.get('altitude'))} · "
                f"D+ cum. {_course_plan_pdf_value(point.get('cumulative_gain'))}<br/>"
                f"<b>Passage prévu { _course_plan_pdf_value(point.get('passage')) }</b>"
            )
            cutoff = _course_plan_pdf_value(point.get("cutoff"))
            if cutoff != "-":
                detail += f" · <font color=\"#d9462a\"><b>barrière {cutoff}</b></font>"
            if point.get("stop") and str(point.get("stop")) not in {"-", "0", "0 min"}:
                detail += f"<br/>Arrêt prévu : {_course_plan_pdf_value(point.get('stop'))}"
            roadbook_cards.append(info_card(point_type, _course_plan_pdf_value(point.get("name")), detail, 168 * mm, accent=point_type.lower() in {"ravito", "assistance", "arrivée"}))
        for card in roadbook_cards:
            story.extend([KeepTogether([card, Spacer(1, 5)])])

    legs = plan.get("legs") if isinstance(plan.get("legs"), list) else []
    if legs and elevation_profile:
        story.append(PageBreak())
        story.append(Paragraph("Tes tronçons, un par un", title_style))
        story.append(Paragraph("Chaque profil est extrait de la trace GPX. Utilise ces blocs pour préparer l'effort et les apports jusqu'au prochain point.", subtitle_style))
        for index, leg in enumerate(legs[:80], start=1):
            if not isinstance(leg, dict):
                continue
            try:
                from_km, to_km = float(leg.get("from_km")), float(leg.get("to_km"))
            except (TypeError, ValueError):
                continue
            leg_profile = [point for point in elevation_profile if from_km - .02 <= float(point["distance_km"]) <= to_km + .02]
            heading = f"{_course_plan_pdf_value(leg.get('from_name'))} - {_course_plan_pdf_value(leg.get('to_name'))}"
            detail = (
                f"{_course_plan_pdf_value(leg.get('distance'))} · {_course_plan_pdf_value(leg.get('gain'))} · "
                f"{_course_plan_pdf_value(leg.get('loss'))} · {_course_plan_pdf_value(leg.get('duration'))} · "
                f"allure prévue {_course_plan_pdf_value(leg.get('pace'))}"
            )
            story.extend([
                info_card(f"TRONÇON {index:02d}", heading, detail, 168 * mm, accent=True), Spacer(1, 4),
                elevation_drawing(leg_profile, f"Profil du tronçon - km {from_km:.1f} à {to_km:.1f}", height=42 * mm),
                Spacer(1, 9),
            ])

    def add_footer(canvas, _doc):
        canvas.saveState()
        canvas.setStrokeColor(border)
        canvas.line(15 * mm, 10 * mm, 195 * mm, 10 * mm)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(muted)
        canvas.drawString(15 * mm, 7.5 * mm, "Running Data Plan - plan de course indicatif")
        canvas.setFont("Helvetica", 6.4)
        canvas.drawString(15 * mm, 4.3 * mm, "La trace GPX peut évoluer : vérifie toujours le parcours et les informations officielles de l’organisateur avant le départ.")
        canvas.drawRightString(195 * mm, 6 * mm, f"Page {canvas.getPageNumber()}")
        canvas.restoreState()

    document.build(story, onFirstPage=add_footer, onLaterPages=add_footer)
    return buffer.getvalue()


def _build_course_plan_roadbook_png(*, plan: dict) -> bytes:
    """Build a compact phone-friendly roadbook image attached with the race PDF."""
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError as exc:
        raise RuntimeError("La génération de la feuille de route PNG n'est pas installée sur le serveur.") from exc

    roadbook = [row for row in (plan.get("roadbook") or []) if isinstance(row, dict)]
    if not roadbook:
        raise RuntimeError("La feuille de route PNG nécessite une projection de course.")

    def _font(size: int, *, bold: bool = False):
        candidates = (
            ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", "/Library/Fonts/Arial Bold.ttf"]
            if bold
            else ["/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", "/Library/Fonts/Arial.ttf"]
        )
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size)
            except OSError:
                continue
        return ImageFont.load_default()

    def _short(value: object, limit: int) -> str:
        text = str(value or "—").strip()
        return text if len(text) <= limit else f"{text[:limit - 1].rstrip()}…"

    width, margin = 1080, 54
    header_height, row_height, footer_height = 210, 76, 70
    height = header_height + len(roadbook) * row_height + footer_height
    # Version claire, pensée pour être enregistrée sur le téléphone et partagée.
    image = Image.new("RGB", (width, height), "#f8f5ef")
    draw = ImageDraw.Draw(image)
    title_font, subtitle_font = _font(40, bold=True), _font(22)
    type_font, name_font, detail_font = _font(18, bold=True), _font(25, bold=True), _font(20)
    draw.rectangle((0, 0, width, header_height), fill="#ffffff")
    draw.rectangle((0, 0, 10, header_height), fill="#ff5a36")
    draw.rectangle((0, header_height - 7, width, header_height), fill="#ff5a36")
    course_name = _short(plan.get("course_name") or "Plan de course", 44)
    draw.text((margin, 32), "RUNNING DATA PLAN · FEUILLE DE ROUTE", font=type_font, fill="#d9462a")
    draw.text((margin, 67), course_name, font=title_font, fill="#1d1d1b")
    departure_date = str(plan.get("departure_date") or "").strip()
    try:
        departure_date = dt.date.fromisoformat(departure_date).strftime("%d/%m/%Y")
    except ValueError:
        departure_date = ""
    overview = " · ".join(part for part in [departure_date, str(plan.get("distance") or "").strip(), str(plan.get("total_time") or "").strip()] if part)
    draw.text((margin, 123), overview or "Feuille de route", font=subtitle_font, fill="#6f6b64")
    draw.text((margin, 164), "PASSAGES ESTIMÉS · ARRÊTS · RAVITOS · ASSISTANCES · CONTRÔLES", font=type_font, fill="#6f6b64")

    point_styles = {
        "départ": {"accent": "#57a8ca", "chip": "#eaf5f9", "text": "#2d7494"},
        "ravito": {"accent": "#d9a12e", "chip": "#fff4d9", "text": "#966b14"},
        "assistance": {"accent": "#e58345", "chip": "#fff0e6", "text": "#b65422"},
        "contrôle": {"accent": "#9884c4", "chip": "#f1edfb", "text": "#68528e"},
        "arrivée": {"accent": "#e55b40", "chip": "#fff0eb", "text": "#b63e28"},
    }
    y = header_height
    for index, row in enumerate(roadbook):
        row_type = _short(row.get("type") or "Point", 16)
        style = point_styles.get(row_type.lower(), {"accent": "#9a958d", "chip": "#f0ede7", "text": "#625e58"})
        row_fill = "#ffffff" if index % 2 == 0 else "#fcfbf8"
        draw.rectangle((0, y, width, y + row_height), fill=row_fill)
        draw.rectangle((0, y, 8, y + row_height), fill=style["accent"])
        draw.line((margin, y + row_height, width - margin, y + row_height), fill="#e2ddd4", width=1)
        pill_right = margin + 164
        draw.rounded_rectangle((margin, y + 19, pill_right, y + 55), radius=18, fill=style["chip"], outline=style["accent"], width=1)
        type_bbox = draw.textbbox((0, 0), row_type.upper(), font=type_font)
        type_width = type_bbox[2] - type_bbox[0]
        draw.text((margin + (164 - type_width) / 2, y + 27), row_type.upper(), font=type_font, fill=style["text"])
        draw.text((pill_right + 22, y + 16), _short(row.get("name"), 33), font=name_font, fill="#1d1d1b")
        stop = str(row.get("stop") or "").strip()
        stop_detail = f"   ·   arrêt {stop}" if stop and stop not in {"-", "0 min", "0"} else ""
        draw.text((pill_right + 22, y + 45), _short(f"{_short(row.get('km'), 16)}{stop_detail}", 38), font=detail_font, fill="#6f6b64")
        passage = re.sub(r"^(J\+\d+)\s*", r"\1   -   ", _short(row.get("passage"), 22))
        passage_bbox = draw.textbbox((0, 0), passage, font=name_font)
        draw.text((width - margin - (passage_bbox[2] - passage_bbox[0]), y + 23), passage, font=name_font, fill="#1d1d1b")
        y += row_height

    draw.rectangle((0, height - footer_height, width, height), fill="#ffffff")
    draw.rectangle((0, height - footer_height, width, height - footer_height + 4), fill="#ff5a36")
    draw.text((margin, height - 47), "Running Data Plan · Feuille de route indicative", font=detail_font, fill="#6f6b64")
    draw.text((width - margin - 250, height - 47), "À enregistrer sur ton téléphone", font=detail_font, fill="#d9462a")
    png_buffer = BytesIO()
    image.save(png_buffer, format="PNG", optimize=True)
    return png_buffer.getvalue()


def _send_course_plan_email(
    *,
    to_email: str,
    recipient_name: str | None,
    course_name: str,
    pdf_data: bytes,
    roadbook_png: bytes,
    plan: dict,
) -> None:
    if not settings.SMTP_HOST or not settings.SMTP_PORT or not settings.SMTP_USER or not settings.SMTP_PASS:
        raise RuntimeError("La configuration SMTP est incomplète.")
    from_name = settings.SMTP_FROM_NAME or "Running Data Plan"
    from_email = settings.SMTP_FROM_EMAIL or settings.SMTP_USER
    first_name = (recipient_name or "").strip()
    greeting = f"Bonjour {first_name}," if first_name else "Bonjour,"
    safe_filename = re.sub(r"[^a-z0-9-]+", "-", (course_name or "plan-course").lower()).strip("-") or "plan-course"
    msg = EmailMessage()
    msg["Subject"] = f"Ton plan de course - {course_name or 'simulation'}"
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = to_email
    nutrition = plan.get("nutrition") if isinstance(plan.get("nutrition"), dict) else {}
    overview_lines = [
        f"- Temps total estimé : {str(plan.get('total_time') or '-').strip()}",
        f"- Parcours : {str(plan.get('distance') or '-').strip()} · {str(plan.get('elevation') or '-').strip()}",
        f"- Intensité : {str(plan.get('zone') or '-').strip()} · départ {' '.join(part for part in [str(plan.get('departure_date') or '').strip(), str(plan.get('departure_time') or '-').strip()] if part)}",
        f"- Repère nutrition : {str(nutrition.get('carbs_rate') or '-').strip()} de glucides/h · {str(nutrition.get('calories_rate') or '-').strip()} d'apports/h",
    ]
    overview_summary = "\n".join(overview_lines)
    withdrawal_notice = (
        "\nTu as demandé expressément la fourniture immédiate de ce contenu numérique et reconnu la perte de ton droit de rétractation dès le début de son exécution, dans les conditions légales applicables.\n"
        if plan.get("digital_content_consent") is True else ""
    )
    msg.set_content(_append_login_link_footer(
        f"{greeting}\n\n"
        f"Ton plan de course pour {course_name or 'ta simulation'} est prêt.\n\n"
        "Tu trouveras le PDF et une feuille de route PNG en pièces jointes. Le PNG, compact et lisible sur téléphone, "
        "reprend les passages, ravitos, assistances, contrôles et l’arrivée. Le PDF est aussi proposé au téléchargement sur ton appareil après cet envoi. "
        "Il rassemble les éléments utiles pour préparer ta course :\n\n"
        "• ton temps estimé et l’intensité retenue ;\n"
        "• les allures et VAM utilisées selon les pentes ;\n"
        "• le profil du parcours et le détail de chaque tronçon ;\n"
        "• les heures de passage, ravitos, assistances et temps d’arrêt prévus ;\n"
        "• les repères nutritionnels à préparer et le timing des prises de glucides.\n\n"
        "Résumé de ta projection :\n"
        f"{overview_summary}\n\n"
        "Ce plan est construit à partir de ton profil coureur, du parcours et de tes réglages. Plus ton historique Strava est riche, "
        "plus les allures utilisées deviennent personnalisées. Pense à tester la nutrition et les temps d’arrêt à l’entraînement : "
        "ce document est une base de préparation, à adapter à ton expérience, ta tolérance digestive et, si nécessaire, avec un professionnel.\n\n"
        f"{withdrawal_notice}"
        "Bonne préparation pour ta course,\n"
        "Toute l'équipe Running Data Plan\n"
    ))
    msg.add_attachment(pdf_data, maintype="application", subtype="pdf", filename=f"{safe_filename}-plan-de-course.pdf")
    msg.add_attachment(roadbook_png, maintype="image", subtype="png", filename=f"{safe_filename}-feuille-de-route.png")
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASS)
        server.send_message(msg)


def _send_payment_confirmation_email(*, attempt: PlanPaymentAttempt, user: User, product: str, credits_added: int = 0) -> None:
    """Send the application-level purchase confirmation; Stripe remains the payment receipt issuer."""
    if not settings.SMTP_HOST or not settings.SMTP_PORT or not settings.SMTP_USER or not settings.SMTP_PASS:
        raise RuntimeError("La configuration SMTP est incomplète.")
    from_name = settings.SMTP_FROM_NAME or "Running Data Plan"
    from_email = settings.SMTP_FROM_EMAIL or settings.SMTP_USER
    amount = f"{attempt.amount_cents / 100:.2f}".replace(".", ",")
    first_name = (user.first_name or "").strip()
    greeting = f"Bonjour {first_name}," if first_name else "Bonjour,"
    if product == "credit_pack":
        delivery = f"{credits_added or 3} crédits ont été ajoutés à ton compte. Tu peux les retrouver dans Profil → Ton abonnement et les utiliser quand tu le souhaites."
        purchase = "Pack de 3 crédits"
    else:
        delivery = "Ton plan de course a été préparé et envoyé par e-mail avec son PDF et sa feuille de route PNG."
        purchase = "Offre premier plan" if product == "first_plan" else "Plan de course"
    withdrawal_notice = (
        "Tu as demandé expressément la fourniture immédiate de ce contenu numérique et reconnu la perte de ton droit de rétractation dès le début de son exécution, dans les conditions légales applicables.\n\n"
        if product in {"first_plan", "single_plan"} else ""
    )
    msg = EmailMessage()
    msg["Subject"] = f"Confirmation de paiement · {purchase}"
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = user.email
    msg.set_content(_append_login_link_footer(
        f"{greeting}\n\n"
        "Nous confirmons la réception de ton paiement.\n\n"
        f"Achat : {purchase}\n"
        f"Montant : {amount} €\n"
        f"Référence : #{attempt.id}\n\n"
        f"{delivery}\n\n"
        f"{withdrawal_notice}"
        "Stripe t’adresse également le justificatif de paiement et la facture associée. Pense à vérifier les dossiers Spam, Indésirables ou Promotions si tu ne les vois pas.\n\n"
        "Une question sur cet achat ? Utilise le formulaire d’assistance depuis Running Data Plan.\n\n"
        "L’équipe Running Data Plan\n"
    ))
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASS)
        server.send_message(msg)


def _send_payment_confirmation_if_needed(db: Session, *, attempt: PlanPaymentAttempt, user: User, product: str, credits_added: int = 0) -> None:
    if attempt.payment_confirmation_sent_at:
        return
    try:
        _send_payment_confirmation_email(attempt=attempt, user=user, product=product, credits_added=credits_added)
        attempt.payment_confirmation_sent_at = dt.datetime.utcnow()
        db.commit()
    except Exception:
        db.rollback()
        logger.exception("[PAYMENT] Confirmation e-mail impossible pour la demande %s", attempt.id)


def _send_course_plan_admin_copy(
    *,
    attempt: PlanPaymentAttempt,
    user: User,
    pdf_data: bytes,
    roadbook_png: bytes,
) -> None:
    """Archive a deliverable plan with the administrator before customer delivery."""
    recipient = (settings.PLAN_ADMIN_EMAIL or "").strip()
    if not recipient:
        raise RuntimeError("PLAN_ADMIN_EMAIL est requis pour archiver les plans.")
    if not settings.SMTP_HOST or not settings.SMTP_PORT or not settings.SMTP_USER or not settings.SMTP_PASS:
        raise RuntimeError("La configuration SMTP est incomplète.")
    safe_filename = re.sub(r"[^a-z0-9-]+", "-", attempt.course_name.lower()).strip("-") or "plan-course"
    from_name = settings.SMTP_FROM_NAME or "Running Data Plan"
    from_email = settings.SMTP_FROM_EMAIL or settings.SMTP_USER
    msg = EmailMessage()
    msg["Subject"] = f"[Running Data Plan] Copie du plan #{attempt.id} - {attempt.course_name}"
    msg["From"] = f"{from_name} <{from_email}>"
    msg["To"] = recipient
    msg.set_content(
        "Copie d'archive d'un plan à conserver.\n\n"
        f"Utilisateur : {user.email} (id={user.id})\n"
        f"Course : {attempt.course_name}\n"
        f"Référence Stripe : {attempt.stripe_checkout_session_id or 'crédit disponible'}\n"
        f"Demande : #{attempt.id}\n\n"
        "Le PDF et la feuille de route PNG sont joints. Conserve cet e-mail pour pouvoir renvoyer le plan si nécessaire.\n"
    )
    msg.add_attachment(pdf_data, maintype="application", subtype="pdf", filename=f"{safe_filename}-plan-de-course.pdf")
    msg.add_attachment(roadbook_png, maintype="image", subtype="png", filename=f"{safe_filename}-feuille-de-route.png")
    with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT) as server:
        server.starttls()
        server.login(settings.SMTP_USER, settings.SMTP_PASS)
        server.send_message(msg)


def _payment_pilot_allowed(user_id: int) -> bool:
    return bool(
        settings.PLAN_PAYMENTS_ENABLED
        and (
            settings.PLAN_PAYMENT_TEST_USER_ID is None
            or settings.PLAN_PAYMENT_TEST_USER_ID <= 0
            or settings.PLAN_PAYMENT_TEST_USER_ID == user_id
        )
    )


def _get_plan_credit_wallet(db: Session, user_id: int) -> PlanCreditWallet:
    wallet = db.query(PlanCreditWallet).filter(PlanCreditWallet.user_id == user_id).one_or_none()
    if wallet is None:
        wallet = PlanCreditWallet(user_id=user_id, credits=0)
        db.add(wallet)
        db.commit()
        db.refresh(wallet)
    return wallet


def _has_purchased_individual_plan(db: Session, user_id: int) -> bool:
    """Whether a user has already paid for an individual plan.

    Delivery failures still consume the introductory offer: payment succeeded and the
    failed delivery can be retried by support without asking the user to pay again.
    """
    return db.query(PlanPaymentAttempt.id).filter(
        PlanPaymentAttempt.user_id == user_id,
        PlanPaymentAttempt.amount_cents.in_((490, 1490)),
        PlanPaymentAttempt.status.in_(("paid_processing", "delivered", "delivery_failed")),
    ).first() is not None


def _serialize_course_plan(course_plan: CoursePlan) -> dict:
    """Small, template-safe representation of a saved race plan."""
    return {
        "id": course_plan.id,
        "course_id": course_plan.course_id or "",
        "course_name": course_plan.course_name or "Plan de course",
        "source_type": course_plan.source_type or "official",
        "status": course_plan.status or "draft",
        "settings": course_plan.settings_payload or {},
        "calculation": course_plan.calculation_payload or {},
        "purchased_at": course_plan.purchased_at.isoformat() if course_plan.purchased_at else None,
        "last_downloaded_at": course_plan.last_downloaded_at.isoformat() if course_plan.last_downloaded_at else None,
        "created_at": course_plan.created_at.isoformat() if course_plan.created_at else None,
        "updated_at": course_plan.updated_at.isoformat() if course_plan.updated_at else None,
    }


def _upsert_course_plan_snapshot(db: Session, user: User, payload: dict) -> CoursePlan:
    """Persist a draft or the latest calculation without changing a paid plan's access."""
    plan_id = str(payload.get("course_plan_id") or payload.get("plan_id") or "").strip()
    course_plan = None
    if plan_id:
        course_plan = db.query(CoursePlan).filter(
            CoursePlan.id == plan_id,
            CoursePlan.user_id == user.id,
        ).one_or_none()
        if course_plan is None:
            raise HTTPException(status_code=404, detail="Plan de course introuvable.")

    course_id = str(payload.get("course_id") or payload.get("official_course_id") or "").strip()[:160]
    course_name = str(payload.get("course_name") or "Nouveau plan").strip()[:160] or "Nouveau plan"
    source_type = str(payload.get("source_type") or ("official" if course_id else "custom")).strip().lower()
    if source_type not in {"official", "custom"}:
        source_type = "official"
    settings_payload = payload.get("settings") or payload.get("plan_settings") or {}
    calculation_payload = payload.get("calculation") or payload.get("calculation_snapshot")
    if not isinstance(settings_payload, dict):
        raise HTTPException(status_code=422, detail="Réglages de plan invalides.")
    if calculation_payload is not None and not isinstance(calculation_payload, dict):
        raise HTTPException(status_code=422, detail="Calcul de plan invalide.")

    if course_plan is None:
        course_plan = CoursePlan(
            id=str(uuid.uuid4()),
            user_id=user.id,
            course_id=course_id or None,
            course_name=course_name,
            source_type=source_type,
            status="draft",
            settings_payload=settings_payload,
            calculation_payload=calculation_payload,
        )
        db.add(course_plan)
    else:
        course_plan.course_id = course_id or course_plan.course_id
        course_plan.course_name = course_name or course_plan.course_name
        course_plan.source_type = source_type
        course_plan.settings_payload = settings_payload
        if calculation_payload is not None:
            course_plan.calculation_payload = calculation_payload
    return course_plan


def _backfill_legacy_course_plans(db: Session, user_id: int) -> None:
    """Expose historic paid deliveries as reopenable plans without changing their audit trail."""
    attempts = db.query(PlanPaymentAttempt).filter(
        PlanPaymentAttempt.user_id == user_id,
        PlanPaymentAttempt.course_plan_id.is_(None),
        PlanPaymentAttempt.status.in_(("paid_processing", "delivered", "delivery_failed")),
        PlanPaymentAttempt.amount_cents.in_((0, 490, 1490)),
    ).all()
    changed = False
    for attempt in attempts:
        try:
            payload = json.loads(attempt.plan_payload or "{}")
            plan_data = payload.get("plan", payload) if isinstance(payload, dict) else {}
        except (TypeError, ValueError):
            plan_data = {}
        if not isinstance(plan_data, dict) or not plan_data.get("course_name"):
            continue
        course_plan = CoursePlan(
            id=str(uuid.uuid4()), user_id=user_id,
            course_id=str(plan_data.get("course_id") or plan_data.get("official_course_id") or "")[:160] or None,
            course_name=str(plan_data.get("course_name") or attempt.course_name or "Plan de course")[:160],
            source_type=str(plan_data.get("source_type") or "official")[:20], status="purchased",
            settings_payload=plan_data.get("settings") if isinstance(plan_data.get("settings"), dict) else {},
            calculation_payload=plan_data.get("calculation") if isinstance(plan_data.get("calculation"), dict) else None,
            payment_attempt_id=attempt.id, purchased_at=attempt.customer_sent_at or attempt.updated_at or attempt.created_at,
            last_downloaded_at=attempt.customer_sent_at,
        )
        db.add(course_plan)
        db.flush()
        attempt.course_plan_id = course_plan.id
        changed = True
    if changed:
        db.commit()


def _stripe_module():
    if not settings.STRIPE_SECRET_KEY:
        raise RuntimeError("STRIPE_SECRET_KEY est manquant.")
    try:
        import stripe
    except ImportError as exc:
        raise RuntimeError("Le module Stripe n'est pas installé. Redéploie après mise à jour des dépendances.") from exc
    stripe.api_key = settings.STRIPE_SECRET_KEY
    return stripe


def _fulfill_paid_plan_attempt(db: Session, checkout_session_id: str) -> PlanPaymentAttempt:
    """Idempotent paid-plan delivery: archive first, then send the customer copy."""
    stripe = _stripe_module()
    checkout = stripe.checkout.Session.retrieve(checkout_session_id)
    if checkout.payment_status != "paid":
        raise RuntimeError("Le paiement Stripe n'est pas encore confirmé.")
    attempt = db.query(PlanPaymentAttempt).filter(
        PlanPaymentAttempt.stripe_checkout_session_id == checkout_session_id
    ).one_or_none()
    if attempt is None:
        raise RuntimeError("Demande de plan introuvable pour cette session Stripe.")
    user = db.query(User).filter(User.id == attempt.user_id).one_or_none()
    if not user:
        raise RuntimeError("Utilisateur introuvable pour cette demande de plan.")
    try:
        payload = json.loads(attempt.plan_payload)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("Snapshot du plan invalide.") from exc

    product = payload.get("product") if isinstance(payload, dict) else None
    if product == "credit_pack":
        if attempt.status == "credited":
            _send_payment_confirmation_if_needed(db, attempt=attempt, user=user, product=product, credits_added=int(payload.get("credits") or 3))
            return attempt
        credits = max(1, int(payload.get("credits") or 3))
        wallet = _get_plan_credit_wallet(db, user.id)
        wallet.credits += credits
        attempt.stripe_payment_intent_id = str(checkout.payment_intent or "") or None
        attempt.status = "credited"
        attempt.last_error = None
        db.commit()
        _send_payment_confirmation_if_needed(db, attempt=attempt, user=user, product=product, credits_added=credits)
        return attempt

    plan = payload.get("plan") if isinstance(payload, dict) and "plan" in payload else payload
    if not isinstance(plan, dict):
        raise RuntimeError("Plan de course introuvable dans le snapshot.")
    course_plan = db.query(CoursePlan).filter(
        CoursePlan.id == attempt.course_plan_id,
        CoursePlan.user_id == user.id,
    ).one_or_none() if attempt.course_plan_id else None
    if attempt.customer_sent_at:
        if course_plan and course_plan.status != "purchased":
            course_plan.status = "purchased"
            course_plan.purchased_at = course_plan.purchased_at or dt.datetime.utcnow()
            course_plan.last_downloaded_at = dt.datetime.utcnow()
            db.commit()
        _send_payment_confirmation_if_needed(db, attempt=attempt, user=user, product=product)
        return attempt

    attempt.stripe_payment_intent_id = str(checkout.payment_intent or "") or None
    attempt.status = "paid_processing"
    db.commit()
    try:
        pdf_data = _build_course_plan_pdf(user=user, plan=plan)
        roadbook_png = _build_course_plan_roadbook_png(plan=plan)
        if not attempt.admin_sent_at:
            _send_course_plan_admin_copy(
                attempt=attempt,
                user=user,
                pdf_data=pdf_data,
                roadbook_png=roadbook_png,
            )
            attempt.admin_sent_at = dt.datetime.utcnow()
            db.commit()
        _send_course_plan_email(
            to_email=user.email,
            recipient_name=user.first_name,
            course_name=attempt.course_name,
            pdf_data=pdf_data,
            roadbook_png=roadbook_png,
            plan=plan,
        )
        attempt.customer_sent_at = dt.datetime.utcnow()
        attempt.status = "delivered"
        attempt.last_error = None
        if course_plan:
            course_plan.status = "purchased"
            course_plan.purchased_at = course_plan.purchased_at or dt.datetime.utcnow()
            course_plan.last_downloaded_at = dt.datetime.utcnow()
        db.add(CoursePlanDownload(
            course_plan_id=course_plan.id if course_plan else None,
            user_id=user.id,
            user_email=user.email,
            first_name=user.first_name,
            last_name=user.last_name,
            course_name=attempt.course_name,
        ))
        db.commit()
        _send_payment_confirmation_if_needed(db, attempt=attempt, user=user, product=product)
    except Exception as exc:
        attempt.status = "delivery_failed"
        attempt.last_error = str(exc)[:2000]
        db.commit()
        raise
    return attempt


def _delete_user_account_data(db: Session, user: User) -> None:
    activities = db.query(Activity).filter(Activity.user_id == user.id).all()
    for activity in activities:
        db.delete(activity)

    db.query(StravaToken).filter(StravaToken.user_id == user.id).delete()
    db.query(LibreCredentials).filter(LibreCredentials.user_id == user.id).delete()
    db.query(DexcomToken).filter(DexcomToken.user_id == user.id).delete()
    db.query(CareLinkCredential).filter(CareLinkCredential.user_id == user.id).delete()
    db.query(NightscoutCredential).filter(NightscoutCredential.user_id == user.id).delete()
    db.query(CoursePlanDownload).filter(CoursePlanDownload.user_id == user.id).delete()
    db.query(PlanPaymentAttempt).filter(PlanPaymentAttempt.user_id == user.id).delete()
    db.query(PlanCreditWallet).filter(PlanCreditWallet.user_id == user.id).delete()
    db.query(UserLoginEvent).filter(UserLoginEvent.user_id == user.id).delete()
    db.query(GlucosePoint).filter(GlucosePoint.user_id == user.id).delete()
    db.query(UserSettings).filter(UserSettings.user_id == user.id).delete()
    db.query(models.UserVamPR).filter(models.UserVamPR.user_id == user.id).delete()
    db.query(ActivityVamPeak).filter(ActivityVamPeak.user_id == user.id).delete()
    db.query(models.RunnerProfileActivityContribution).filter(
        models.RunnerProfileActivityContribution.user_id == user.id
    ).delete()
    db.query(models.RunnerProfileMonthly).filter(models.RunnerProfileMonthly.user_id == user.id).delete()
    db.delete(user)

def _render_login_page(request: Request):
    login_next = _sanitize_next_path(request.query_params.get("next"))
    official_courses = _load_official_course_catalog()
    hero_points = [
        {
            "title": "Projection chrono & VAM",
            "detail": "Compare tes VAM, cadences et allures selon le pourcentage de pente pour anticiper tes chronos.",
        },
        {
            "title": "Lecture fine du terrain",
            "detail": "Visualise l’enchaînement de montées, splits et temps en zones pour optimiser ton D+.",
        },
        {
            "title": "CGM en option",
            "detail": "Connecte Dexcom / FreeStyle Libre pour suivre glycémie et énergie en parallèle de tes efforts.",
        },
    ]
    onboarding_steps = [
        "Se connecter ou créer un compte Running Data Plan",
        "Lier Strava (et optionnellement ton capteur Dexcom / Libre)",
        "Laisser l’appli enrichir automatiquement chaque activité",
    ]
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "seo_title": "Running Data Plan : plan de course trail, pacing et glycémie",
            "seo_description": "Créez un plan de course trail personnalisé à partir de vos données Strava : pacing, allures selon la pente, ravitaillements et suivi de glycémie en option.",
            "canonical_url": f"{_get_app_base_url()}/",
            "seo_image_url": f"{_get_app_base_url()}/static/logo.png",
            "seo_og_type": "website",
            "seo_robots": "index,follow",
            "schema_json": json.dumps({
                "@context": "https://schema.org",
                "@type": "WebSite",
                "name": "Running Data Plan",
                "url": _get_app_base_url(),
                "inLanguage": "fr-FR",
            }, ensure_ascii=False),
            "hero_points": hero_points,
            "onboarding_steps": onboarding_steps,
            "official_courses": official_courses,
            "login_next": login_next or "",
        },
    )


@app.get("/ui/login", response_class=HTMLResponse)
def ui_login_form(request: Request):
    """
    Page de connexion (UI).
    """
    return _render_login_page(request)


@app.post("/ui/login", response_class=HTMLResponse)
def ui_login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next: str = Form(""),
):
    """
    Traite le formulaire de login :
    - vérifie email + mot de passe,
    - si OK -> redirige vers /ui/user/{user_id}
    - sinon -> affiche une page d'erreur élégante
    """
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
    finally:
        db.close()

    next_path = _sanitize_next_path(next)

    if not user or not pwd_context.verify(password, user.password_hash):
        return templates.TemplateResponse(
            "login_error.html",
            {"request": request, "email": email, "login_next": next_path or ""},
            status_code=401
        )

    request.session["user_id"] = int(user.id)
    try:
        login_db = SessionLocal()
        login_db.add(UserLoginEvent(user_id=user.id))
        login_db.commit()
    except Exception:
        logger.exception("[AUTH] Impossible d'enregistrer la connexion de l'utilisateur %s", user.id)
    finally:
        if 'login_db' in locals():
            login_db.close()
    return RedirectResponse(url=next_path or f"/ui/user/{user.id}", status_code=302)


@app.get("/ui/forgot-password", response_class=HTMLResponse)
def ui_forgot_password_form(request: Request):
    return templates.TemplateResponse("forgot_password.html", {"request": request})


@app.post("/ui/forgot-password", response_class=HTMLResponse)
def ui_forgot_password_send(request: Request, email: str = Form(...)):
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.email == email).first()
        if user:
            token = secrets.token_urlsafe(32)
            token_hash = _hash_reset_token(token)
            expires_at = dt.datetime.utcnow() + dt.timedelta(hours=1)
            rec = models.PasswordResetToken(
                user_id=user.id,
                token_hash=token_hash,
                expires_at=expires_at,
            )
            db.add(rec)
            db.commit()

            base_url = str(request.base_url).rstrip("/")
            reset_url = f"{base_url}/ui/reset-password?token={token}"
            _send_reset_email(to_email=user.email, reset_url=reset_url)
    finally:
        db.close()

    return templates.TemplateResponse(
        "forgot_password_sent.html",
        {"request": request, "email": email},
    )


@app.get("/ui/reset-password", response_class=HTMLResponse)
def ui_reset_password_form(request: Request, token: str | None = None):
    if not token:
        return templates.TemplateResponse(
            "reset_password_invalid.html", {"request": request}, status_code=400
        )

    token_hash = _hash_reset_token(token)
    db = SessionLocal()
    try:
        rec = (
            db.query(models.PasswordResetToken)
            .filter(models.PasswordResetToken.token_hash == token_hash)
            .first()
        )
        if (
            rec is None
            or rec.used_at is not None
            or rec.expires_at <= dt.datetime.utcnow()
        ):
            return templates.TemplateResponse(
                "reset_password_invalid.html", {"request": request}, status_code=400
            )
        user = db.query(User).get(rec.user_id)
        if not user:
            return templates.TemplateResponse(
                "reset_password_invalid.html", {"request": request}, status_code=400
            )
        email = user.email
    finally:
        db.close()

    return templates.TemplateResponse(
        "reset_password.html",
        {"request": request, "token": token, "email": email},
    )


@app.post("/ui/reset-password", response_class=HTMLResponse)
def ui_reset_password_apply(
    request: Request,
    token: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    token_hash = _hash_reset_token(token)
    if password != confirm_password:
        return templates.TemplateResponse(
            "reset_password.html",
            {
                "request": request,
                "token": token,
                "email": None,
                "error": "Les mots de passe ne correspondent pas.",
            },
            status_code=400,
        )
    db = SessionLocal()
    try:
        rec = (
            db.query(models.PasswordResetToken)
            .filter(models.PasswordResetToken.token_hash == token_hash)
            .first()
        )
        if (
            rec is None
            or rec.used_at is not None
            or rec.expires_at <= dt.datetime.utcnow()
        ):
            return templates.TemplateResponse(
                "reset_password_invalid.html", {"request": request}, status_code=400
            )

        user = db.query(User).get(rec.user_id)
        if not user:
            return templates.TemplateResponse(
                "reset_password_invalid.html", {"request": request}, status_code=400
            )

        user.password_hash = pwd_context.hash(password)
        rec.used_at = dt.datetime.utcnow()
        db.add(user)
        db.add(rec)
        db.commit()
    finally:
        db.close()

    return templates.TemplateResponse(
        "reset_password_done.html", {"request": request}
    )

#-----------------------------------------------------------------------------
#-------------------UI : Dashboard utilisateur -----------------------
#-----------------------------------------------------------------------------
@app.get("/ui/user/{user_id}", response_class=HTMLResponse)
def ui_user_dashboard(user_id: int, request: Request):
    """
    Dashboard pour un utilisateur donné :
    - liste les activités enregistrées
    - affiche la glycémie des 24 dernières heures (si tu as ajouté ce bloc)
    - montre les 2 (ici 5) dernières activités avec stats clés + mini-carte
    - + records VAM et tableau VAM par bandes de pente (option hr_zone=?)
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    custom_course_mode = request.query_params.get("mode") == "custom"

    db = SessionLocal()

    # ✅ variables par défaut (sécurité en cas d'erreur intermédiaire)
    user = None
    activities = []
    last_activities = []
    gly_labels_js = "[]"
    gly_values_js = "[]"
    user_prs = []
    vam_by_slope = []
    hr_zone_filter = "all"
    fc_info = None
    volume_summary = None
    sport_distribution = []
    vam_highlights = []
    dash_distance_projections = []
    runner_profile_overview = {"zones": {}}
    dashboard_cohort_curves = {"zones": {}, "minimum_runners": 8}
    dashboard_pace_lookup = {}
    dashboard_hr_zones = [name for (name, _, _) in HR_ZONES]
    official_courses = _load_official_course_catalog()
    daily_glucose_chart = []
    daily_activity_windows = []
    daily_glucose_window_label = None
    daily_glucose_day_start = None
    daily_glucose_day_end = None
    show_daily_glucose = False
    dashboard_warning = None
    debug_js = "{}"
    plan_credits = 0
    has_purchased_plan = False
    plan_download_count = 0
    saved_course_plans = []
    resume_course_plan = None

    try:
        user = db.query(User).get(user_id)
        if not user:
            return HTMLResponse(
                content=f"""
                <html>
                  <head><meta charset="utf-8"><title>User introuvable</title></head>
                  <body>
                    <h1>Utilisateur introuvable</h1>
                    <p>Aucun utilisateur avec id={user_id}</p>
                    <p><a href="/ui/login">Retour à la connexion</a></p>
                  </body>
                </html>
                """,
                status_code=404,
            )
        if _payment_pilot_allowed(user_id):
            plan_credits = _get_plan_credit_wallet(db, user_id).credits
            has_purchased_plan = _has_purchased_individual_plan(db, user_id)
        _backfill_legacy_course_plans(db, user_id)
        plan_download_count = db.query(CoursePlanDownload.id).filter(CoursePlanDownload.user_id == user_id).count()
        course_plan_rows = (
            db.query(CoursePlan)
            .filter(CoursePlan.user_id == user_id, CoursePlan.status != "archived")
            .order_by(CoursePlan.updated_at.desc(), CoursePlan.created_at.desc())
            .all()
        )
        saved_course_plans = [_serialize_course_plan(item) for item in course_plan_rows]
        resume_plan_id = str(request.query_params.get("plan") or "").strip()
        if resume_plan_id:
            resume_course_plan = next((item for item in saved_course_plans if item["id"] == resume_plan_id), None)

        _maybe_refresh_glucose_for_page_view(db, user, page_name="dashboard")

        # ---------------------------
        # 📊 Liste des activités (pour le sélecteur)
        # ---------------------------
        activities_db = (
            db.query(Activity)
            .filter(Activity.user_id == user_id)
            .order_by(desc(Activity.start_date))
            .limit(200)
            .all()
        )

        activities = []
        for a in activities_db:
            date_str = a.start_date.strftime("%Y-%m-%d %H:%M") if a.start_date else "n/a"
            dist_km = (a.distance or 0) / 1000 if a.distance else 0
            name = a.name or f"Activité {a.strava_activity_id}"
            label = f"{date_str} · {dist_km:.1f} km · {name}"
            activities.append({"id": a.id, "label": label})

        # ---------------------------
        # 🆕 dernières activités avec stats + carte
        # ---------------------------
        recent = (
            db.query(Activity)
            .filter(Activity.user_id == user_id)
            .order_by(desc(Activity.start_date))
            .limit(5)
            .all()
        )

        last_activities = []
        for a in recent[:5]:
            # TIR (calc fallback)
            tir = a.time_in_range_percent
            if tir is None:
                rows = (
                    db.query(ActivityStreamPoint.glucose_mgdl)
                    .filter(ActivityStreamPoint.activity_id == a.id)
                    .all()
                )
                vals = [r[0] for r in rows if r[0] is not None]
                if vals:
                    in_range = sum(1 for v in vals if 70 <= v <= 180)
                    tir = (in_range / len(vals)) * 100.0

            # Build GPS list (downsample to ~200 points)
            gps_rows = (
                db.query(ActivityStreamPoint.lat, ActivityStreamPoint.lon)
                .filter(
                    ActivityStreamPoint.activity_id == a.id,
                    ActivityStreamPoint.lat.isnot(None),
                    ActivityStreamPoint.lon.isnot(None),
                )
                .order_by(ActivityStreamPoint.idx.asc())
                .all()
            )
            gps = [[float(lat), float(lon)] for (lat, lon) in gps_rows]
            if len(gps) > 200:
                step = max(1, len(gps) // 200)
                gps = gps[::step]

            # 🎯 couleur par niveau
            level = a.level
            level_color = None
            if level == 1:
                level_color = "#22c55e"   # vert
            elif level == 2:
                level_color = "#3b82f6"   # bleu
            elif level == 3:
                level_color = "#ef4444"   # rouge
            elif level == 4:
                level_color = "#020617"   # noir
            elif level == 5:
                level_color = "#eab308"   # or

            summary_block = normalize_summary_block_layout(a.glucose_summary_block or "")

            def _strip_signature(block: str) -> str:
                if not block:
                    return ""
                lines = block.splitlines()
                signature_tokens = [
                    "join us",
                    "voir l'analyse complète",
                    "made with ❤️",
                    "made with love",
                    "/ui/user/",
                ]
                while lines:
                    last = lines[-1].strip().lower()
                    if not last:
                        lines.pop()
                        continue
                    if any(token in last for token in signature_tokens):
                        lines.pop()
                        continue
                    break
                return "\n".join(lines).strip()

            clean_block = _strip_signature(summary_block)

            last_activities.append({
                "id": a.id,
                "name": a.name or f"Activité {a.strava_activity_id}",
                "date_str": a.start_date.strftime("%Y-%m-%d %H:%M") if a.start_date else "n/a",
                "dist_km": round(((a.distance or 0) / 1000.0), 1),
                "dplus": int(a.total_elevation_gain or 0),
                "duration_sec": int(a.elapsed_time or 0),
                "tir_percent": tir,
                "gps": gps,  # JSON-serializable
                "level": level,
                "level_color": level_color,
                "sport": a.sport or (a.activity_type or "").lower(),
                "summary_block": summary_block,
                "summary_block_clean": clean_block,
            })

        # VAM 5/15/30 des dernières activités (via caches Activity)
        for it in last_activities:
            a_id = it["id"]
            a = db.query(Activity).get(a_id)
            it["vam_5"]  = float(a.max_vam_5m)  if a and a.max_vam_5m  is not None else None
            it["vam_15"] = float(a.max_vam_15m) if a and a.max_vam_15m is not None else None
            it["vam_30"] = float(a.max_vam_30m) if a and a.max_vam_30m is not None else None

        libre_connected = (
            db.query(LibreCredentials.id).filter(LibreCredentials.user_id == user_id).first() is not None
        )
        dexcom_connected = has_dexcom_share_credentials(
            db.query(DexcomToken).filter(DexcomToken.user_id == user_id).all()
        )
        carelink_connected = (
            db.query(CareLinkCredential.id).filter(CareLinkCredential.user_id == user_id).first() is not None
        )
        nightscout_connected = (
            db.query(NightscoutCredential.id).filter(NightscoutCredential.user_id == user_id).first() is not None
        )
        show_daily_glucose = libre_connected or dexcom_connected or carelink_connected or nightscout_connected

        # ---------------------------
        # 📈 Glycémie du jour + plages de sport
        # ---------------------------
        if show_daily_glucose:
            now_local = dt.datetime.now().astimezone()
            day_start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end_local = day_start_local + dt.timedelta(days=1)
            day_start_utc_naive = day_start_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
            day_end_utc_naive = day_end_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
            daily_glucose_window_label = day_start_local.strftime("%d/%m/%Y")
            daily_glucose_day_start = day_start_local.isoformat()
            daily_glucose_day_end = day_end_local.isoformat()

            glucose_points_today = (
                db.query(GlucosePoint)
                .filter(GlucosePoint.user_id == user_id)
                .filter(GlucosePoint.ts >= day_start_utc_naive)
                .filter(GlucosePoint.ts < day_end_utc_naive)
                .order_by(GlucosePoint.ts.asc())
                .all()
            )

            daily_glucose_chart = [
                {
                    "ts": _safe_dt(point.ts).isoformat(),
                    "mgdl": float(point.mgdl),
                }
                for point in glucose_points_today
                if point.ts is not None and point.mgdl is not None
            ]

            sport_windows_today = (
                db.query(Activity)
                .filter(Activity.user_id == user_id)
                .filter(Activity.start_date.isnot(None))
                .filter(Activity.start_date >= day_start_utc_naive - dt.timedelta(days=1))
                .filter(Activity.start_date < day_end_utc_naive)
                .order_by(Activity.start_date.asc())
                .all()
            )

            sport_style_map = {
                "run": {"label": "Course", "color": "rgba(239, 68, 68, 0.18)", "stroke": "rgba(239, 68, 68, 0.8)"},
                "trailrun": {"label": "Trail", "color": "rgba(249, 115, 22, 0.18)", "stroke": "rgba(249, 115, 22, 0.85)"},
                "ride": {"label": "Vélo", "color": "rgba(77, 226, 255, 0.18)", "stroke": "rgba(77, 226, 255, 0.85)"},
                "virtualride": {"label": "Home trainer", "color": "rgba(59, 130, 246, 0.18)", "stroke": "rgba(59, 130, 246, 0.85)"},
                "hike": {"label": "Rando", "color": "rgba(184, 255, 69, 0.18)", "stroke": "rgba(184, 255, 69, 0.85)"},
                "walk": {"label": "Marche", "color": "rgba(148, 163, 184, 0.2)", "stroke": "rgba(148, 163, 184, 0.85)"},
                "ski_rando": {"label": "Ski rando", "color": "rgba(168, 85, 247, 0.18)", "stroke": "rgba(168, 85, 247, 0.85)"},
                "ski_alpine": {"label": "Ski", "color": "rgba(14, 165, 233, 0.18)", "stroke": "rgba(14, 165, 233, 0.85)"},
            }

            for activity in sport_windows_today:
                if not activity.start_date:
                    continue
                start_aware = _safe_dt(activity.start_date)
                elapsed_sec = int(activity.elapsed_time or 0)
                if elapsed_sec <= 0:
                    continue
                end_aware = start_aware + dt.timedelta(seconds=elapsed_sec)
                day_start_aware_utc = day_start_local.astimezone(dt.timezone.utc)
                day_end_aware_utc = day_end_local.astimezone(dt.timezone.utc)
                if end_aware <= day_start_aware_utc or start_aware >= day_end_aware_utc:
                    continue

                clipped_start = max(start_aware, day_start_aware_utc)
                clipped_end = min(end_aware, day_end_aware_utc)
                sport_key = normalize_activity_type(activity.sport or activity.activity_type or "") or (activity.sport or "").lower()
                style = sport_style_map.get(sport_key, {"label": "Sport", "color": "rgba(255,255,255,0.1)", "stroke": "rgba(255,255,255,0.55)"})

                daily_activity_windows.append(
                    {
                        "start_ts": clipped_start.isoformat(),
                        "end_ts": clipped_end.isoformat(),
                        "label": style["label"],
                        "activity_name": activity.name or style["label"],
                        "color": style["color"],
                        "stroke": style["stroke"],
                    }
                )

        # ---------------------------
        # ❤️ FC max + zones cardio (profil rapide)
        # ---------------------------
        if user:
            fc_max_value = compute_user_fc_max(user)
            if fc_max_value:
                if user.max_heartrate:
                    fc_source = "Saisie dans le profil"
                elif user.birthdate:
                    fc_source = "Estimation (208 - 0,7 × âge)"
                else:
                    fc_source = "Valeur par défaut"
                zones_preview = []
                for idx, (zone_name, lo_ratio, hi_ratio) in enumerate(HR_ZONES, start=1):
                    bpm_min = int(round(lo_ratio * fc_max_value))
                    bpm_max = int(round(hi_ratio * fc_max_value))
                    zones_preview.append(
                        {
                            "label": zone_name,
                            "bpm_min": bpm_min,
                            "bpm_max": bpm_max,
                            "percent_label": f"{int(lo_ratio * 100)}–{int(hi_ratio * 100)} %",
                        }
                    )
                fc_info = {
                    "fc_max": int(round(fc_max_value)),
                    "source": fc_source,
                    "needs_update": user.max_heartrate is None,
                    "zones": zones_preview,
                }

                # Temps passé par zone sur 28 derniers jours
                recent_hr_cutoff = datetime.utcnow() - timedelta(days=28)
                hr_rows = (
                    db.query(
                        models.RunnerProfileActivityContribution.hr_zone,
                        func.sum(models.RunnerProfileActivityContribution.total_duration_sec).label("duration_sec"),
                    )
                    .filter(models.RunnerProfileActivityContribution.user_id == user_id)
                    .filter(models.RunnerProfileActivityContribution.metric_scope == "slope_zone")
                    .filter(models.RunnerProfileActivityContribution.activity_start_date.isnot(None))
                    .filter(models.RunnerProfileActivityContribution.activity_start_date >= recent_hr_cutoff)
                    .group_by(models.RunnerProfileActivityContribution.hr_zone)
                    .all()
                )
                if hr_rows:
                    duration_map = {row.hr_zone: float(row.duration_sec or 0.0) for row in hr_rows if row.hr_zone}
                    total_recent = sum(duration_map.values())
                    usage = []
                    for zone_name, _, _ in HR_ZONES:
                        secs = duration_map.get(zone_name, 0.0)
                        percent = (secs / total_recent * 100.0) if total_recent else 0.0
                        usage.append(
                            {
                                "label": zone_name,
                                "duration_str": _format_duration(secs) if secs else "00:00",
                                "percent": percent,
                            }
                        )
                    fc_info["recent_usage"] = {
                        "window_days": 28,
                        "total_seconds": total_recent,
                        "zones": usage,
                    }

        # ---------------------------
        # 📦 Volume hebdo moyen + SL
        # ---------------------------
        volume_summary = get_cached_volume_weekly_summary(
            db,
            user_id=user_id,
            sport="run",
        )

        # Le dashboard est désormais le point d'entrée de la projection :
        # on réutilise le profil archive (et le même fallback que la page
        # Profil coureur) pour calculer une simulation GPX directement ici.
        runner_profile_overview = get_cached_runner_profile(
            db,
            user_id=user_id,
            sport="run",
        )
        if not runner_profile_overview or not runner_profile_overview.get("zones"):
            rebuilt_months = rebuild_runner_profile_range_from_contributions(
                db,
                user_id=user_id,
                sport="run",
            )
            if rebuilt_months:
                runner_profile_overview = get_cached_runner_profile(
                    db,
                    user_id=user_id,
                    sport="run",
                )
        if not runner_profile_overview or not runner_profile_overview.get("zones"):
            runner_profile_overview = build_runner_profile(
                db,
                user_id=user_id,
                sport="run",
            )
        dashboard_pace_lookup = _build_pace_lookup_from_profile(
            runner_profile_overview,
            dashboard_hr_zones,
        )
        dashboard_cohort_curves = _build_anonymized_pace_benchmarks(
            db,
            sport="run",
            excluded_user_id=user_id,
        )

        # ---------------------------
        # 🧗‍♂️ Meilleurs D+ / VAM et projections chrono
        # ---------------------------
        dplus_windows = get_cached_dplus_windows(
            db,
            user_id=user_id,
            sport="run",
        )
        if dplus_windows:
            lookup_windows = {item["window_id"]: item for item in dplus_windows if item.get("window_id")}
            highlight_ids = ["5m", "15m", "30m", "1h"]
            for win_id in highlight_ids:
                row = lookup_windows.get(win_id)
                if not row:
                    continue
                vam_highlights.append(
                    {
                        "label": row.get("label") or win_id,
                        "gain_m": row.get("gain_m") or 0.0,
                        "vam": row.get("gain_per_hour"),
                        "activity_name": row.get("activity_name"),
                        "activity_date": row.get("activity_date"),
                    }
                )

        series_matrix = get_series_splits_matrix(
            db,
            user_id=user_id,
            sport="run",
        )
        dash_distance_efforts = get_cached_distance_efforts(
            db,
            user_id=user_id,
            sport="run",
        )
        dash_distance_projections = (
            compute_distance_projections(series_matrix, dash_distance_efforts) if series_matrix else []
        )

        # ---------------------------
        # 🧭 Répartition des sports (28 derniers jours)
        # ---------------------------
        mix_since = datetime.utcnow() - timedelta(days=28)
        recent_activities = (
            db.query(
                models.RunnerProfileActivityContribution.sport,
                models.RunnerProfileActivityContribution.total_distance_m,
                models.RunnerProfileActivityContribution.total_duration_sec,
                models.RunnerProfileActivityContribution.total_elevation_gain_m,
                models.RunnerProfileActivityContribution.activity_name,
                models.RunnerProfileActivityContribution.extra,
            )
            .filter(models.RunnerProfileActivityContribution.user_id == user_id)
            .filter(models.RunnerProfileActivityContribution.metric_scope == "activity_meta")
            .filter(models.RunnerProfileActivityContribution.activity_start_date.isnot(None))
            .filter(models.RunnerProfileActivityContribution.activity_start_date >= mix_since)
            .all()
        )

        agg = defaultdict(lambda: {"count": 0, "distance_m": 0.0, "duration_s": 0.0, "dplus_m": 0.0})
        for row in recent_activities:
            sport_raw = (row.sport or "").lower()
            extra = row.extra if isinstance(row.extra, dict) else {}
            name_lower = (row.activity_name or extra.get("name") or "").lower()

            sport_norm = normalize_activity_type(sport_raw) if sport_raw else None
            if sport_norm == "ski_alpine":
                ski_tokens = ["rando", "skimo", "backcountry", "ski tour", "ski de rando", "ski touring", "mountain", "alpinism"]
                if any(k in name_lower for k in ski_tokens):
                    sport_norm = "ski_rando"

            key = sport_norm or sport_raw or "other"
            agg[key]["count"] += 1
            agg[key]["distance_m"] += float(row.total_distance_m or 0.0)
            agg[key]["duration_s"] += float(row.total_duration_sec or 0.0)
            agg[key]["dplus_m"] += float(row.total_elevation_gain_m or 0.0)

        total_duration = sum(val["duration_s"] for val in agg.values())
        sport_label_map = {
            "run": ("Course à pied", "🏃"),
            "trailrun": ("Trail", "⛰️"),
            "trail run": ("Trail", "⛰️"),
            "ride": ("Vélo", "🚴"),
            "virtualride": ("Home trainer", "🚴‍♂️"),
            "ebikeride": ("Vélo électrique", "🚴‍♀️"),
            "ski_alpine": ("Ski alpin", "⛷️"),
            "alpineski": ("Ski alpin", "⛷️"),  # alias
            "ski": ("Ski de rando", "🎿"),  # compat
            "ski_rando": ("Ski de rando", "🎿"),
            "backcountryski": ("Ski de rando", "🎿"),  # alias direct
            "skitouring": ("Ski de rando", "🎿"),
            "skimo": ("Ski de rando", "🎿"),
            "ski_nordic": ("Ski nordique", "⛷️"),
            "nordicski": ("Ski nordique", "⛷️"),  # alias
            "rollerski": ("Ski roue", "🎿"),
            "hike": ("Randonnée", "🥾"),
            "walk": ("Marche", "🚶"),
        }
        for key, vals in agg.items():
            key_norm = normalize_activity_type(key) if key else None
            lookup_key = key_norm or key
            label, emoji = sport_label_map.get(lookup_key, ("Autre", "⚪"))
            duration_s = float(vals["duration_s"] or 0.0)
            percent_time = (duration_s / total_duration * 100.0) if total_duration else 0.0
            dist_km = (vals["distance_m"] / 1000.0) if vals["distance_m"] else 0.0
            sport_distribution.append(
                {
                    "label": label,
                    "emoji": emoji,
                    "percent_time": percent_time,
                    "distance_km": dist_km,
                    "duration_str": _format_duration(duration_s) if duration_s else "–",
                    "dplus_m": float(vals["dplus_m"] or 0.0),
                }
            )

        # ---------------------------
        # 🏆 Records VAM user (5/15/30) par sport + date formatée JJ-MM-AAAA
        # ---------------------------
        user_prs_rows = (
            db.query(
                models.UserVamPR.window_min,
                models.UserVamPR.vam_m_per_h,
                models.UserVamPR.sport,
                models.UserVamPR.activity_id,
                models.Activity.start_date,
            )
            .join(models.Activity, models.Activity.id == models.UserVamPR.activity_id, isouter=True)
            .filter(models.UserVamPR.user_id == user_id)
            .order_by(models.UserVamPR.sport.asc(), models.UserVamPR.window_min.asc())
            .all()
        )

        # ---------------------------
        # (supprimé) Tableau VAM par bandes de pente — on ne charge plus cette section
        # ---------------------------
        hr_zone_filter = request.query_params.get("hr_zone", "all")

        q = (
            db.query(
                models.ActivityZoneSlopeAgg.slope_band.label("slope_band"),
                func.min(models.ActivityZoneSlopeAgg.avg_vam_m_per_h).label("vam_min"),
                func.avg(models.ActivityZoneSlopeAgg.avg_vam_m_per_h).label("vam_avg"),
                func.max(models.ActivityZoneSlopeAgg.avg_vam_m_per_h).label("vam_max"),
                func.sum(models.ActivityZoneSlopeAgg.duration_sec).label("duration_sec"),
                func.sum(models.ActivityZoneSlopeAgg.num_points).label("num_points"),
            )
            .join(models.Activity, models.Activity.id == models.ActivityZoneSlopeAgg.activity_id)
            .filter(models.Activity.user_id == user_id)
        )

        if hr_zone_filter and hr_zone_filter.lower() != "all":
            q = q.filter(models.ActivityZoneSlopeAgg.hr_zone == hr_zone_filter)

        vam_by_slope_rows = (
            q.add_columns(models.ActivityZoneSlopeAgg.sport.label("sport"))
             .group_by(models.ActivityZoneSlopeAgg.slope_band, models.ActivityZoneSlopeAgg.sport)
             .order_by(models.ActivityZoneSlopeAgg.slope_band.asc())
             .all()
        )

        vam_by_slope = []
        for row in vam_by_slope_rows:
            band = row.slope_band
            vmin = float(row.vam_min) if row.vam_min is not None else None
            vavg = float(row.vam_avg) if row.vam_avg is not None else None
            vmax = float(row.vam_max) if row.vam_max is not None else None
            vam_by_slope.append({
                "slope_band": band,
                "vam_min": vmin,
                "vam_avg": vavg,
                "vam_max": vmax,
                "duration_sec": int(row.duration_sec or 0),
                "num_points": int(row.num_points or 0),
                "sport": row.sport or "run",
            })
        debug_js = json.dumps(
            {
                "user_id": user_id,
                "activities_count": len(activities),
                "last_activities_count": len(last_activities),
                "show_daily_glucose": show_daily_glucose,
                "daily_glucose_points": len(daily_glucose_chart),
                "sport_distribution_count": len(sport_distribution),
                "vam_highlights_count": len(vam_highlights),
                "distance_projections_count": len(dash_distance_projections),
            },
            ensure_ascii=False,
        )
    except Exception as exc:
        logger.exception("Erreur pendant le rendu du dashboard user=%s", user_id)
        if user is None:
            return HTMLResponse(
                content=(
                    "<html><head><meta charset='utf-8'><title>Erreur dashboard</title></head>"
                    "<body><h1>Erreur dashboard</h1>"
                    "<p>Impossible de charger cette page pour le moment.</p>"
                    "<p><a href='/ui/login'>Retour à la connexion</a></p></body></html>"
                ),
                status_code=500,
            )
        dashboard_warning = (
            "Certaines statistiques du dashboard n'ont pas pu être chargées. "
            "Le contenu principal reste disponible."
        )
        debug_js = json.dumps(
            {
                "user_id": user_id,
                "error": str(exc),
                "activities_count": len(activities),
                "last_activities_count": len(last_activities),
            },
            ensure_ascii=False,
        )

    finally:
        db.close()

    # ---------------------------
    # 🧠 Envoi au template
    # ---------------------------
    return templates.TemplateResponse(
        "user_dashboard.html",
        {
            "request": request,
            "user": user,
            "activities": activities,
            "last_activities": last_activities,  # mini-cartes + stats + vam_5/15/30
            "gly_labels_js": gly_labels_js,
            "gly_values_js": gly_values_js,
            "tir_low": 70,
            "tir_high": 180,
            "user_prs": user_prs,          # records 5/15/30
            "vam_by_slope": vam_by_slope,  # tableau bandes de pente
            "hr_zone_filter": hr_zone_filter,
            "fc_info": fc_info,
            "volume_summary": volume_summary,
            "vam_highlights": vam_highlights,
            "dash_distance_projections": dash_distance_projections,
            "runner_profile_overview": runner_profile_overview,
            "dashboard_cohort_curves": dashboard_cohort_curves,
            "dashboard_pace_lookup": dashboard_pace_lookup,
            "dashboard_hr_zones": dashboard_hr_zones,
            "official_courses": official_courses,
            "sport_distribution": sport_distribution,
            "daily_glucose_chart": daily_glucose_chart,
            "daily_activity_windows": daily_activity_windows,
            "daily_glucose_window_label": daily_glucose_window_label,
            "daily_glucose_day_start": daily_glucose_day_start,
            "daily_glucose_day_end": daily_glucose_day_end,
            "show_daily_glucose": show_daily_glucose,
            "dashboard_warning": dashboard_warning,
            "custom_course_mode": custom_course_mode,
            "plan_payment_test_enabled": _payment_pilot_allowed(user_id),
            "plan_credits": plan_credits,
            "has_purchased_plan": has_purchased_plan,
            "plan_download_count": plan_download_count,
            "saved_course_plans": saved_course_plans,
            "resume_course_plan": resume_course_plan,
            "debug_js": debug_js,
        },
    )


@app.get("/ui/user/{user_id}/course-simulator")
def ui_user_custom_course_simulator(user_id: int, request: Request):
    """Entrée dédiée vers le simulateur d'une trace GPX personnelle."""
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard
    return RedirectResponse(url=f"/ui/user/{user_id}?mode=custom#simulation", status_code=303)


#-----------------------------------------------------------------------------
#-------------------UI : Liste des activités -----------------------
#-----------------------------------------------------------------------------
@app.get("/ui/user/{user_id}/activities", response_class=HTMLResponse)
def ui_user_activities(user_id: int, request: Request):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    page = _safe_positive_int(request.query_params.get("page"), 1)
    page_size = 5

    WINDOW_DEFS = [
        {"id": "15m", "label": "15 min", "seconds": 15 * 60},
        {"id": "1h", "label": "1 h", "seconds": 60 * 60},
        {"id": "2h", "label": "2 h", "seconds": 2 * 60 * 60},
        {"id": "5h", "label": "5 h", "seconds": 5 * 60 * 60},
    ]

    def _build_activity_row(activity: Activity) -> dict:
        distance_km = float(activity.distance) / 1000.0 if activity.distance else None
        elevation_gain = float(activity.total_elevation_gain) if activity.total_elevation_gain is not None else None
        elevation_loss = None
        duration_str = _format_duration(activity.elapsed_time) if activity.elapsed_time else None
        start_dt = _safe_dt(activity.start_date)
        start_label = start_dt.strftime("%d %b %Y · %H:%M") if start_dt else "—"

        window_values = {w["id"]: None for w in WINDOW_DEFS}

        points = (
            db.query(
                ActivityStreamPoint.elapsed_time,
                ActivityStreamPoint.altitude,
            )
            .filter(ActivityStreamPoint.activity_id == activity.id)
            .order_by(ActivityStreamPoint.idx.asc())
            .all()
        )

        if points:
            times = []
            cum_gain = []
            cum_loss = []
            total_gain_calc = 0.0
            total_loss_calc = 0.0
            prev_alt = None

            for pt in points:
                if pt.elapsed_time is None:
                    continue
                alt = float(pt.altitude) if pt.altitude is not None else None
                if alt is not None and prev_alt is not None:
                    delta = alt - prev_alt
                    if delta > 0:
                        total_gain_calc += delta
                    elif delta < 0:
                        total_loss_calc += -delta
                if alt is not None:
                    prev_alt = alt

                times.append(float(pt.elapsed_time))
                cum_gain.append(total_gain_calc)
                cum_loss.append(total_loss_calc)

            if times:
                if total_gain_calc > 0:
                    elevation_gain = total_gain_calc
                if total_loss_calc > 0:
                    elevation_loss = total_loss_calc

                for win in WINDOW_DEFS:
                    best_gain = 0.0
                    seconds = win["seconds"]
                    start_idx = 0
                    for idx, t in enumerate(times):
                        while start_idx < idx and (t - times[start_idx]) > seconds:
                            start_idx += 1
                        gain_window = cum_gain[idx] - cum_gain[start_idx]
                        if gain_window > best_gain:
                            best_gain = gain_window
                    window_values[win["id"]] = best_gain if best_gain > 0 else 0.0

        return {
            "id": activity.id,
            "name": activity.name or f"Activité {activity.id}",
            "start_label": start_label,
            "distance_km": distance_km,
            "elevation_gain_m": elevation_gain,
            "elevation_loss_m": elevation_loss,
            "duration_str": duration_str,
            "dplus_windows": window_values,
            "sport": activity.sport or (activity.activity_type or "").lower(),
        }

    try:
        user = db.query(User).get(user_id)
        if not user:
            return HTMLResponse(status_code=404, content="Utilisateur introuvable")

        total_activities = db.query(Activity.id).filter(Activity.user_id == user_id).count()
        offset = (page - 1) * page_size
        activities = (
            db.query(Activity)
            .filter(Activity.user_id == user_id)
            .order_by(desc(Activity.start_date))
            .offset(offset)
            .limit(page_size)
            .all()
        )

        activity_rows = [_build_activity_row(act) for act in activities]
        has_prev = page > 1
        has_next = offset + page_size < total_activities

    finally:
        db.close()

    return templates.TemplateResponse(
        "user_activities.html",
        {
            "request": request,
            "user": user,
            "activities": activity_rows,
            "window_defs": WINDOW_DEFS,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total_activities,
                "start_index": offset + 1 if total_activities else 0,
                "end_index": min(offset + page_size, total_activities),
                "has_prev": has_prev,
                "has_next": has_next,
                "prev_url": f"/ui/user/{user_id}/activities?page={page - 1}" if has_prev else None,
                "next_url": f"/ui/user/{user_id}/activities?page={page + 1}" if has_next else None,
            },
        },
    )



# -----------------------------------------------------------------------------
# UI : Dashboard activité + suppression activité
# -----------------------------------------------------------------------------

@app.get("/ui/user/{user_id}/activity/{activity_id}", response_class=HTMLResponse)
async def ui_user_activity_detail(user_id: int, activity_id: int, request: Request):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    def format_duration_short(sec: float | None) -> str:
        if sec is None or sec <= 0:
            return "–"
        s = int(round(sec))
        h = s // 3600
        m = (s % 3600) // 60
        if h > 0:
            return f"{h}h{m:02d}"
        if m > 0:
            return f"{m} min"
        return f"{s}s"

    def format_distance_short(meters: float | None) -> str:
        if meters is None or meters <= 0:
            return "–"
        km = float(meters) / 1000.0
        return f"{km:.2f}".rstrip("0").rstrip(".") + " km"

    def format_pace_short(meters: float | None, seconds: float | None) -> str:
        if meters is None or seconds is None or meters <= 0 or seconds <= 0:
            return "–"
        sec_per_km = float(seconds) / (float(meters) / 1000.0)
        mins = int(sec_per_km // 60)
        secs = int(round(sec_per_km % 60))
        if secs == 60:
            mins += 1
            secs = 0
        return f"{mins}:{secs:02d}/km"

    def format_speed_short(meters: float | None, seconds: float | None) -> str:
        if meters is None or seconds is None or meters <= 0 or seconds <= 0:
            return "–"
        kmh = (float(meters) / float(seconds)) * 3.6
        return f"{kmh:.1f} km/h"

    def build_summary(label: str, detail: str, tone: str = "neutral") -> dict:
        palette = {
            "positive": ("#dcfce7", "#166534"),
            "warning": ("#fef3c7", "#b45309"),
            "danger": ("#fee2e2", "#b91c1c"),
            "neutral": ("#e2e8f0", "#334155"),
        }
        bg, fg = palette.get(tone, palette["neutral"])
        return {
            "label": label,
            "detail": detail,
            "bg_color": bg,
            "fg_color": fg,
            "tone": tone,
        }

    db = SessionLocal()
    try:
        # --- 1) USER + ACTIVITY ---
        user = db.query(User).get(user_id)
        if not user:
            return HTMLResponse("Utilisateur introuvable", status_code=404)

        _maybe_refresh_glucose_for_page_view(db, user, page_name="activity_detail")

        activity = (
            db.query(Activity)
            .filter(Activity.id == activity_id, Activity.user_id == user_id)
            .first()
        )
        if not activity:
            return HTMLResponse("Activité introuvable", status_code=404)

        sport_norm = (activity.sport or activity.activity_type or "").lower()
        is_running_activity = sport_norm == "run"

        # --- 2) STREAM POINTS ---
        points = (
            db.query(ActivityStreamPoint)
            .filter(ActivityStreamPoint.activity_id == activity.id)
            .order_by(ActivityStreamPoint.idx.asc())
            .all()
        )
        has_streams = len(points) > 1
        cardiac_drift = compute_terrain_adjusted_cardiac_drift(points)

        hr_zones = ["Zone 1", "Zone 2", "Zone 3", "Zone 4", "Zone 5"]

        # --- 3bis) GGLYCÉMIE : distribution par zones ---
        # On regarde si on a au moins quelques points de glycémie valides
        glucose_points = [
            p for p in points
            if p.glucose_mgdl is not None and p.elapsed_time is not None
        ]
        has_glucose = len(glucose_points) > 1

        # Par défaut : aucune ligne
        glucose_zone_rows = []
        glucose_chart_points = []
        glucose_zone_vs_hr_rows = []
        glucose_hr_columns = []
        glucose_profile_summary = None
        hr_zone_summary = []
        activity_type_summary = None
        story_export_data = None

        if has_glucose:
            # Définition des zones (à ajuster si tu veux plus tard)
            # id, nom, description courte, étiquette de plage, min_inclus, max_exclu
            glucose_zone_defs = [
                ("G1", "Zone 1", "Hypo",         "< 70 mg/dL",      None, 70),
                ("G2", "Zone 2", "Bas",          "70–100 mg/dL",    70,   100),
                ("G3", "Zone 3", "Cible basse",  "100–140 mg/dL",   100,  140),
                ("G4", "Zone 4", "Cible haute",  "140–180 mg/dL",   140,  180),
                ("G5", "Zone 5", "Élevée",       "> 180 mg/dL",     180,  None),
            ]

            # Temps cumulé par zone (en secondes)
            zone_time = {zid: 0.0 for (zid, *_rest) in glucose_zone_defs}
            glucose_hr_time = {
                zid: {hz: 0.0 for hz in hr_zones}
                for (zid, *_rest) in glucose_zone_defs
            }

            # Fonction utilitaire : trouver la zone à partir d'une valeur
            def find_zone_id(glu: float | None) -> str | None:
                if glu is None:
                    return None
                for zid, _name, _desc, _range_label, zmin, zmax in glucose_zone_defs:
                    if (zmin is None or glu >= zmin) and (zmax is None or glu < zmax):
                        return zid
                return None

            # On trie par temps (normalement déjà le cas avec idx, mais au cas où)
            glucose_points_sorted = sorted(glucose_points, key=lambda p: p.elapsed_time or 0)

            # On approxime la durée d'un point comme (t[i+1] - t[i]) et on
            # l'affecte à la zone de glycémie du point i
            for i in range(len(glucose_points_sorted) - 1):
                p = glucose_points_sorted[i]
                n = glucose_points_sorted[i + 1]

                if p.elapsed_time is None or n.elapsed_time is None:
                    continue
                dt_sec = float(n.elapsed_time) - float(p.elapsed_time)
                if dt_sec <= 0:
                    continue

                zid = find_zone_id(p.glucose_mgdl)
                if zid is None:
                    continue

                zone_time[zid] += dt_sec
                hz = p.hr_zone if p.hr_zone in hr_zones else None
                if hz:
                    glucose_hr_time[zid][hz] += dt_sec

            total_time = sum(zone_time.values())
            hr_time_from_glucose = {
                hz: sum(glucose_hr_time[zid][hz] for zid in glucose_hr_time)
                for hz in hr_zones
            }

            # Construction des lignes pour le tableau du template
            for idx, (zid, name, desc, range_label, _zmin, _zmax) in enumerate(glucose_zone_defs, start=1):
                t = zone_time.get(zid, 0.0)
                pct = round(t * 100.0 / total_time) if total_time > 0 else 0

                hr_cells = []
                for hz in hr_zones:
                    cell_time = glucose_hr_time[zid][hz]
                    hr_cells.append({
                        "hr_zone": hz,
                        "time_sec": cell_time,
                        "time_str": format_duration_short(cell_time),
                        "percent_of_hr": round(cell_time * 100.0 / hr_time_from_glucose[hz]) if hr_time_from_glucose[hz] > 0 else 0,
                    })

                glucose_zone_rows.append({
                    "id": zid,
                    "zone_index": idx,
                    "name": name,
                    "description": desc,
                    "range": range_label,
                    "time_sec": t,
                    "time_str": format_duration_short(t),
                    "percent": pct,
                })

                glucose_zone_vs_hr_rows.append({
                    "id": zid,
                    "name": name,
                    "description": desc,
                    "range": range_label,
                    "hr_cells": hr_cells,
                })

            for hz in hr_zones:
                column_time = hr_time_from_glucose[hz]
                glucose_hr_columns.append({
                    "zone": hz,
                    "time_sec": column_time,
                    "time_str": format_duration_short(column_time),
                    "percent_of_total": round(column_time * 100.0 / total_time) if total_time > 0 else 0,
                })

            if total_time > 0:
                hypo_ratio = zone_time.get("G1", 0.0) / total_time
                hyper_ratio = zone_time.get("G5", 0.0) / total_time
                in_range_sec = sum(zone_time.get(zid, 0.0) for zid in ("G2", "G3", "G4"))
                hyper_pct = round(hyper_ratio * 100)
                hypo_pct = round(hypo_ratio * 100)
                in_range_pct = round(in_range_sec * 100.0 / total_time)

                if hyper_ratio >= 0.25 and hyper_ratio >= hypo_ratio + 0.05:
                    glucose_profile_summary = build_summary(
                        "Profil hyperglycémie",
                        f"{hyper_pct}% du temps > 180 mg/dL. Pense à réduire les apports rapides.",
                        "warning",
                    )
                elif hypo_ratio >= 0.2 and hypo_ratio >= hyper_ratio + 0.05:
                    glucose_profile_summary = build_summary(
                        "Profil hypoglycémie",
                        f"{hypo_pct}% du temps < 70 mg/dL. Prévoir une recharge glucidique en amont.",
                        "danger",
                    )
                else:
                    glucose_profile_summary = build_summary(
                        "Profil stable",
                        f"{in_range_pct}% du temps entre 70 et 180 mg/dL.",
                        "positive",
                    )

            start_dt = _safe_dt(activity.start_date)
            for p in glucose_points_sorted:
                if p.glucose_mgdl is None or p.elapsed_time is None:
                    continue
                ts_iso = None
                if start_dt is not None:
                    ts_iso = (start_dt + dt.timedelta(seconds=float(p.elapsed_time))).isoformat()
                glucose_chart_points.append({
                    "elapsed_sec": float(p.elapsed_time),
                    "ts": ts_iso,
                    "mgdl": float(p.glucose_mgdl),
                })
        else:
            glucose_chart_points = []

        # --- 3) Synthèse cardio pour typologie séance ---
        hr_zone_time = {z: 0.0 for z in hr_zones}
        hr_points = [
            p for p in points
            if p.elapsed_time is not None and p.hr_zone in hr_zones
        ]
        hr_points_sorted = sorted(hr_points, key=lambda p: p.elapsed_time or 0)

        for i in range(len(hr_points_sorted) - 1):
            p = hr_points_sorted[i]
            n = hr_points_sorted[i + 1]
            if p.elapsed_time is None or n.elapsed_time is None:
                continue
            dt_sec = float(n.elapsed_time) - float(p.elapsed_time)
            if dt_sec <= 0:
                continue
            hr_zone_time[p.hr_zone] += dt_sec

        total_hr_time = sum(hr_zone_time.values())

        if total_hr_time > 0:
            for hz in hr_zones:
                sec = hr_zone_time.get(hz, 0.0)
                hr_zone_summary.append({
                    "zone": hz,
                    "time_sec": sec,
                    "time_str": format_duration_short(sec),
                    "percent": round(sec * 100.0 / total_hr_time),
                })

            endurance_sec = sum(hr_zone_time.get(z, 0.0) for z in hr_zones[:3])
            threshold_sec = hr_zone_time.get("Zone 4", 0.0)
            sprint_sec = hr_zone_time.get("Zone 5", 0.0)

            endurance_ratio = endurance_sec / total_hr_time
            threshold_ratio = threshold_sec / total_hr_time
            sprint_ratio = sprint_sec / total_hr_time

            endurance_pct = round(endurance_ratio * 100)
            threshold_pct = round(threshold_ratio * 100)
            sprint_pct = round(sprint_ratio * 100)

            if sprint_ratio >= 0.15:
                activity_type_summary = build_summary(
                    "Séance fractionnée (Z5)",
                    f"{sprint_pct}% du temps en Zone 5. Travail explosif / sprints.",
                    "warning",
                )
            elif threshold_ratio >= 0.25:
                activity_type_summary = build_summary(
                    "Séance seuil (Z4)",
                    f"{threshold_pct}% du temps en Zone 4. Accent sur le travail au seuil.",
                    "warning",
                )
            else:
                activity_type_summary = build_summary(
                    "Séance endurance (Z1-Z3)",
                    f"{endurance_pct}% cumulés en Zones 1 à 3.",
                    "positive",
                )
        else:
            activity_type_summary = build_summary(
                "Type d’effort indéterminé",
                "Pas assez de points cardio pour classer la séance.",
                "neutral",
            )




        # --- 3) Basic Stats ---
        dist_km = (activity.distance or 0) / 1000
        total_dist_m = activity.distance or 0
        dplus = int(activity.total_elevation_gain or 0)
        duration_sec = int(activity.elapsed_time or 0)
        fc = round(activity.average_heartrate) if activity.average_heartrate else None
        gly_avg = round(activity.avg_glucose) if activity.avg_glucose else None

        club_data = build_club_payload(user.club_slug)
        share_show_club_logo = bool(
            user.settings.share_show_club_logo
        ) if getattr(user, "settings", None) and user.settings.share_show_club_logo is not None else False

        story_export_data = _build_story_export_data(
            activity,
            glucose_chart_points,
            club_data=club_data,
            share_show_club_logo=share_show_club_logo,
        )

        # --- 4) GPS simplified + carte 3D colorée selon la glycémie ---
        gps = []
        activity_map_points = []
        for p in points:
            if p.lat is not None and p.lon is not None:
                gps.append([float(p.lat), float(p.lon)])
                activity_map_points.append({
                    "latitude": round(float(p.lat), 6),
                    "longitude": round(float(p.lon), 6),
                    "altitude_m": round(float(p.altitude or 0), 1),
                    "glucose_mgdl": round(float(p.glucose_mgdl), 1) if p.glucose_mgdl is not None else None,
                })
        if len(gps) > 300:
            gps = gps[:: max(1, len(gps)//300) ]
        if len(activity_map_points) > 550:
            final_map_point = activity_map_points[-1]
            step = max(1, math.ceil(len(activity_map_points) / 550))
            activity_map_points = activity_map_points[::step]
            if activity_map_points[-1] != final_map_point:
                activity_map_points.append(final_map_point)
        gps_js = json.dumps(gps)
        has_gps = len(gps) > 1
        activity_map_js = json.dumps(activity_map_points, ensure_ascii=False, separators=(",", ":"))
        has_activity_map = len(activity_map_points) > 1

        # --- 5) Level Color ---
        level = activity.level
        level_color = {
            1: "#22c55e",
            2: "#3b82f6",
            3: "#ef4444",
            4: "#020617",
            5: "#eab308",
        }.get(level)

        # --- 6) Bandes de pente communes (VAM + cadence) ---
        slopes_order = [
            ("Sneg30p",   "< -30%"),
            ("Sneg20_30", "-30% à -20%"),
            ("Sneg10_20", "-20% à -10%"),
            ("Sneg5_10",  "-10% à -5%"),
            ("Sneg0_5",   "-5% à 0%"),
            ("S0_5",      "0% à 5%"),
            ("S5_10",     "5% à 10%"),
            ("S10_20",    "10% à 20%"),
            ("S20_30",    "20% à 30%"),
            ("S30p",      "> 30%"),
        ]

        # --- 6a) VAM : mapping des bandes 5% BDD -> bandes larges POSITIVES ---
        VAM_DB_TO_GROUP = {
            "S0_5":   "S0_5",
            "S5_10":  "S5_10",
            "S10_15": "S10_20",
            "S15_20": "S10_20",
            "S20_25": "S20_30",
            "S25_30": "S20_30",
            "S30_40": "S30p",
            "S40p":   "S30p",
        }

        vam_by_slope_zone = {
            key: {z: None for z in hr_zones}
            for key, _ in slopes_order
        }

        tmp_vam = {
            key: {z: [] for z in hr_zones}
            for key, _ in slopes_order
        }

        aggs = (
            db.query(ActivityZoneSlopeAgg)
            .filter(ActivityZoneSlopeAgg.activity_id == activity.id)
            .all()
        )
        for agg in aggs:
            group_key = VAM_DB_TO_GROUP.get(agg.slope_band)
            if not group_key:
                continue
            if agg.hr_zone not in hr_zones:
                continue
            if agg.avg_vam_m_per_h:
                tmp_vam[group_key][agg.hr_zone].append(float(agg.avg_vam_m_per_h))

        for group_key, zones_dict in tmp_vam.items():
            for z, vals in zones_dict.items():
                if vals:
                    vam_by_slope_zone[group_key][z] = round(sum(vals) / len(vals))

                # --- 3ter) Distribution temps par zones de VAM × zone cardio ---
        vam_points = [
            p for p in points
            if p.vertical_speed_m_per_h is not None and p.elapsed_time is not None
        ]
        has_vam = len(vam_points) > 1

        # Zones de vitesse ascensionnelle (m/h) - à ajuster si tu veux
        vam_zone_defs = [
            ("V1", "Zone 1", "< 300 m/h",       None,   300),
            ("V2", "Zone 2", "300–600 m/h",     300,    600),
            ("V3", "Zone 3", "600–900 m/h",     600,    900),
            ("V4", "Zone 4", "900–1200 m/h",    900,    1200),
            ("V5", "Zone 5", "> 1200 m/h",      1200,   None),
        ]

        # Temps cumulé par zone VAM et par zone cardio
        # Clé "ALL" = toutes zones cardiaques confondues
        vam_zone_time = {
            "ALL": {zid: 0.0 for (zid, *_rest) in vam_zone_defs}
        }
        for z in hr_zones:
            vam_zone_time[z] = {zid: 0.0 for (zid, *_rest) in vam_zone_defs}

        def find_vam_zone_id(vam_val: float | None) -> str | None:
            if vam_val is None:
                return None
            for zid, _name, _label, vmin, vmax in vam_zone_defs:
                if (vmin is None or vam_val >= vmin) and (vmax is None or vam_val < vmax):
                    return zid
            return None

        # Tri par temps
        vam_points_sorted = sorted(vam_points, key=lambda p: p.elapsed_time or 0)

        for i in range(len(vam_points_sorted) - 1):
            p = vam_points_sorted[i]
            n = vam_points_sorted[i + 1]

            if p.elapsed_time is None or n.elapsed_time is None:
                continue

            dt_sec = float(n.elapsed_time) - float(p.elapsed_time)
            if dt_sec <= 0:
                continue

            # ❌ on ignore les sections où ça ne monte pas assez
            # - pente < 5 % (plat / faux plat)
            # - ou vitesse verticale ≤ 0 (descente / plat)
            if p.slope_percent is None or p.slope_percent < 5:
                continue
            if p.vertical_speed_m_per_h is None or p.vertical_speed_m_per_h <= 0:
                continue

            zid = find_vam_zone_id(p.vertical_speed_m_per_h)
            if zid is None:
                continue

            # toutes zones cardiaques
            vam_zone_time["ALL"][zid] += dt_sec

            # zone cardio spécifique si dispo
            hz = p.hr_zone
            if hz in hr_zones:
                vam_zone_time[hz][zid] += dt_sec


        # Sélection de la zone cardio pour le filtre (query param)
        vam_hr_filter = request.query_params.get("vam_hr", "ALL")
        if vam_hr_filter not in hr_zones and vam_hr_filter != "ALL":
            vam_hr_filter = "ALL"

        # Construction des lignes pour le tableau
        vam_zone_rows = []
        if has_vam:
            current_times = (
                vam_zone_time["ALL"]
                if vam_hr_filter == "ALL"
                else vam_zone_time[vam_hr_filter]
            )
            total_vam_time = sum(current_times.values())

            def format_vam_duration(sec: float) -> str:
                s = int(round(sec))
                h = s // 3600
                m = (s % 3600) // 60
                if h > 0:
                    return f"{h}h{m:02d}"
                if m > 0:
                    return f"{m} min"
                return f"{s}s"

            for zid, name, label, _vmin, _vmax in vam_zone_defs:
                t = current_times.get(zid, 0.0)
                pct = round(t * 100.0 / total_vam_time) if total_vam_time > 0 else 0

                vam_zone_rows.append({
                    "id": zid,
                    "name": name,
                    "range": label,
                    "time_sec": t,
                    "time_str": format_vam_duration(t) if t > 0 else "–",
                    "percent": pct,
                })

        # --- 7) SEGMENTS : nombre de tronçons paramétrable via ?segments=N ---
        try:
            seg_count_param = int(request.query_params.get("segments", 3))
        except ValueError:
            seg_count_param = 3

        # Autoriser entre 2 et 8 segments
        seg_count = max(2, min(8, seg_count_param))

        segments = []
        if total_dist_m > 0:
            for i in range(seg_count):
                ratio_start = i / seg_count
                ratio_end = (i + 1) / seg_count
                segments.append((ratio_start, ratio_end))

        # Classification cadence
        def cadence_class(ppm: float):
            if ppm is None:
                return "n/a"
            if ppm < 100:
                return "MARCHE"
            if ppm < 150:
                return "TROTTINAGE"
            return "COURSE"

        def slope_to_group(sp: float):
            if sp is None:
                return None
            if sp < -30:
                return "Sneg30p"
            if -30 <= sp < -20:
                return "Sneg20_30"
            if -20 <= sp < -10:
                return "Sneg10_20"
            if -10 <= sp < -5:
                return "Sneg5_10"
            if -5 <= sp < 0:
                return "Sneg0_5"
            if 0 <= sp < 5:
                return "S0_5"
            if 5 <= sp < 10:
                return "S5_10"
            if 10 <= sp < 20:
                return "S10_20"
            if 20 <= sp < 30:
                return "S20_30"
            return "S30p"

        # --- 8) PROGRESSION TABLE slope × % advancement (cadence) ---
        progression_table = {key: [] for key, _ in slopes_order}

        for slope_key, _ in slopes_order:
            for r_start, r_end in segments:

                cad_vals = []

                for p in points:
                    if p.distance is None or p.slope_percent is None:
                        continue
                    if total_dist_m == 0:
                        continue

                    progress_ratio = p.distance / total_dist_m

                    if not (r_start <= progress_ratio < r_end):
                        continue

                    sp = p.slope_percent
                    group = slope_to_group(sp)
                    if group != slope_key:
                        continue

                    if p.cadence is not None:
                        if is_running_activity:
                            cad_vals.append(p.cadence * 2)  # ppm (run = steps)
                        else:
                            cad_vals.append(p.cadence)      # garder rpm pour vélo etc.

                if cad_vals:
                    avg_val = round(sum(cad_vals) / len(cad_vals))
                    progression_table[slope_key].append({
                        "ppm": avg_val,
                        "class": cadence_class(avg_val) if is_running_activity else None,
                    })
                else:
                    progression_table[slope_key].append({
                        "ppm": None,
                        "class": "n/a",
                    })

        # --- 9) Labels affichés en KM (mais basés sur les % ci-dessus) ---
        segments_km_labels = []
        if total_dist_m > 0:
            for r_start, r_end in segments:
                start_km = dist_km * r_start
                end_km = dist_km * r_end
                segments_km_labels.append(
                    f"{round(start_km, 1)}–{round(end_km, 1)} km"
                )

        # --- 10) Vue globale par tronçon (FC, allure, D+, D-) ---
        def format_pace(sec_per_km: float | None) -> str | None:
            if sec_per_km is None or sec_per_km <= 0:
                return None
            s = int(round(sec_per_km))
            m, s = divmod(s, 60)
            return f"{m:d}:{s:02d}/km"

        def format_speed_kmh(sec_per_km: float | None) -> str | None:
            if sec_per_km is None or sec_per_km <= 0:
                return None
            speed_kmh = 3600.0 / sec_per_km
            return f"{speed_kmh:.1f} km/h"

        hr_by_segment = []
        pace_by_segment = []
        dplus_by_segment = []
        dminus_by_segment = []

        if total_dist_m > 0 and segments:
            for r_start, r_end in segments:
                hr_vals = []

                first_dist = None
                first_time = None
                last_dist = None
                last_time = None

                prev_alt = None
                seg_dplus = 0.0
                seg_dminus = 0.0

                has_points = False

                for p in points:
                    if p.distance is None:
                        continue

                    progress_ratio = p.distance / total_dist_m
                    if not (r_start <= progress_ratio < r_end):
                        continue

                    has_points = True

                    if p.heartrate is not None:
                        hr_vals.append(float(p.heartrate))

                    if p.elapsed_time is not None and p.distance is not None:
                        t = float(p.elapsed_time)
                        d = float(p.distance)
                        if first_dist is None:
                            first_dist = d
                            first_time = t
                        last_dist = d
                        last_time = t

                    if p.altitude is not None:
                        alt = float(p.altitude)
                        if prev_alt is not None:
                            diff = alt - prev_alt
                            if diff > 0:
                                seg_dplus += diff
                            elif diff < 0:
                                seg_dminus += -diff
                        prev_alt = alt

                if hr_vals:
                    hr_by_segment.append(round(sum(hr_vals) / len(hr_vals)))
                else:
                    hr_by_segment.append(None)

                pace_str = None
                sec_per_km = None
                if (
                    has_points
                    and first_dist is not None and last_dist is not None
                    and last_dist > first_dist
                    and first_time is not None and last_time is not None
                    and last_time > first_time
                ):
                    dist_m = last_dist - first_dist
                    dur_sec = last_time - first_time
                    if dist_m > 0 and dur_sec > 0:
                        sec_per_km = dur_sec / (dist_m / 1000.0)
                        if sec_per_km > 0:
                            pace_str = format_pace(sec_per_km)

                if not is_running_activity:
                    pace_str = format_speed_kmh(sec_per_km) if sec_per_km else None

                pace_by_segment.append(pace_str)

                if has_points:
                    dplus_by_segment.append(int(round(seg_dplus)))
                    dminus_by_segment.append(int(round(seg_dminus)))
                else:
                    dplus_by_segment.append(None)
                    dminus_by_segment.append(None)

        # --- 11) Profil altitude enrichi : pente, allure, VAM, FC et glycémie ---
        # La pente est lissée sur quelques points pour éviter que le bruit GPS ne
        # transforme le profil en succession de couleurs illisibles.
        raw_profile_points = []
        previous_profile_point = None
        for p in points:
            if p.distance is None or p.altitude is None:
                continue
            distance_m = float(p.distance)
            altitude_m = float(p.altitude)
            grade = float(p.slope_percent) if p.slope_percent is not None else None
            if grade is None and previous_profile_point is not None:
                distance_delta = distance_m - previous_profile_point["distance_m"]
                if distance_delta > 2:
                    grade = (altitude_m - previous_profile_point["altitude_m"]) / distance_delta * 100.0
            raw_profile_points.append({
                "distance_m": distance_m,
                "altitude_m": altitude_m,
                "elapsed_sec": float(p.elapsed_time) if p.elapsed_time is not None else None,
                "grade": grade,
                "heartrate": float(p.heartrate) if p.heartrate is not None else None,
                "glucose": float(p.glucose_mgdl) if p.glucose_mgdl is not None else None,
                "velocity": float(p.velocity) if p.velocity is not None else None,
                "vam": float(p.vertical_speed_m_per_h) if p.vertical_speed_m_per_h is not None else None,
            })
            previous_profile_point = raw_profile_points[-1]

        terrain_accumulators = {
            "descent": {"distance_m": 0.0, "seconds": 0.0, "vertical_m": 0.0, "hr": [], "glucose": []},
            "flat": {"distance_m": 0.0, "seconds": 0.0, "vertical_m": 0.0, "hr": [], "glucose": []},
            "climb": {"distance_m": 0.0, "seconds": 0.0, "vertical_m": 0.0, "hr": [], "glucose": []},
        }
        profile_chart_points = []
        smoothed_grades = []
        for index, point in enumerate(raw_profile_points):
            nearby_grades = [
                candidate["grade"]
                for candidate in raw_profile_points[max(0, index - 3): min(len(raw_profile_points), index + 4)]
                if candidate["grade"] is not None
            ]
            smooth_grade = sum(nearby_grades) / len(nearby_grades) if nearby_grades else 0.0
            smooth_grade = max(-50.0, min(50.0, smooth_grade))
            smoothed_grades.append(smooth_grade)

        for index, point in enumerate(raw_profile_points):
            pace_sec_per_km = None
            if point["velocity"] and point["velocity"] > 0:
                pace_sec_per_km = 1000.0 / point["velocity"]
            profile_chart_points.append({
                "x": round(point["distance_m"] / 1000.0, 3),
                "y": round(point["altitude_m"], 1),
                "grade": round(smoothed_grades[index], 1),
                "pace": round(pace_sec_per_km, 1) if pace_sec_per_km else None,
                "vam": round(point["vam"]) if point["vam"] is not None else None,
                "hr": round(point["heartrate"]) if point["heartrate"] is not None else None,
                "glucose": round(point["glucose"]) if point["glucose"] is not None else None,
            })
            if index == 0:
                continue
            previous = raw_profile_points[index - 1]
            distance_delta = point["distance_m"] - previous["distance_m"]
            time_delta = (point["elapsed_sec"] - previous["elapsed_sec"]) if point["elapsed_sec"] is not None and previous["elapsed_sec"] is not None else None
            if distance_delta <= 0 or time_delta is None or time_delta <= 0:
                continue
            grade = (smoothed_grades[index] + smoothed_grades[index - 1]) / 2.0
            terrain_key = "climb" if grade >= 5 else "descent" if grade <= -5 else "flat"
            accumulator = terrain_accumulators[terrain_key]
            accumulator["distance_m"] += distance_delta
            accumulator["seconds"] += time_delta
            accumulator["vertical_m"] += point["altitude_m"] - previous["altitude_m"]
            if point["heartrate"] is not None:
                accumulator["hr"].append(point["heartrate"])
            if point["glucose"] is not None:
                accumulator["glucose"].append(point["glucose"])

        terrain_summary = []
        terrain_labels = {"descent": "Descente", "flat": "Plat & faux-plat", "climb": "Montée"}
        for terrain_key in ("descent", "flat", "climb"):
            accumulator = terrain_accumulators[terrain_key]
            seconds = accumulator["seconds"]
            distance_m = accumulator["distance_m"]
            vertical_m = accumulator["vertical_m"]
            pace = seconds / (distance_m / 1000.0) if seconds > 0 and distance_m > 0 else None
            vertical_rate = vertical_m / seconds * 3600.0 if seconds > 0 else None
            terrain_summary.append({
                "key": terrain_key,
                "label": terrain_labels[terrain_key],
                "distance_km": round(distance_m / 1000.0, 1) if distance_m else None,
                "pace": format_pace_short(distance_m, seconds) if seconds > 0 and distance_m > 0 else "–",
                "vam": round(vertical_rate) if vertical_rate is not None else None,
                "hr": round(sum(accumulator["hr"]) / len(accumulator["hr"])) if accumulator["hr"] else None,
                "glucose": round(sum(accumulator["glucose"]) / len(accumulator["glucose"])) if accumulator["glucose"] else None,
            })

        # Garde un volume raisonnable pour le navigateur tout en préservant le profil.
        if len(profile_chart_points) > 900:
            stride = max(1, len(profile_chart_points) // 900)
            profile_chart_points = profile_chart_points[::stride]
            if raw_profile_points:
                profile_chart_points[-1] = {
                    "x": round(raw_profile_points[-1]["distance_m"] / 1000.0, 3),
                    "y": round(raw_profile_points[-1]["altitude_m"], 1),
                    "grade": round(smoothed_grades[-1], 1),
                    "pace": None,
                    "vam": round(raw_profile_points[-1]["vam"]) if raw_profile_points[-1]["vam"] is not None else None,
                    "hr": round(raw_profile_points[-1]["heartrate"]) if raw_profile_points[-1]["heartrate"] is not None else None,
                    "glucose": round(raw_profile_points[-1]["glucose"]) if raw_profile_points[-1]["glucose"] is not None else None,
                }
        alt_profile = [[point["x"], point["y"]] for point in profile_chart_points]
        alt_profile_js = json.dumps(alt_profile)
        profile_chart_points_js = json.dumps(profile_chart_points)

        # --- 12) Détection des montées remarquables de la sortie ---
        MIN_VAM_START = 400.0
        MIN_POINTS = 20
        MIN_DPLUS_M = 100.0
        MAX_DMINUS_RATIO = 0.05
        MIN_DIST_M = 300.0
        MIN_PENTE_PCT = 7.0
        MERGE_GAP_KM = 0.100

        climbs = []
        n_pts = len(points)

        def format_pace(sec_per_km: float | None) -> str | None:
            if sec_per_km is None or sec_per_km <= 0:
                return None
            s = int(round(sec_per_km))
            m, s = divmod(s, 60)
            return f"{m:d}:{s:02d}/km"

        i = 0
        while i < n_pts:
            while (
                i < n_pts
                and (
                    points[i].vertical_speed_m_per_h is None
                    or points[i].vertical_speed_m_per_h < MIN_VAM_START
                    or points[i].altitude is None
                    or points[i].distance is None
                    or points[i].elapsed_time is None
                )
            ):
                i += 1

            if i >= n_pts:
                break

            start_idx = i
            start_p = points[start_idx]
            start_alt = float(start_p.altitude)
            start_dist = float(start_p.distance)
            start_time = float(start_p.elapsed_time)

            prev_alt = start_alt

            dplus_brut = 0.0
            dminus_brut = 0.0
            hr_sum = 0.0
            hr_count = 0

            j = start_idx + 1
            last_idx = start_idx

            while j < n_pts:
                p = points[j]

                if p.altitude is None or p.distance is None or p.elapsed_time is None:
                    break

                if p.heartrate is not None:
                    try:
                        hr_val = float(p.heartrate)
                        hr_sum += hr_val
                        hr_count += 1
                    except (TypeError, ValueError):
                        pass

                alt = float(p.altitude)
                diff = alt - prev_alt
                if diff > 0:
                    dplus_brut += diff
                elif diff < 0:
                    dminus_brut += -diff
                prev_alt = alt

                last_idx = j

                if dplus_brut > 0 and dminus_brut > dplus_brut * MAX_DMINUS_RATIO:
                    break

                j += 1

            end_idx = last_idx
            end_p = points[end_idx]

            if end_idx <= start_idx or (end_idx - start_idx + 1) < MIN_POINTS:
                i = end_idx + 1
                continue

            end_alt = float(end_p.altitude)
            end_dist = float(end_p.distance)
            end_time = float(end_p.elapsed_time)

            longueur_m = max(0.0, end_dist - start_dist)
            net_up = max(dplus_brut - dminus_brut, 0.0)

            if longueur_m < MIN_DIST_M or dplus_brut < MIN_DPLUS_M:
                i = end_idx + 1
                continue

            pente_moy_pct = (net_up / longueur_m * 100.0) if longueur_m > 0 else 0.0
            if abs(pente_moy_pct) < MIN_PENTE_PCT:
                i = end_idx + 1
                continue

            dur_sec = end_time - start_time if end_time is not None and start_time is not None else 0.0

            avg_vam = None
            if dur_sec > 0 and net_up > 0:
                avg_vam = (net_up / dur_sec) * 3600.0

            avg_pace_str = None
            if dur_sec > 0 and longueur_m > 0:
                sec_per_km = dur_sec / (longueur_m / 1000.0)
                if sec_per_km > 0:
                    avg_pace_str = format_pace(sec_per_km)

            hr_avg = None
            if hr_count > 0:
                hr_avg = hr_sum / hr_count

            climbs.append({
                "start_idx": start_idx,
                "end_idx": end_idx,
                "km_debut": start_dist / 1000.0,
                "km_fin": end_dist / 1000.0,
                "longueur_m": longueur_m,
                "dplus_brut": dplus_brut,
                "dminus_brut": dminus_brut,
                "denivele_m": net_up,
                "pente_moy_pct": pente_moy_pct,
                "avg_vam": avg_vam,
                "avg_pace": avg_pace_str,
                "duration_sec": dur_sec,
                "hr_sum": hr_sum,
                "hr_count": hr_count,
                "hr_avg": hr_avg,
            })

            i = end_idx + 1

        climbs = sorted(climbs, key=lambda c: c["km_debut"])
        merged = []
        for c in climbs:
            if not merged:
                merged.append(c)
                continue

            prev = merged[-1]
            gap_km = c["km_debut"] - prev["km_fin"]

            if gap_km <= MERGE_GAP_KM:
                prev["end_idx"] = c["end_idx"]
                prev["km_fin"] = c["km_fin"]
                prev["longueur_m"] += c["longueur_m"]
                prev["dplus_brut"] += c["dplus_brut"]
                prev["dminus_brut"] += c["dminus_brut"]
                prev["denivele_m"] = max(prev["dplus_brut"] - prev["dminus_brut"], 0.0)

                prev["duration_sec"] += c.get("duration_sec", 0.0)
                prev["hr_sum"] += c.get("hr_sum", 0.0)
                prev["hr_count"] += c.get("hr_count", 0)

                if prev["longueur_m"] > 0:
                    prev["pente_moy_pct"] = (prev["denivele_m"] / prev["longueur_m"]) * 100.0

                dur = prev["duration_sec"]
                if dur > 0 and prev["denivele_m"] > 0:
                    prev["avg_vam"] = (prev["denivele_m"] / dur) * 3600.0
                else:
                    prev["avg_vam"] = None

                if dur > 0 and prev["longueur_m"] > 0:
                    sec_per_km = dur / (prev["longueur_m"] / 1000.0)
                    prev["avg_pace"] = format_pace(sec_per_km) if sec_per_km > 0 else None
                else:
                    prev["avg_pace"] = None

                if prev["hr_count"] > 0:
                    prev["hr_avg"] = prev["hr_sum"] / prev["hr_count"]
                else:
                    prev["hr_avg"] = None
            else:
                merged.append(c)

        climbs = merged[:10]

        # --- 13) Montée sélectionnée (via query params) ---
        selected_climb = None
        climb_start_param = request.query_params.get("climb_start")
        climb_end_param = request.query_params.get("climb_end")

        if climb_start_param is not None and climb_end_param is not None:
            try:
                cs = int(climb_start_param)
                ce = int(climb_end_param)
            except ValueError:
                cs = ce = None

            if cs is not None and ce is not None:
                for c in climbs:
                    if c["start_idx"] == cs and c["end_idx"] == ce:
                        selected_climb = c
                        break

        if selected_climb is None and climbs:
            selected_climb = climbs[0]

        # --- 14 bis SPLITS : 1 / 5 / 10 / 20 km ---

        split_km_options = [1, 5, 10, 20]

        split_km_param = request.query_params.get("split_km", "1")
        try:
            split_km = float(split_km_param)
        except ValueError:
            split_km = 1.0

        if split_km not in split_km_options:
            split_km = 1.0

        split_m = split_km * 1000.0

        splits_rows: list[dict] = []

        if total_dist_m > 0 and points:
            num_splits = int(math.ceil(total_dist_m / split_m))

            def format_pace_for_split(sec_per_km: float | None) -> str | None:
                if sec_per_km is None or sec_per_km <= 0:
                    return None
                s = int(round(sec_per_km))
                m, s = divmod(s, 60)
                return f"{m:d}:{s:02d}/km"

            def format_time_hms(sec: float | None) -> str:
                if sec is None or sec <= 0:
                    return "–"
                sec = int(round(sec))
                h, rem = divmod(sec, 3600)
                m, s = divmod(rem, 60)
                if h:
                    return f"{h}h{m:02d}"
                else:
                    return f"{m:02d}:{s:02d}"

            # Accumulateurs par split
            acc = []
            for i in range(num_splits):
                start_m = i * split_m
                end_m = min((i + 1) * split_m, total_dist_m)
                acc.append({
                    "start_km": start_m / 1000.0,
                    "end_km": end_m / 1000.0,
                    "hr_vals": [],
                    "cad_vals": [],
                    "gly_vals": [],
                    "vam_vals": [],
                    "first_dist": None,
                    "first_time": None,
                    "last_dist": None,
                    "last_time": None,
                    "prev_alt": None,
                    "dplus": 0.0,
                    "dminus": 0.0,
                })

            # Parcours des points
            for p in points:
                if p.distance is None:
                    continue

                d = float(p.distance)
                idx_split = int(d // split_m)
                if idx_split < 0 or idx_split >= num_splits:
                    continue

                a = acc[idx_split]

                # FC
                if p.heartrate is not None:
                    a["hr_vals"].append(float(p.heartrate))

                # Cadence -> ppm
                if p.cadence is not None:
                    a["cad_vals"].append(float(p.cadence) * 2.0)

                # Glycémie
                if p.glucose_mgdl is not None:
                    a["gly_vals"].append(float(p.glucose_mgdl))

                # Vitesse ascensionnelle
                if p.vertical_speed_m_per_h is not None:
                    a["vam_vals"].append(float(p.vertical_speed_m_per_h))

                # Dist / temps pour allure
                if p.elapsed_time is not None and p.distance is not None:
                    t = float(p.elapsed_time)
                    if a["first_dist"] is None:
                        a["first_dist"] = d
                        a["first_time"] = t
                    a["last_dist"] = d
                    a["last_time"] = t

                # D+ / D-
                if p.altitude is not None:
                    alt = float(p.altitude)
                    if a["prev_alt"] is not None:
                        diff = alt - a["prev_alt"]
                        if diff > 0:
                            a["dplus"] += diff
                        elif diff < 0:
                            a["dminus"] += -diff
                    a["prev_alt"] = alt

            cum_dplus = 0.0
            cum_dminus = 0.0

            for idx, a in enumerate(acc):
                # Colonne 1 : taille de split (1, 5, 10, 20)
                split_km_value = split_km

                dist_seg_m = None
                dur_seg_s = None
                pace_str = None

                if (
                    a["first_dist"] is not None and a["last_dist"] is not None
                    and a["last_dist"] > a["first_dist"]
                    and a["first_time"] is not None and a["last_time"] is not None
                    and a["last_time"] > a["first_time"]
                ):
                    dist_seg_m = a["last_dist"] - a["first_dist"]
                    dur_seg_s = a["last_time"] - a["first_time"]
                    sec_per_km = dur_seg_s / (dist_seg_m / 1000.0)
                    pace_str = format_pace_for_split(sec_per_km)

                hr_avg = round(sum(a["hr_vals"]) / len(a["hr_vals"])) if a["hr_vals"] else None
                cad_avg = round(sum(a["cad_vals"]) / len(a["cad_vals"])) if a["cad_vals"] else None
                gly_avg_split = round(sum(a["gly_vals"]) / len(a["gly_vals"])) if a["gly_vals"] else None

                # VAM split (si D- ≤ 5% D+)
                vam_seg = None
                if dur_seg_s and a["dplus"] > 0:
                    if a["dminus"] <= 0.05 * a["dplus"]:
                        vam_seg = round((a["dplus"] / dur_seg_s) * 3600.0)

                # VAM max 1'
                vam_max_1min = round(max(a["vam_vals"])) if a["vam_vals"] else None

                cum_dplus += a["dplus"]
                cum_dminus += a["dminus"]

                splits_rows.append({
                    "split_km": split_km_value,             # ex : 1, 5, 10, 20 (colonne 1)
                    "split_cum_km": a["end_km"],            # ✅ vraie distance cumulée (10, 20, 26.3…)
                    "dist_km": (dist_seg_m / 1000.0) if dist_seg_m else None,
                    "time_str": format_time_hms(dur_seg_s),
                    "pace": pace_str,
                    "hr_avg": hr_avg,
                    "cad_avg": cad_avg,
                    "gly_avg": gly_avg_split,
                    "dplus": int(round(a["dplus"])) if a["dplus"] else 0,
                    "dminus": int(round(a["dminus"])) if a["dminus"] else 0,
                    "dplus_cum": int(round(cum_dplus)),
                    "dminus_cum": int(round(cum_dminus)),
                    "vam_max_1min": vam_max_1min,
                    "vam_seg": vam_seg,
                })

        else:
            splits_rows = []




        # --- 14) Onglet courant (tab) 🔸 NOUVEAU ---
        tab = request.query_params.get("tab", "overview")
        if tab not in ("overview", "segments", "climbs", "cardio", "glycemia", "vam", "splits"):
            tab = "overview"

    finally:
        db.close()

    # --- RETURN PAGE ---
    return templates.TemplateResponse(
        "activity_detail.html",
        {
            "request": request,
            "user": user,
            "activity": activity,
            "dist_km": round(dist_km, 2),
            "dplus": dplus,
            "duration_sec": duration_sec,
            "fc": fc,
            "gly_avg": gly_avg,
            "level": level,
            "level_color": level_color,
            "gps_js": gps_js,
            "has_gps": has_gps,
            "activity_map_js": activity_map_js,
            "has_activity_map": has_activity_map,
            "slopes_order": slopes_order,
            "hr_zones": hr_zones,
            "vam_by_slope_zone": vam_by_slope_zone,
            "segments_km_labels": segments_km_labels,
            "progression_table": progression_table,
            "hr_by_segment": hr_by_segment,
            "current_seg_count": seg_count,
            "pace_by_segment": pace_by_segment,
            "dplus_by_segment": dplus_by_segment,
            "dminus_by_segment": dminus_by_segment,
            "climbs": climbs,
            "selected_climb": selected_climb,
            "alt_profile_js": alt_profile_js,
            "profile_chart_points_js": profile_chart_points_js,
            "terrain_summary": terrain_summary,
            "tab": tab,
            "has_glucose": has_glucose,
            "glucose_zone_rows": glucose_zone_rows,
            "glucose_chart_points": glucose_chart_points,
            "glucose_zone_vs_hr_rows": glucose_zone_vs_hr_rows,
            "glucose_hr_columns": glucose_hr_columns,
            "glucose_profile_summary": glucose_profile_summary,
            "hr_zone_summary": hr_zone_summary,
            "activity_type_summary": activity_type_summary,
            "story_export_data": story_export_data,
            "has_vam": has_vam,
            "vam_zone_rows": vam_zone_rows,
            "vam_hr_filter": vam_hr_filter,             # 🔸 on envoie l’onglet au template
            "split_km": split_km,
            "split_km_options": split_km_options,
            "splits_rows": splits_rows,
            "is_running_activity": is_running_activity,
            "cardiac_drift": cardiac_drift,

        }
    )


@app.get("/ui/user/{user_id}/activity/{activity_id}/share", response_class=HTMLResponse)
async def ui_user_activity_share(user_id: int, activity_id: int, request: Request):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return HTMLResponse("Utilisateur introuvable", status_code=404)

        _maybe_refresh_glucose_for_page_view(db, user, page_name="activity_share")

        activity = (
            db.query(Activity)
            .filter(Activity.id == activity_id, Activity.user_id == user_id)
            .first()
        )
        if not activity:
            return HTMLResponse("Activité introuvable", status_code=404)

        points = (
            db.query(ActivityStreamPoint)
            .filter(ActivityStreamPoint.activity_id == activity.id)
            .order_by(ActivityStreamPoint.idx.asc())
            .all()
        )

        start_dt = _safe_dt(activity.start_date)
        glucose_chart_points = []
        route_points = []
        altitude_profile_points = []
        for point in points:
            if point.glucose_mgdl is None or point.elapsed_time is None:
                pass
            else:
                ts_iso = None
                if start_dt is not None:
                    ts_iso = (start_dt + dt.timedelta(seconds=float(point.elapsed_time))).isoformat()
                glucose_chart_points.append({
                    "elapsed_sec": float(point.elapsed_time),
                    "ts": ts_iso,
                    "mgdl": float(point.glucose_mgdl),
                })
            if point.lat is not None and point.lon is not None:
                route_points.append({
                    "lat": float(point.lat),
                    "lon": float(point.lon),
                    "altitude_m": float(point.altitude or 0.0),
                    "elapsed_sec": float(point.elapsed_time) if point.elapsed_time is not None else None,
                    "glucose_mgdl": float(point.glucose_mgdl) if point.glucose_mgdl is not None else None,
                })
            if point.distance is not None and point.altitude is not None:
                altitude_profile_points.append([
                    float(point.distance) / 1000.0,
                    float(point.altitude),
                ])

        if len(route_points) > 500:
            route_points = route_points[:: max(1, len(route_points) // 500)]
        if len(altitude_profile_points) > 500:
            altitude_profile_points = altitude_profile_points[:: max(1, len(altitude_profile_points) // 500)]

        club_data = build_club_payload(user.club_slug)
        share_show_club_logo = bool(
            user.settings.share_show_club_logo
        ) if getattr(user, "settings", None) and user.settings.share_show_club_logo is not None else False

        story_export_data = _build_story_export_data(
            activity,
            glucose_chart_points,
            route_points=route_points,
            altitude_profile_points=altitude_profile_points,
            club_data=club_data,
            share_show_club_logo=share_show_club_logo,
        )
        return templates.TemplateResponse(
            "activity_share.html",
            {
                "request": request,
                "user": user,
                "activity": activity,
                "story_export_data": story_export_data,
            },
        )
    finally:
        db.close()




#-------------------------------------------------------------------------------
# SUPPRESSION D’UNE ACTIVITÉ
#-------------------------------------------------------------------------------
@app.post("/ui/user/{user_id}/activity/{activity_id}/delete", response_class=HTMLResponse)
def ui_user_activity_delete(request: Request, user_id: int, activity_id: int):
    """
    Supprime une activité (et ses points de stream) pour un utilisateur donné,
    puis redirige vers le dashboard utilisateur.
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        activity = (
            db.query(Activity)
            .filter(
                Activity.user_id == user_id,
                Activity.id == activity_id,
            )
            .first()
        )

        if not activity:
            return RedirectResponse(
                url=f"/ui/user/{user_id}",
                status_code=303,
            )

        delete_activity_live_data(
            db,
            activity=activity,
            rebuild_month_cache=True,
        )
        db.commit()

    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}",
        status_code=303,
    )


# -----------------------------------------------------------------------------
# Déconnexion Strava / Libre / Dexcom
# -----------------------------------------------------------------------------

@app.post("/ui/user/{user_id}/strava/disconnect")
def ui_strava_disconnect(request: Request, user_id: int):
    """
    Supprime les tokens Strava pour cet utilisateur
    et le considère comme 'non connecté à Strava'.
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        db.query(StravaToken).filter(StravaToken.user_id == user_id).delete()
        db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile",
        status_code=303,
    )


@app.post("/ui/user/{user_id}/dexcom/credentials")
def ui_dexcom_credentials_save(
    request: Request,
    user_id: int,
    username: str = Form(""),
    password: str = Form(""),
    region: str = Form(""),
):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    username = (username or "").strip()
    password = password or ""
    region_value = (region or settings.DEXCOM_SHARE_REGION_DEFAULT or "ous").strip().lower()

    if not username or not password:
        msg = quote_plus("Identifiant et mot de passe Dexcom Share requis.")
        return RedirectResponse(
            url=f"/ui/user/{user_id}/profile?dexcom_status=error&dexcom_msg={msg}#dexcom",
            status_code=303,
        )

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        existing_records = (
            db.query(DexcomToken)
            .filter(DexcomToken.user_id == user_id)
            .order_by(DexcomToken.id.desc())
            .all()
        )
        status, msg = test_dexcom_credentials(
            username=username,
            password=password,
            region=region_value,
            user_id=user_id,
        )
        if status == "error":
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?dexcom_status={status}&dexcom_msg={quote_plus(msg)}#dexcom",
                status_code=303,
            )

        token = _get_dexcom_share_record(existing_records)
        if token is None:
            token = existing_records[0] if existing_records else DexcomToken(
                user_id=user_id,
                access_token="",
                refresh_token="",
                expires_at=0,
            )
            if token.id is None:
                db.add(token)

        token.share_username = username
        token.share_password = encrypt_secret(password)
        token.share_region = region_value
        token.access_token = token.access_token or ""
        token.refresh_token = token.refresh_token or ""
        token.expires_at = token.expires_at or 0
        if get_active_glucose_source(user) is None:
            set_active_glucose_source(user, "dexcom")
        db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile?dexcom_status={status}&dexcom_msg={quote_plus(msg)}#dexcom",
        status_code=303,
    )


@app.post("/ui/user/{user_id}/dexcom/test")
def ui_dexcom_test_connection(request: Request, user_id: int):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            msg = quote_plus("Utilisateur introuvable.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?dexcom_status=error&dexcom_msg={msg}#dexcom",
                status_code=303,
            )
        result = test_provider_connection(user, "dexcom")
    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile?dexcom_status={result.status}&dexcom_msg={quote_plus(result.message)}#dexcom",
        status_code=303,
    )


@app.post("/ui/user/{user_id}/carelink/credentials")
def ui_carelink_credentials_save(
    request: Request,
    user_id: int,
    username: str = Form(""),
    password: str = Form(""),
    region: str = Form("EU"),
):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    username = (username or "").strip()
    region_value = (region or "EU").strip().upper()
    if region_value not in {"EU", "US"}:
        region_value = "EU"

    if not username:
        msg = quote_plus("Identifiant CareLink requis.")
        return RedirectResponse(
            url=f"/ui/user/{user_id}/profile?carelink_status=error&carelink_msg={msg}#carelink",
            status_code=303,
        )

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        cred = user.carelink_credentials or CareLinkCredential(
            user_id=user_id,
            username=username,
        )
        if cred.id is None:
            db.add(cred)

        cred.username = username
        cred.region = region_value
        cred.password_encrypted = encrypt_secret(password) if password else cred.password_encrypted
        if not cred.status or cred.status == "not_configured":
            cred.status = "needs_reauth"
        if not cred.error_message:
            cred.error_message = (
                "Cette intégration est expérimentale et dépend de CareLink Connect. "
                "Elle peut nécessiter une nouvelle authentification si Medtronic modifie son système."
            )
        if get_active_glucose_source(user) is None:
            set_active_glucose_source(user, "medtronic_carelink")
        db.commit()

        result = test_carelink_connection(user)
        cred.status = result.status
        cred.error_message = None if result.ok else result.message
        if result.last_sync_at:
            cred.last_sync_at = result.last_sync_at
        db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=(
            f"/ui/user/{user_id}/profile?carelink_status={result.status}"
            f"&carelink_msg={quote_plus(result.message)}#carelink"
        ),
        status_code=303,
    )


@app.post("/ui/user/{user_id}/carelink/test")
def ui_carelink_test_connection(request: Request, user_id: int):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            msg = quote_plus("Utilisateur introuvable.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?carelink_status=error&carelink_msg={msg}#carelink",
                status_code=303,
            )
        result = test_carelink_connection(user)
        if user.carelink_credentials:
            user.carelink_credentials.status = result.status
            user.carelink_credentials.error_message = None if result.ok else result.message
            if result.last_sync_at:
                user.carelink_credentials.last_sync_at = result.last_sync_at
            db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=(
            f"/ui/user/{user_id}/profile?carelink_status={result.status}"
            f"&carelink_msg={quote_plus(result.message)}#carelink"
        ),
        status_code=303,
    )


@app.post("/ui/user/{user_id}/nightscout/credentials")
def ui_nightscout_credentials_save(
    request: Request,
    user_id: int,
    url: str = Form(""),
    read_token: str = Form(""),
):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    try:
        normalized_url = normalize_base_url(url)
    except Exception as exc:  # noqa: BLE001
        msg = quote_plus(str(exc))
        return RedirectResponse(
            url=f"/ui/user/{user_id}/profile?nightscout_status=error&nightscout_msg={msg}#nightscout",
            status_code=303,
        )

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        cred = user.nightscout_credentials or NightscoutCredential(user_id=user_id, base_url=normalized_url)
        if cred.id is None:
            db.add(cred)

        cred.base_url = normalized_url
        if (read_token or "").strip():
            cred.read_token_encrypted = encrypt_secret(read_token.strip())
        cred.last_error_message = None

        if get_active_glucose_source(user) is None:
            set_active_glucose_source(user, "nightscout")

        db.commit()
    finally:
        db.close()

    msg = quote_plus("Configuration Nightscout enregistrée.")
    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile?nightscout_status=ok&nightscout_msg={msg}#nightscout",
        status_code=303,
    )


@app.post("/ui/user/{user_id}/nightscout/test")
def ui_nightscout_test_connection(request: Request, user_id: int):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            msg = quote_plus("Utilisateur introuvable.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?nightscout_status=error&nightscout_msg={msg}#nightscout",
                status_code=303,
            )

        result = test_provider_connection(user, "nightscout")
        if user.nightscout_credentials:
            user.nightscout_credentials.last_error_message = None if result.ok else result.message
            if result.last_sync_at:
                user.nightscout_credentials.last_success_at = result.last_sync_at
            db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=(
            f"/ui/user/{user_id}/profile?nightscout_status={result.status}"
            f"&nightscout_msg={quote_plus(result.message)}#nightscout"
        ),
        status_code=303,
    )


@app.post("/ui/user/{user_id}/glucose-source")
def ui_set_glucose_source(
    request: Request,
    user_id: int,
    provider: str = Form(""),
):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    provider = (provider or "").strip().lower()
    anchor = _provider_anchor(provider)

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            msg = quote_plus("Utilisateur introuvable.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?{anchor}_status=error&{anchor}_msg={msg}",
                status_code=303,
            )
        if provider not in {"abbott", "dexcom", "medtronic_carelink", "nightscout"}:
            msg = quote_plus("Source glycémique invalide.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?{anchor}_status=error&{anchor}_msg={msg}#{anchor}",
                status_code=303,
            )
        if not _provider_is_configured(user, provider):
            label = get_glucose_source_label(provider)
            msg = quote_plus(f"Configure d'abord {label} avant de l'activer.")
            return RedirectResponse(
                url=f"/ui/user/{user_id}/profile?{anchor}_status=warn&{anchor}_msg={msg}#{anchor}",
                status_code=303,
            )

        set_active_glucose_source(user, provider)
        db.commit()
    finally:
        db.close()

    label = quote_plus(f"{get_glucose_source_label(provider)} est maintenant la source active.")
    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile?{anchor}_status=ok&{anchor}_msg={label}#{anchor}",
        status_code=303,
    )


@app.post("/ui/user/{user_id}/libre/disconnect")
def ui_libre_disconnect(request: Request, user_id: int):
    """
    Supprime les identifiants LibreLinkUp pour cet utilisateur.
    L'historique glycémie (glucose_points) est conservé pour l'instant.
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        db.query(LibreCredentials).filter(LibreCredentials.user_id == user_id).delete()
        user = db.query(User).get(user_id)
        if user and get_active_glucose_source(user) == "abbott":
            set_active_glucose_source(user, None)
        db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile#libre",
        status_code=303,
    )


@app.post("/ui/user/{user_id}/carelink/disconnect")
def ui_carelink_disconnect(request: Request, user_id: int):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        db.query(CareLinkCredential).filter(CareLinkCredential.user_id == user_id).delete()
        user = db.query(User).get(user_id)
        if user and get_active_glucose_source(user) == "medtronic_carelink":
            set_active_glucose_source(user, None)
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url=f"/ui/user/{user_id}/profile#carelink", status_code=303)


@app.post("/ui/user/{user_id}/nightscout/disconnect")
def ui_nightscout_disconnect(request: Request, user_id: int):
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        db.query(NightscoutCredential).filter(NightscoutCredential.user_id == user_id).delete()
        user = db.query(User).get(user_id)
        if user and get_active_glucose_source(user) == "nightscout":
            set_active_glucose_source(user, None)
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url=f"/ui/user/{user_id}/profile#nightscout", status_code=303)


@app.post("/ui/user/{user_id}/dexcom/disconnect")
def ui_dexcom_disconnect(request: Request, user_id: int):
    """
    Supprime les tokens Dexcom pour cet utilisateur.
    L'historique glycémie (glucose_points) est conservé.
    Si l'utilisateur avait cgm_source='dexcom', on bascule en mode Auto (None).
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        db.query(DexcomToken).filter(DexcomToken.user_id == user_id).delete()
        user = db.query(User).get(user_id)
        if user and get_active_glucose_source(user) == "dexcom":
            set_active_glucose_source(user, None)
        db.commit()
    finally:
        db.close()

    return RedirectResponse(url=f"/ui/user/{user_id}/profile", status_code=303)


@app.post("/ui/user/{user_id}/delete-account", response_class=HTMLResponse)
def ui_user_delete_account(request: Request, user_id: int):
    """
    Supprime définitivement le compte utilisateur et toutes ses données associées.
    """
    guard = _guard_user_route(request, user_id)
    if guard:
        return guard

    db = SessionLocal()
    try:
        user = db.query(User).get(user_id)
        if not user:
            return templates.TemplateResponse(
                "error.html",
                {
                    "request": request,
                    "title": "Utilisateur introuvable",
                    "message": f"Aucun utilisateur avec id={user_id}",
                    "back_url": "/ui/login",
                },
                status_code=404,
            )

        _delete_user_account_data(db, user)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

    request.session.clear()
    return RedirectResponse(url="/ui/login", status_code=303)

#-------------------------------------------------------------------------------
# IMPORT D’UNE ACTIVITÉ (API)
#-------------------------------------------------------------------------------
async def _process_imported_activity_in_background(filepath: str, suffix: str, user_id: int) -> None:
    """Exécute l'import hors de la requête HTTP pour garder l'UI réactive."""
    try:
        if suffix == ".gpx":
            await asyncio.to_thread(lambda: asyncio.run(enrich_activity_from_gpx(filepath, user_id=user_id)))
        else:
            await asyncio.to_thread(lambda: asyncio.run(enrich_activity_from_fit(filepath, user_id=user_id)))
    except Exception:
        logger.exception("[ACTIVITY_IMPORT] Échec import %s pour user_id=%s", suffix, user_id)
    finally:
        try:
            os.unlink(filepath)
        except FileNotFoundError:
            pass


@app.post("/api/users/{user_id}/import-activity")
async def import_activity(
    user_id: int,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    filename = file.filename or ""
    suffix = os.path.splitext(filename)[1].lower()
    if suffix not in {".gpx", ".fit"}:
        raise HTTPException(status_code=400, detail="Format non supporté : utilise un fichier GPX ou FIT.")

    # Le nom fourni par le navigateur ne doit jamais servir de chemin temporaire.
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, dir="/tmp") as temp_file:
        filepath = temp_file.name
        temp_file.write(await file.read())

    if suffix == ".fit" and FitFile is None:
        os.unlink(filepath)
        raise HTTPException(status_code=503, detail="La lecture des fichiers FIT n'est pas disponible sur ce serveur.")

    background_tasks.add_task(
        _process_imported_activity_in_background,
        filepath,
        suffix,
        user_id,
    )

    return {
        "status": "queued",
        "message": "Import lancé en arrière-plan",
    }
