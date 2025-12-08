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
import datetime as dt
import subprocess
import json
from typing import Optional
import statistics
import threading
import shutil
from datetime import datetime, timedelta
from collections import Counter
import re
from sqlalchemy import desc, func
from sqlalchemy.orm import Session

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
)
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from .logic import (
    select_window,
    compute_stats,
    merge_desc,
    compute_hr_zones,
    compute_user_fc_max,
    _format_duration,
    upsert_activity_record,
    save_activity_stream_points,
    match_glucose_to_time_stream,
    compute_and_store_zone_slope_aggs,
    compute_difficulty_and_level,   # 👈 AJOUT
    build_runner_profile,
    build_fatigue_profile,
    compute_best_dplus_windows,
    HR_ZONES,
)
from .settings import settings
from .strava_client import StravaClient
from .libre_client import read_graph
from .dexcom_client import DexcomClient
from app.database import SessionLocal, init_db, get_db
from app.models import (
    StravaToken,
    LibreCredentials,
    User,
    Activity,
    ActivityStreamPoint,
    GlucosePoint,
    DexcomToken,
    UserSettings,
    ActivityVamPeak,
    ActivityZoneSlopeAgg
)
from app import auth
from app import models
from app.auth import pwd_context
from app.cgm_service import run_polling_loop
from app.indicators.slope_cadence import build_slope_cadence_data
from app.routers import auth_strava, auth_dexcom, webhooks

from statistics import mean
import math
import xml.etree.ElementTree as ET
import os

# Helper pour s'assurer que les datetime sont bien tz-aware
def _safe_dt(ts):
    return ts if (ts is None or ts.tzinfo is not None) else ts.replace(tzinfo=dt.timezone.utc)


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


def _guard_user_route(request: Request, user_id: int | None = None):
    session_user_id = _get_session_user_id(request)
    if session_user_id is None:
        return RedirectResponse(url="/ui/login", status_code=302)

    if user_id is not None and session_user_id != int(user_id):
        return RedirectResponse(url=f"/ui/user/{session_user_id}", status_code=302)

    return None


def _guard_admin(request: Request):
    session_user_id = _get_session_user_id(request)
    if session_user_id is None:
        return RedirectResponse(url="/ui/login", status_code=302)
    if session_user_id != 1:
        return RedirectResponse(url=f"/ui/user/{session_user_id}", status_code=302)
    return None


# -----------------------------------------------------------------------------
# Instance FastAPI + static + templates
# -----------------------------------------------------------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY, same_site="lax")

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
        return [], None

    def try_libre():
        try:
            g = read_graph(user_id=user_id) or []
            if g:
                print(f"[CGM] {len(g)} points obtenus via LibreLinkUp pour user_id={user_id}")
            return g
        except Exception as e:
            print(f"[CGM] Erreur LibreLinkUp pour user_id={user_id} :", e)
            return []

    def try_dexcom():
        try:
            cli = DexcomClient(user_id=user_id, db=db)
            # Dexcom: préfère souvent des datetimes *aware* en UTC
            start_aw = start if start.tzinfo is not None else start.replace(tzinfo=dt.timezone.utc)
            end_aw   = end   if end.tzinfo   is not None else end.replace(tzinfo=dt.timezone.utc)
            g = cli.get_graph(start=start_aw, end=end_aw) or []
            if g:
                print(f"[CGM] {len(g)} points obtenus via Dexcom pour user_id={user_id}")
            return g
        except Exception as e:
            print(f"[CGM] Erreur Dexcom pour user_id={user_id} :", e)
            return []

    graph = []
    source_label: Optional[str] = None

    if user.cgm_source == "libre":
        graph = try_libre()
        if graph:
            source_label = "libre"
        else:
            graph = try_dexcom()
            if graph:
                source_label = "dexcom"
    elif user.cgm_source == "dexcom":
        graph = try_dexcom()
        if graph:
            source_label = "dexcom"
        else:
            graph = try_libre()
            if graph:
                source_label = "libre"
    else:
        # Auto / None → priorité Dexcom puis Libre
        graph = try_dexcom()
        if graph:
            source_label = "dexcom"
        else:
            graph = try_libre()
            if graph:
                source_label = "libre"

    return graph, source_label


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

    for p in stream:
        if p.elapsed_time is None or p.altitude is None:
            continue
        times.append(float(p.elapsed_time))
        alts.append(float(p.altitude))
        ts_abs.append(p.timestamp)

    if len(times) < 2:
        print("⚠️ Données incomplètes pour calculer les VAM (times/altitude).")
        return

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
        # 1) Activité -> bornes temps (aware UTC)
        start_raw = dt.datetime.fromisoformat(act["start_date"].replace("Z", "+00:00"))
        elapsed = act.get("elapsed_time") or act.get("moving_time") or 0
        end_raw = start_raw + dt.timedelta(seconds=int(elapsed))

        start_aw = to_utc_aware(start_raw)
        end_aw   = to_utc_aware(end_raw)

        # 2) Glycémie depuis la BASE -> on récupère en AWARE UTC
        graph_db = load_glucose_graph_from_db(
            db, user_id=user_id, start=start_aw, end=end_aw, margin_min=10
        )

        # Fallback : si trop peu de points, on lit “live”, on insère, puis on recharge DB
        if not graph_db or len(graph_db) < 2:
            try:
                # Les clients externes préfèrent souvent des datetimes aware
                graph_live, source_label = get_cgm_graph_for_user(
                    db=db, user_id=user_id, start=start_aw, end=end_aw
                )
                if graph_live:
                    src = "dexcom" if source_label == "dexcom" else "archive_libre"
                    # L’insertion normalise côté DB (peu importe), on rechargera en aware
                    store_glucose_points_from_graph(db, user_id=user_id, points=graph_live, source=src)
                    graph_db = load_glucose_graph_from_db(
                        db, user_id=user_id, start=start_aw, end=end_aw, margin_min=10
                    )
            except Exception as e:
                print(f"[CGM] Fallback live KO : {e}")

        # 3) Streams + matching CGM
        time_stream = streams.get("time", {}).get("data") or []
        hr_stream   = streams.get("heartrate", {}).get("data") or []

        # start_aw est AWARE UTC ; graph_db contient des ts AWARE UTC
        g_vals, g_trends, g_sources = match_glucose_to_time_stream(
            graph=graph_db,
            start=start_aw,
            time_stream=time_stream,
            max_delta_sec=600,
        )
        streams["glucose_mgdl"]   = {"data": g_vals}
        streams["glucose_trend"]  = {"data": g_trends}
        streams["glucose_source"] = {"data": g_sources}

        # 4) Upsert activité + stats
        athlete_id = act.get("athlete", {}).get("id")
        if athlete_id is None:
            print("⚠️ athlete_id manquant, arrêt.")
            return

        # select_window utilise start/end -> on passe AWARE UTC
        samples = select_window(graph_db, start_aw, end_aw, buffer_min=5)
        stats = compute_stats(samples)

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

        # 6) Sections dans l'ordre imposé : Gly → VAM → Allure → Cadence
        # -----------------------------------------------------------------
        settings = db.query(UserSettings).filter(UserSettings.user_id == user_id).one_or_none()
        if settings is None:
            class _S:
                desc_include_glycemia = True
                desc_include_vam = False
                desc_include_pace = False
                desc_include_cadence = False
            settings = _S()

        # Glycémie
        gly_section_lines = []
        if settings.desc_include_glycemia:
            if stats:
                gly_section_lines.append(stats["block"])
            else:
                gly_section_lines.append(
                    "🔬Glycémie : Aucune donnée disponible sur cette période\n"
                    "(Les données CGM ne couvrent peut-être pas la fenêtre de l'activité.)"
                )

        # VAM (lire les max déjà stockés sur l'activité)
        vam_section_lines = []
        if settings.desc_include_vam:
            a = db.query(Activity).get(activity_obj.id)
            l = []
            if a and a.max_vam_5m:  l.append(f"5′ : {round(a.max_vam_5m)} m/h")
            if a and a.max_vam_15m: l.append(f"15′ : {round(a.max_vam_15m)} m/h")
            if a and a.max_vam_30m: l.append(f"30′ : {round(a.max_vam_30m)} m/h")
            vam_section_lines.append("⛰️ VAM : " + (" | ".join(l) if l else "n/a"))

        # Allure / vitesse (moyenne simple)
        pace_section_lines = []
        if settings.desc_include_pace:
            agg = (
                db.query(func.avg(ActivityStreamPoint.velocity))
                .filter(ActivityStreamPoint.activity_id == activity_obj.id)
                .one()
            )
            avg_v_ms = agg[0] if agg and agg[0] is not None else None
            if avg_v_ms and avg_v_ms > 0:
                pace_s_per_km = 1000.0 / float(avg_v_ms)
                m = int(pace_s_per_km // 60)
                s = int(round(pace_s_per_km % 60))
                pace_section_lines.append(f"🏃 Allure moy : {m}:{s:02d} /km (v={avg_v_ms:.2f} m/s)")
            else:
                pace_section_lines.append("🏃 Allure : n/a")

        # Cadence (moyenne simple)
        cad_section_lines = []
        if settings.desc_include_cadence:
            agg = (
                db.query(func.avg(ActivityStreamPoint.cadence))
                .filter(ActivityStreamPoint.activity_id == activity_obj.id)
                .one()
            )
            avg_cad = agg[0] if agg and agg[0] is not None else None
            cad_section_lines.append(f"🔁 Cadence moy : {round(avg_cad)} spm" if avg_cad else "🔁 Cadence : n/a")

        # Assemblage final (VAM → Allure → Cadence → Gly)
        blocks_ordered = []
        if vam_section_lines:  blocks_ordered.append("\n".join(vam_section_lines))
        if pace_section_lines: blocks_ordered.append("\n".join(pace_section_lines))
        if cad_section_lines:  blocks_ordered.append("\n".join(cad_section_lines))
        if gly_section_lines:  blocks_ordered.append("\n".join(gly_section_lines))

        # Ajoute la signature uniquement s'il y a du contenu
        if blocks_ordered:
            blocks_ordered.append("—> Made with ❤️ by Benoit")

        full_block = "\n".join(blocks_ordered)

        # 7) Mise à jour Strava + persistance du block (optionnel)
        if update_strava_description and cli is not None and activity_id is not None:
            new_desc = merge_desc(act.get("description") or "", full_block)
            await cli.update_activity_description(activity_id, new_desc)

        activity_obj.glucose_summary_block = full_block
        db.add(activity_obj)
        db.commit()

    finally:
        db.close()



# -----------------------------------------------------------------------------
# Orchestrateur : enrichir une activité Strava (wrapper autour du core)
# -----------------------------------------------------------------------------
async def enrich_activity(activity_id: int, user_id: int = 1):
    cli = StravaClient(user_id=user_id)

    # 1) On récupère act + streams depuis Strava
    act = await cli.get_activity(activity_id)
    streams = await cli.get_streams(activity_id)

    # 2) On délègue tout le boulot à la fonction générique
    await process_activity_core(
        act=act,
        streams=streams,
        user_id=user_id,
        activity_id=activity_id,
        cli=cli,
        update_strava_description=True,  # ici on veut MAJ la description Strava
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
    fake_id = int(start_ts.timestamp()) * 100 + int(user_id)

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

    await process_activity_core(
        act=act,
        streams=streams,
        user_id=user_id,
        activity_id=None,                  # pas d'ID Strava
        cli=None,                          # pas de client Strava
        update_strava_description=False,   # on ne met pas à jour une activité Strava
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

    # 2️⃣ Lecture des données LibreLinkUp (via ton script Node.js)
    try:
        result = subprocess.run(
            ["node", "libre_node/reader.mjs"],
            capture_output=True,
            text=True,
            timeout=20
        )
        result.check_returncode()
        glucose_data = json.loads(result.stdout)
    except Exception as e:
        return {"error": f"Erreur lecture LibreLinkUp : {e}"}

    # 3️⃣ Filtrage des points pendant la durée de l’activité (+/- 5 min)
    points = []
    for p in glucose_data:
        ts = datetime.fromisoformat(p["ts"].replace("Z", "+00:00"))
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

        cred = (
            db.query(LibreCredentials)
            .filter(LibreCredentials.user_id == user_id)
            .first()
        )

        if cred:
            cred.email = email
            cred.password_encrypted = password
            cred.region = region
        else:
            cred = LibreCredentials(
                user_id=user_id,
                email=email,
                password_encrypted=password,
                region=region,
            )
            db.add(cred)

        db.commit()
        db.refresh(cred)

    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile",
        status_code=302,
    )




# -----------------------------------------------------------------------------
# MINI INTERFACE WEB
# -----------------------------------------------------------------------------



@app.get("/ui", response_class=HTMLResponse)
def ui_home(request: Request):
    guard = _guard_admin(request)
    if guard:
        return guard

    db = SessionLocal()
    try:
        users = db.query(User).all()
        ui_users = []
        for u in users:
            ui_users.append({
                "id": u.id,
                "email": u.email,
                "has_strava": bool(u.strava_tokens),
                "libre_email": u.libre_credentials.email if u.libre_credentials else None,
            })
    finally:
        db.close()

    return templates.TemplateResponse(
        "home.html",
        {"request": request, "users": ui_users},
    )

@app.get("/", response_class=HTMLResponse)
def home_redirect(request: Request):
    session_user_id = _get_session_user_id(request)
    if session_user_id == 1:
        return RedirectResponse(url="/ui")
    if session_user_id:
        return RedirectResponse(url=f"/ui/user/{session_user_id}")
    return _render_login_page(request)


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
        await enrich_activity(int(activity_id), user_id=user_id)
        msg = "Activité enrichie avec succès 🎉"
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

        # Statuts connexions
        has_strava = bool(user.strava_tokens)
        has_libre = user.libre_credentials is not None
        has_dexcom = bool(user.dexcom_tokens)

        libre_email = user.libre_credentials.email if user.libre_credentials else ""
        libre_region = user.libre_credentials.region if user.libre_credentials else ""

        strava_athlete_id = user.strava_tokens[0].athlete_id if user.strava_tokens else None
        cgm_source = user.cgm_source or ""

        # ✅ EAGER: lire les préférences et créer des bools simples
        settings = db.query(UserSettings).filter(UserSettings.user_id == user_id).one_or_none()
        include_vam = bool(settings.desc_include_vam) if settings else False
        include_pace = bool(settings.desc_include_pace) if settings else False
        include_cad  = bool(settings.desc_include_cadence) if settings else False
        include_gly  = bool(settings.desc_include_glycemia) if settings else False

        # On rend la page en passant des primitives (pas d’accès lazy après fermeture)
        ctx = {
            "request": request,
            "user": user,
            "has_strava": has_strava,
            "has_libre": has_libre,
            "has_dexcom": has_dexcom,
            "libre_email": libre_email,
            "libre_region": libre_region,
            "strava_athlete_id": strava_athlete_id,
            "cgm_source": cgm_source,
            # flags pour la template
            "include_vam": include_vam,
            "include_pace": include_pace,
            "include_cad": include_cad,
            "include_gly": include_gly,
        }
        return templates.TemplateResponse("user_profile.html", ctx)

    finally:
        db.close()


    return templates.TemplateResponse(
        "user_profile.html",
        {
            "request": request,
            "user": user,
            "has_strava": has_strava,
            "has_libre": has_libre,
            "has_dexcom": has_dexcom,
            "libre_email": libre_email,
            "libre_region": libre_region,
            "strava_athlete_id": strava_athlete_id,
            "cgm_source": cgm_source,
            "settings": settings,  # 👈 NEW: passé au template
        },
    )


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
    cgm_source: str = Form(""),      # "", "libre", "dexcom" (Auto/libre/Dexcom)
    profile_image: UploadFile | None = File(None),  # 👈 fichier uploadé

    # 👇 NEW : cases à cocher "Description Strava"
    desc_include_glycemia: bool = Form(False),
    desc_include_vam: bool = Form(False),
    desc_include_pace: bool = Form(False),
    desc_include_cadence: bool = Form(False),
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

        # -------- Source CGM --------
        cgm_source_val = (cgm_source or "").strip().lower()
        if cgm_source_val in ("libre", "dexcom"):
            user.cgm_source = cgm_source_val
        else:
            user.cgm_source = None  # "Auto" / vide

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

        settings.desc_include_glycemia = bool(desc_include_glycemia)
        settings.desc_include_vam      = bool(desc_include_vam)
        settings.desc_include_pace     = bool(desc_include_pace)
        settings.desc_include_cadence  = bool(desc_include_cadence)

        db.commit()
        db.refresh(user)

    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile",
        status_code=302,
    )



from fastapi import Query  # si pas déjà importé

@app.get("/ui/user/{user_id}/runner-profile", response_class=HTMLResponse)
def ui_runner_profile(
    request: Request,
    user_id: int,
    sport: str = Query("run"),
    period: str = Query("all"),           # "all" ou "last_12_months"
    tab: str = Query("ascent"),         # "ascent", "vam", "pace", ...
    hr_zone_fatigue: str = Query("all"),  # filtre zone cardio pour la fatigue
    db: Session = Depends(get_db),
):
    """
    Page 'profil coureur' :
    - tab=overview  : profil cardio × pente × VAM × allure × cadence
    - tab=fatigue   : dégradation de l’allure selon la durée de la sortie
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

    if period == "last_12_months":
        date_from = now_utc - dt.timedelta(days=365)
        # on laisse date_to à None => jusqu’à maintenant

    # 3) Profil coureur (zones × pente)
    profile = build_runner_profile(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )

    # 4) Profil fatigue (dégradation par durée)
    logic_hr_zone = None if hr_zone_fatigue == "all" else hr_zone_fatigue

    fatigue_profile = build_fatigue_profile(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
        hr_zone=logic_hr_zone,   # ✅ bon nom
    )


    # 5) Ordre des zones cardio + pentes (positives et négatives)
    hr_zone_names = [name for (name, _, _) in HR_ZONES]

    slopes_order = [
        ("Sneg40p",   "<-40%"),
        ("Sneg30_40", "-40 à -30%"),
        ("Sneg25_30", "-30 à -25%"),
        ("Sneg20_25", "-25 à -20%"),
        ("Sneg15_20", "-20 à -15%"),
        ("Sneg10_15", "-15 à -10%"),
        ("Sneg5_10",  "-10 à -5%"),
        ("Sneg0_5",   "-5 à 0%"),

        ("S0_5",   "0–5%"),
        ("S5_10",  "5–10%"),
        ("S10_15", "10–15%"),
        ("S15_20", "15–20%"),
        ("S20_25", "20–25%"),
        ("S25_30", "25–30%"),
        ("S30_40", "30–40%"),
        ("S40p",   ">40%"),
    ]

    # 6) Stats glycémie (temps passé dans les zones sur différentes fenêtres)
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

    glucose_chart_24h = [
        {
            "ts": _ts_iso(p.ts),
            "mgdl": float(p.mgdl),
        }
        for p in points_24h
        if p.mgdl is not None and p.ts is not None
    ]

    # 7) D+ max sur fenêtres glissantes
    best_dplus_windows = compute_best_dplus_windows(
        db,
        user_id=user_id,
        sport=sport,
        date_from=date_from,
        date_to=date_to,
    )

    return templates.TemplateResponse(
        "runner_profile.html",
        {
            "request": request,
            "user": user,
            "hr_zones": hr_zone_names,
            "slopes_order": slopes_order,
            "profile": profile,
            "sport": sport,
            "period": period,
            "tab": tab,
            "fatigue_profile": fatigue_profile,
            "hr_zone_fatigue": hr_zone_fatigue,
            "glucose_zone_summary": glucose_zone_summary,
            "glucose_chart_24h": glucose_chart_24h,
            "best_dplus_windows": best_dplus_windows,
        },
    )




# -----------------------------------------------------------------------------
# UI : Login
# -----------------------------------------------------------------------------

def _render_login_page(request: Request):
    hero_points = [
        {
            "title": "Fusion Strava × CGM",
            "detail": "Superpose fréquence cardiaque, puissance et glycémie sur chaque activité.",
        },
        {
            "title": "Alertes hypo / hyper",
            "detail": "Repère instantanément les passages critiques pendant tes sorties.",
        },
        {
            "title": "Coaching data-driven",
            "detail": "Analyse les montées, splits, VAM et temps en zone glycémie.",
        },
    ]
    onboarding_steps = [
        "Se connecter ou créer un compte Strava Glucose",
        "Lier Strava et ton capteur Dexcom / Libre",
        "Laisser l’appli enrichir automatiquement chaque activité",
    ]
    return templates.TemplateResponse(
        "login.html",
        {
            "request": request,
            "hero_points": hero_points,
            "onboarding_steps": onboarding_steps,
        },
    )


@app.get("/ui/login", response_class=HTMLResponse)
def ui_login_form(request: Request):
    """
    Page de connexion (UI).
    """
    return _render_login_page(request)


@app.post("/ui/login", response_class=HTMLResponse)
def ui_login(request: Request, email: str = Form(...), password: str = Form(...)):
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

    if not user or not pwd_context.verify(password, user.password_hash):
        return templates.TemplateResponse(
            "login_error.html",
            {"request": request, "email": email},
            status_code=401
        )

    request.session["user_id"] = int(user.id)
    return RedirectResponse(url=f"/ui/user/{user.id}", status_code=302)

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
        for a in recent:
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
            })

        # VAM 5/15/30 des dernières activités (via caches Activity)
        for it in last_activities:
            a_id = it["id"]
            a = db.query(Activity).get(a_id)
            it["vam_5"]  = float(a.max_vam_5m)  if a and a.max_vam_5m  is not None else None
            it["vam_15"] = float(a.max_vam_15m) if a and a.max_vam_15m is not None else None
            it["vam_30"] = float(a.max_vam_30m) if a and a.max_vam_30m is not None else None

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

        user_prs = []
        for (w, v, s, aid, start_dt) in user_prs_rows:
            if start_dt:
                date_str = start_dt.strftime("%d-%m-%Y")  # ← format JJ-MM-AAAA sans heure
            else:
                date_str = "n/a"
            user_prs.append({
                "window_min": w,
                "vam_m_per_h": float(v) if v is not None else None,
                "sport": s,
                "activity_id": aid,
                "date_str": date_str,
            })


        # ---------------------------
        # ⛰️ Tableau VAM par bandes de pente (min / moy / max)
        #     - filtre facultatif par zone cardio via ?hr_zone=all|Zone%201|...
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
            q.group_by(models.ActivityZoneSlopeAgg.slope_band)
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
            })

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

        activity = (
            db.query(Activity)
            .filter(Activity.id == activity_id, Activity.user_id == user_id)
            .first()
        )
        if not activity:
            return HTMLResponse("Activité introuvable", status_code=404)

        # --- 2) STREAM POINTS ---
        points = (
            db.query(ActivityStreamPoint)
            .filter(ActivityStreamPoint.activity_id == activity.id)
            .order_by(ActivityStreamPoint.idx.asc())
            .all()
        )
        has_streams = len(points) > 1

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

        # --- 4) GPS simplified ---
        gps = []
        for p in points:
            if p.lat is not None and p.lon is not None:
                gps.append([float(p.lat), float(p.lon)])
        if len(gps) > 300:
            gps = gps[:: max(1, len(gps)//300) ]
        gps_js = json.dumps(gps)
        has_gps = len(gps) > 1

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
                        cad_vals.append(p.cadence * 2)  # ppm

                if cad_vals:
                    avg_ppm = round(sum(cad_vals) / len(cad_vals))
                    progression_table[slope_key].append({
                        "ppm": avg_ppm,
                        "class": cadence_class(avg_ppm),
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
                if (
                    has_points
                    and first_dist is not None and last_dist is not None
                    and last_dist > first_dist
                    and first_time is not None and last_time is not None
                    and last_time > first_time
                ):
                    dist_m = last_dist - first_dist
                    dur_sec = last_time - first_time
                    sec_per_km = dur_sec / (dist_m / 1000.0)
                    if sec_per_km > 0:
                        pace_str = format_pace(sec_per_km)

                pace_by_segment.append(pace_str)

                if has_points:
                    dplus_by_segment.append(int(round(seg_dplus)))
                    dminus_by_segment.append(int(round(seg_dminus)))
                else:
                    dplus_by_segment.append(None)
                    dminus_by_segment.append(None)

        # --- 11) Profil altitude complet pour l'affichage (km, altitude) ---
        alt_profile = []
        for p in points:
            if p.distance is not None and p.altitude is not None:
                alt_profile.append([
                    float(p.distance) / 1000.0,
                    float(p.altitude),
                ])
        alt_profile_js = json.dumps(alt_profile)

        # --- 12) Détection des montées remarquables de la sortie ---
        MIN_VAM_START = 400.0
        MIN_POINTS = 20
        MIN_DPLUS_M = 100.0
        MAX_DMINUS_RATIO = 0.05
        MIN_DIST_M = 300.0
        MIN_PENTE_PCT = 10.0
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
        if tab not in ("overview", "segments", "climbs", "glycemia", "vam", "splits"):
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
            "tab": tab,
            "has_glucose": has_glucose,
            "glucose_zone_rows": glucose_zone_rows,
            "glucose_chart_points": glucose_chart_points,
            "glucose_zone_vs_hr_rows": glucose_zone_vs_hr_rows,
            "glucose_hr_columns": glucose_hr_columns,
            "glucose_profile_summary": glucose_profile_summary,
            "hr_zone_summary": hr_zone_summary,
            "activity_type_summary": activity_type_summary,
            "has_vam": has_vam,
            "vam_zone_rows": vam_zone_rows,
            "vam_hr_filter": vam_hr_filter,             # 🔸 on envoie l’onglet au template
            "split_km": split_km,
            "split_km_options": split_km_options,
            "splits_rows": splits_rows,

        }
    )




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

        # Supprimer d'abord les points de stream associés
        db.query(ActivityStreamPoint).filter(
            ActivityStreamPoint.activity_id == activity.id
        ).delete()

        # Puis l'activité elle-même
        db.delete(activity)
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
        db.commit()
    finally:
        db.close()

    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile#libre",
        status_code=303,
    )


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
        if user and user.cgm_source == "dexcom":
            user.cgm_source = None
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

        activities = db.query(Activity).filter(Activity.user_id == user_id).all()
        for activity in activities:
            db.delete(activity)

        db.query(StravaToken).filter(StravaToken.user_id == user_id).delete()
        db.query(LibreCredentials).filter(LibreCredentials.user_id == user_id).delete()
        db.query(DexcomToken).filter(DexcomToken.user_id == user_id).delete()
        db.query(GlucosePoint).filter(GlucosePoint.user_id == user_id).delete()
        db.query(UserSettings).filter(UserSettings.user_id == user_id).delete()
        db.query(models.UserVamPR).filter(models.UserVamPR.user_id == user_id).delete()
        db.query(ActivityVamPeak).filter(ActivityVamPeak.user_id == user_id).delete()

        db.delete(user)
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
@app.post("/api/import-activity")
async def import_activity(file: UploadFile = File(...)):
    # 1) Sauvegarder le fichier dans /tmp
    temp_dir = "/tmp"
    os.makedirs(temp_dir, exist_ok=True)
    filepath = os.path.join(temp_dir, file.filename)

    with open(filepath, "wb") as f:
        f.write(await file.read())

    # 2) Pour l'instant : on gère uniquement les fichiers .gpx
    if file.filename.lower().endswith(".gpx"):
        try:
            # user_id en dur = 1 pour commencer
            await enrich_activity_from_gpx(filepath, user_id=1)
        except ValueError as e:
            # Erreurs type "GPX sans <time>" etc.
            raise HTTPException(status_code=400, detail=str(e))
    else:
        raise HTTPException(
            status_code=400,
            detail="Format non supporté : utilise un fichier .gpx",
        )

    # 3) Réponse simple au frontend
    return {"status": "ok", "message": "Import lancé"}
