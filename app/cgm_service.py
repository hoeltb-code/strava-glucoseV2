# app/cgm_service.py
# -----------------------------------------------------------------------------
# Ce module gère la collecte automatique des données de glycémie (CGM) depuis
# LibreLinkUp et Dexcom, afin de disposer d’un historique local, plus précis
# et résilient que les seules données archivées par les plateformes.
#
# Il exécute un *polling* périodique (toutes les 420 secondes par défaut) pour
# chaque utilisateur disposant d’une source CGM (Libre ou Dexcom) :
#
# 🔹 Pour chaque utilisateur :
#     1. Sélection de la source CGM prioritaire selon `user.cgm_source` :
#        - 'libre'  → LibreLinkUp en priorité, fallback Dexcom.
#        - 'dexcom' → Dexcom en priorité, fallback LibreLinkUp.
#        - None     → Dexcom > LibreLinkUp si disponibles.
#     2. Appel de la source choisie pour lire les dernières valeurs de glycémie.
#     3. Normalisation des timestamps (UTC sans fuseau horaire).
#     4. Sélection du point le plus récent et enregistrement dans la table
#        `glucose_points`, s’il n’existe pas déjà, avec source="realtime".
#     5. Suppression automatique des données "realtime" plus anciennes que
#        `REALTIME_RETENTION_HOURS`.
#
# 🔹 Le polling s’exécute dans un thread dédié au démarrage de l’application
#    (voir `main.py`, événement `startup`).
# -----------------------------------------------------------------------------

import os
import time
import datetime as dt

from app.database import SessionLocal
from app.models import User, LibreCredentials, GlucosePoint, DexcomToken
from app.libre_client import read_graph
from app.dexcom_client import DexcomClient, has_dexcom_share_credentials

POLL_INTERVAL_SECONDS = int(os.getenv("CGM_POLL_INTERVAL_SECONDS", "420") or "420")
REALTIME_RETENTION_HOURS = int(os.getenv("CGM_REALTIME_RETENTION_HOURS", "48") or "48")

# Pour éviter d'inonder les APIs quand le nombre d'utilisateurs grossit,
# on traite les utilisateurs par très petits groupes (1 par défaut) et on
# impose deux types de délais :
#   • par utilisateur (MIN_SECONDS_BETWEEN_POLLS_PER_USER)
#   • global entre deux appels CGM toute source confondue
MAX_USERS_PER_POLL = int(os.getenv("CGM_MAX_USERS_PER_POLL", "1") or "1")
MIN_SECONDS_BETWEEN_POLLS_PER_USER = int(os.getenv("CGM_MIN_SECONDS_PER_USER", "420") or "420")
MIN_SECONDS_BETWEEN_GLOBAL_CALLS = int(
    os.getenv("CGM_MIN_SECONDS_BETWEEN_GLOBAL_CALLS", "420") or "420"
)

# Flag global pour gérer le rate limit LibreLinkUp :
# si on reçoit un 429 / Error 1015, on n'appelle plus Libre jusqu'à LIBRE_RATE_LIMIT_UNTIL
LIBRE_RATE_LIMIT_UNTIL = None
LIBRE_RATE_LIMIT_COOLDOWN_MINUTES = 60  # durée du "ban" après un 429

# Pointeur sur le prochain utilisateur à traiter quand on limite la taille des lots
USER_POLL_CURSOR = 0

# Historique des tentatives de polling par utilisateur (en mémoire)
LAST_POLL_ATTEMPTS = {}

# Empêche deux appels CGM successifs à moins de MIN_SECONDS_BETWEEN_GLOBAL_CALLS
LAST_GLOBAL_CGM_CALL = None


def _global_throttle_allows_call():
    """Retourne (True, None) si on peut interroger une CGM tout de suite."""
    if MIN_SECONDS_BETWEEN_GLOBAL_CALLS <= 0:
        return True, None

    now = dt.datetime.utcnow()
    if LAST_GLOBAL_CGM_CALL is None:
        return True, None

    delta = (now - LAST_GLOBAL_CGM_CALL).total_seconds()
    if delta >= MIN_SECONDS_BETWEEN_GLOBAL_CALLS:
        return True, None

    remaining = int(MIN_SECONDS_BETWEEN_GLOBAL_CALLS - delta)
    return False, remaining


def _mark_global_call():
    global LAST_GLOBAL_CGM_CALL
    LAST_GLOBAL_CGM_CALL = dt.datetime.utcnow()


def _should_skip_user_poll(db, user_id: int):
    """Retourne (True, raison) si l'utilisateur a été interrogé trop récemment."""
    if MIN_SECONDS_BETWEEN_POLLS_PER_USER <= 0:
        return False, None

    now = dt.datetime.utcnow()

    last_attempt = LAST_POLL_ATTEMPTS.get(user_id)
    if last_attempt:
        since_last_attempt = (now - last_attempt).total_seconds()
        if since_last_attempt < MIN_SECONDS_BETWEEN_POLLS_PER_USER:
            remaining = int(MIN_SECONDS_BETWEEN_POLLS_PER_USER - since_last_attempt)
            return True, f"tentative API il y a {int(since_last_attempt)}s (reste {remaining}s)"

    latest_ts = (
        db.query(GlucosePoint.ts)
        .filter(
            GlucosePoint.user_id == user_id,
            GlucosePoint.source == "realtime",
        )
        .order_by(GlucosePoint.ts.desc())
        .limit(1)
        .scalar()
    )

    if latest_ts:
        if latest_ts.tzinfo is not None:
            latest_naive = latest_ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
        else:
            latest_naive = latest_ts

        age_seconds = (now - latest_naive).total_seconds()
        if age_seconds < MIN_SECONDS_BETWEEN_POLLS_PER_USER:
            remaining = int(MIN_SECONDS_BETWEEN_POLLS_PER_USER - age_seconds)
            return True, f"dernières données en base âgée de {int(age_seconds)}s (reste {remaining}s)"

    return False, None


def _get_realtime_points_for_user(db, user: User):
    """
    Récupère une liste de points CGM pour un user donné en respectant sa
    préférence de source (user.cgm_source) et les disponibilités réelles :
      - 'libre'  => LibreLinkUp d'abord, puis Dexcom en fallback
      - 'dexcom' => Dexcom d'abord, puis LibreLinkUp en fallback
      - None     => Dexcom puis LibreLinkUp

    Retourne (points, source_label) où :
      - points : liste de dicts {"ts": datetime, "mgdl": int, "trend": str|None}
      - source_label : "libre", "dexcom" ou None si aucune source dispo
    """
    global LIBRE_RATE_LIMIT_UNTIL

    user_id = user.id

    def try_libre():
        global LIBRE_RATE_LIMIT_UNTIL

        # Si l'utilisateur n'a pas d'identifiants Libre, on skip
        if not user.libre_credentials:
            return []

        now_utc = dt.datetime.utcnow()

        # Si on est encore dans la période de rate-limit, on n'appelle même pas l'API
        if LIBRE_RATE_LIMIT_UNTIL and now_utc < LIBRE_RATE_LIMIT_UNTIL:
            print(
                f"[CGM] user={user_id} -> LibreLinkUp est en cooldown jusqu'à "
                f"{LIBRE_RATE_LIMIT_UNTIL}, on saute l'appel."
            )
            return []

        try:
            pts = read_graph(user_id=user_id) or []
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points LibreLinkUp (polling)")
            return pts
        except Exception as e:
            msg = str(e)
            print(f"[CGM] user={user_id} -> erreur LibreLinkUp (polling) : {msg}")

            # Si on détecte un rate limit (429 / Error 1015), on enclenche le cooldown global
            if "429" in msg or "Error 1015" in msg or "rate limited" in msg.lower():
                LIBRE_RATE_LIMIT_UNTIL = now_utc + dt.timedelta(
                    minutes=LIBRE_RATE_LIMIT_COOLDOWN_MINUTES
                )
                print(
                    f"[CGM] LibreLinkUp rate-limité (erreur 429/1015). "
                    f"On désactive les appels Libre jusqu'à {LIBRE_RATE_LIMIT_UNTIL}."
                )

            # On retourne [] pour permettre le fallback Dexcom éventuel
            return []

    def try_dexcom():
        # Si l'utilisateur n'a pas d'identifiants Dexcom Share, on skip
        if not has_dexcom_share_credentials(user.dexcom_tokens):
            return []
        try:
            now = dt.datetime.now(dt.timezone.utc)
            # on récupère une fenêtre raisonnable récente
            start = now - dt.timedelta(hours=REALTIME_RETENTION_HOURS)
            cli = DexcomClient(user_id=user_id, db=db)
            pts = cli.get_graph(start=start, end=now) or []
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points Dexcom (polling)")
            return pts
        except Exception as e:
            print(f"[CGM] user={user_id} -> erreur Dexcom (polling) : {e}")
            return []

    points = []
    source_label = None

    if user.cgm_source == "libre":
        points = try_libre()
        if points:
            source_label = "libre"
        else:
            points = try_dexcom()
            if points:
                source_label = "dexcom"
    elif user.cgm_source == "dexcom":
        points = try_dexcom()
        if points:
            source_label = "dexcom"
        else:
            points = try_libre()
            if points:
                source_label = "libre"
    else:
        # Auto : on privilégie Dexcom si dispo, sinon Libre
        points = try_dexcom()
        if points:
            source_label = "dexcom"
        else:
            points = try_libre()
            if points:
                source_label = "libre"

    return points, source_label


def poll_glucose_once():
    """
    Récupère une fois les données CGM pour tous les utilisateurs qui ont
    une source CGM disponible (LibreCredentials et/ou DexcomToken).
    Le passage est batché (MAX_USERS_PER_POLL) et saute un utilisateur qui a
    déjà été interrogé récemment afin d'éviter un ban côté API.
    """
    now = dt.datetime.utcnow().isoformat()
    print(f"[CGM] poll_glucose_once() appelé à {now}")

    # 1️⃣ Récupération des users concernés (au moins une source CGM)
    global USER_POLL_CURSOR

    db = SessionLocal()
    try:
        users = (
            db.query(User)
            .outerjoin(LibreCredentials, LibreCredentials.user_id == User.id)
            .outerjoin(DexcomToken, DexcomToken.user_id == User.id)
            .filter(
                (LibreCredentials.user_id != None) | (DexcomToken.user_id != None)
            )
            .order_by(User.id)
            .all()
        )
    finally:
        db.close()

    if not users:
        print("[CGM] Aucun utilisateur avec une source CGM (Libre/Dexcom) en base.")
        return

    total_users = len(users)
    batch_limit = MAX_USERS_PER_POLL if MAX_USERS_PER_POLL > 0 else total_users

    if total_users > batch_limit:
        start_idx = USER_POLL_CURSOR % total_users
        users_to_process = []
        idx = start_idx
        while len(users_to_process) < batch_limit:
            users_to_process.append(users[idx])
            idx = (idx + 1) % total_users
        USER_POLL_CURSOR = idx
        print(
            f"[CGM] {total_users} utilisateurs CGM détectés, traitement par lots de "
            f"{batch_limit} par cycle (offset={start_idx}→{idx})."
        )
    else:
        users_to_process = users
        USER_POLL_CURSOR = 0

    # 2️⃣ Boucle sur chaque utilisateur
    for user in users_to_process:
        user_id = user.id
        db = SessionLocal()
        try:
            # recharger l'user dans cette session
            user_db = db.query(User).get(user_id)
            if not user_db:
                continue

            skip_poll, reason = _should_skip_user_poll(db, user_id)
            if skip_poll:
                print(f"[CGM] user={user_id} -> on saute le polling ({reason}).")
                continue

            can_call_now, remaining = _global_throttle_allows_call()
            if not can_call_now:
                print(
                    f"[CGM] user={user_id} -> on saute le polling (quota global, +{remaining}s)."
                )
                continue

            # 2.1 Points CGM selon la source prioritaire de l'utilisateur
            LAST_POLL_ATTEMPTS[user_id] = dt.datetime.utcnow()
            _mark_global_call()
            points, source_label = _get_realtime_points_for_user(db, user_db)

            if not points or not source_label:
                print(f"[CGM] user={user_id} -> aucun point CGM (Libre/Dexcom) reçu, on passe.")
                continue

            print(f"[CGM] user={user_id} -> {len(points)} points reçus (source={source_label})")

            # 2.2 Normalisation : tout en datetime naïf UTC
            normalized_points = []
            for p in points:
                ts = p["ts"]
                if ts.tzinfo is not None:
                    ts_utc_naive = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
                else:
                    ts_utc_naive = ts  # on considère que c'est déjà de l'UTC naïf

                normalized_points.append(
                    {
                        "ts": ts_utc_naive,
                        "mgdl": p["mgdl"],
                        "trend": p.get("trend"),
                    }
                )

            # 2.3 Tri des points par timestamp (croissant)
            normalized_points.sort(key=lambda p: p["ts"])

            # 2.4 Insertion de TOUS les nouveaux points
            new_count = 0
            for p in normalized_points:
                ts = p["ts"]
                mgdl = p["mgdl"]
                trend = p.get("trend")

                # Vérifier si ce timestamp existe déjà pour cet utilisateur
                existing = (
                    db.query(GlucosePoint)
                    .filter(GlucosePoint.user_id == user_id, GlucosePoint.ts == ts)
                    .one_or_none()
                )
                if existing:
                    continue

                gp = GlucosePoint(
                    user_id=user_id,
                    ts=ts,          # ts naïf UTC
                    mgdl=mgdl,
                    trend=trend,
                    # On garde "realtime" pour que la purge continue à fonctionner
                    # quelle que soit la source CGM.
                    source="realtime",
                )
                db.add(gp)
                new_count += 1

            if new_count:
                print(
                    f"[CGM] user={user_id} -> {new_count} nouveaux points realtime "
                    f"insérés (source={source_label})."
                )
            else:
                print(
                    f"[CGM] user={user_id} -> aucun nouveau point CGM à insérer "
                    f"(tout déjà en base)."
                )

            # 2.5 Rétention : suppression des points "realtime" de plus de REALTIME_RETENTION_HOURS
            cutoff = dt.datetime.utcnow() - dt.timedelta(hours=REALTIME_RETENTION_HOURS)
            deleted_count = (
                db.query(GlucosePoint)
                .filter(
                    GlucosePoint.user_id == user_id,
                    GlucosePoint.source == "realtime",
                    GlucosePoint.ts < cutoff,
                )
                .delete()
            )

            if deleted_count:
                print(
                    f"[CGM] user={user_id} -> {deleted_count} anciens points realtime supprimés "
                    f"(> {REALTIME_RETENTION_HOURS}h)."
                )

            db.commit()

        except Exception as e:
            # Si un truc foire après l'ouverture de la session
            print(f"[CGM] Erreur inattendue pour user={user_id} : {e}")
            db.rollback()
        finally:
            db.close()


def run_polling_loop():
    """
    Boucle infinie qui tourne dans un thread séparé.
    Simple, bloquante, mais suffisante pour un usage local/dev.
    """
    print(f"[CGM] Démarrage du polling glycémie (toutes les {POLL_INTERVAL_SECONDS} secondes)...")
    while True:
        try:
            poll_glucose_once()
        except Exception as e:
            print("[CGM] Erreur dans la boucle de polling :", e)
        time.sleep(POLL_INTERVAL_SECONDS)
