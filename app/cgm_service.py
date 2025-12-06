# app/cgm_service.py
# -----------------------------------------------------------------------------
# Ce module gère la collecte automatique des données de glycémie (CGM) depuis
# LibreLinkUp et Dexcom, afin de disposer d’un historique local, plus précis
# et résilient que les seules données archivées par les plateformes.
#
# Il exécute un *polling* périodique (toutes les 180 secondes par défaut) pour
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

import time
import datetime as dt

from app.database import SessionLocal
from app.models import User, LibreCredentials, GlucosePoint, DexcomToken
from app.libre_client import read_graph
from app.dexcom_client import DexcomClient

POLL_INTERVAL_SECONDS = 180          # intervalle de polling
REALTIME_RETENTION_HOURS = 48       # on garde 48h de données


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
    user_id = user.id

    def try_libre():
        # Si l'utilisateur n'a pas d'identifiants Libre, on skip
        if not user.libre_credentials:
            return []
        try:
            pts = read_graph(user_id=user_id) or []
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points LibreLinkUp (polling)")
            return pts
        except Exception as e:
            print(f"[CGM] user={user_id} -> erreur LibreLinkUp (polling) : {e}")
            return []

    def try_dexcom():
        # Si l'utilisateur n'a pas de tokens Dexcom, on skip
        if not user.dexcom_tokens:
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
    """
    now = dt.datetime.utcnow().isoformat()
    print(f"[CGM] poll_glucose_once() appelé à {now}")

    # 1️⃣ Récupération des users concernés (au moins une source CGM)
    db = SessionLocal()
    try:
        users = (
            db.query(User)
            .outerjoin(LibreCredentials, LibreCredentials.user_id == User.id)
            .outerjoin(DexcomToken, DexcomToken.user_id == User.id)
            .filter(
                (LibreCredentials.user_id != None) | (DexcomToken.user_id != None)
            )
            .all()
        )
    finally:
        db.close()

    if not users:
        print("[CGM] Aucun utilisateur avec une source CGM (Libre/Dexcom) en base.")
        return

    # 2️⃣ Boucle sur chaque utilisateur
    for user in users:
        user_id = user.id
        db = SessionLocal()
        try:
            # recharger l'user dans cette session
            user_db = db.query(User).get(user_id)
            if not user_db:
                continue

            # 2.1 Points CGM selon la source prioritaire de l'utilisateur
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
