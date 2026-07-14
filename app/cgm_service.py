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
import threading
from zoneinfo import ZoneInfo

from app.database import SessionLocal
from app.models import (
    User,
    LibreCredentials,
    GlucosePoint,
    DexcomToken,
    Activity,
    CareLinkCredential,
    NightscoutCredential,
)
from app.settings import settings
from app.libre_client import (
    read_graph,
    test_libre_credentials,
    get_last_libre_status,
    is_libre_status_rate_limited,
    is_libre_status_credentials_error,
    set_libre_status_flag,
)
from app.dexcom_client import DexcomClient, has_dexcom_share_credentials
from app.providers.medtronic_carelink import (
    CareLinkNeedsReauthError,
    fetch_glucose as fetch_carelink_glucose,
)
from app.providers.nightscout import fetch_nightscout_glucose
from app.providers.registry import get_active_glucose_source

POLL_INTERVAL_SECONDS = int(os.getenv("CGM_POLL_INTERVAL_SECONDS", "420") or "420")
REALTIME_RETENTION_HOURS = int(os.getenv("CGM_REALTIME_RETENTION_HOURS", "48") or "48")

# Pour éviter d'inonder les APIs quand le nombre d'utilisateurs grossit,
# on traite désormais tous les utilisateurs par défaut à chaque cycle, tout
# en gardant deux garde-fous :
#   • par utilisateur (MIN_SECONDS_BETWEEN_POLLS_PER_USER)
#   • global entre deux appels CGM toute source confondue
MAX_USERS_PER_POLL = int(os.getenv("CGM_MAX_USERS_PER_POLL", "0") or "0")
MIN_SECONDS_BETWEEN_POLLS_PER_USER = int(os.getenv("CGM_MIN_SECONDS_PER_USER", "420") or "420")
MIN_SECONDS_BETWEEN_GLOBAL_CALLS = int(
    os.getenv("CGM_MIN_SECONDS_BETWEEN_GLOBAL_CALLS", "10") or "10"
)
MIN_SECONDS_BETWEEN_LIBRE_CALLS = int(
    os.getenv("CGM_MIN_SECONDS_BETWEEN_LIBRE_CALLS", "420") or "420"
)
LIBRE_BACKGROUND_POLL_INTERVAL_HOURS = int(
    os.getenv("LIBRE_BACKGROUND_POLL_INTERVAL_HOURS", "24") or "24"
)
LIBRE_BACKGROUND_FETCH_FOR_INACTIVE_USERS = (
    os.getenv("LIBRE_BACKGROUND_FETCH_FOR_INACTIVE_USERS", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
LIBRE_BACKGROUND_FETCH_NIGHT_ONLY = (
    os.getenv("LIBRE_BACKGROUND_FETCH_NIGHT_ONLY", "1").strip().lower()
    in {"1", "true", "yes", "on"}
)
LIBRE_BACKGROUND_NIGHT_START_HOUR = min(
    23,
    max(0, int(os.getenv("LIBRE_BACKGROUND_NIGHT_START_HOUR", "21") or "21")),
)
LIBRE_BACKGROUND_NIGHT_END_HOUR = min(
    23,
    max(0, int(os.getenv("LIBRE_BACKGROUND_NIGHT_END_HOUR", "9") or "9")),
)
LIBRE_PAGE_REFRESH_MINUTES = int(
    os.getenv("LIBRE_PAGE_REFRESH_MINUTES", "30") or "30"
)
LIBRE_RECENT_STRAVA_ACTIVITY_HOURS = int(
    os.getenv("LIBRE_RECENT_STRAVA_ACTIVITY_HOURS", "12") or "12"
)
LIBRE_BACKGROUND_FETCH_FOR_RECENT_STRAVA_ACTIVITY = (
    os.getenv("LIBRE_BACKGROUND_FETCH_FOR_RECENT_STRAVA_ACTIVITY", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)
LIBRE_PAGE_VIEW_PRIORITY_MINUTES = int(
    os.getenv("LIBRE_PAGE_VIEW_PRIORITY_MINUTES", "45") or "45"
)

# Flag global pour gérer le rate limit LibreLinkUp :
# si on reçoit un 429 / Error 1015, on n'appelle plus Libre jusqu'à LIBRE_RATE_LIMIT_UNTIL
LIBRE_RATE_LIMIT_UNTIL = None
LIBRE_RATE_LIMIT_HARD_CAP_MINUTES = 30
LIBRE_RATE_LIMIT_COOLDOWN_MINUTES = int(
    os.getenv("LIBRE_RATE_LIMIT_COOLDOWN_MINUTES", "30") or "30"
)
LIBRE_RATE_LIMIT_COOLDOWN_MINUTES = min(
    max(LIBRE_RATE_LIMIT_COOLDOWN_MINUTES, 1),
    LIBRE_RATE_LIMIT_HARD_CAP_MINUTES,
)
LIBRE_RATE_LIMIT_MAX_COOLDOWN_MINUTES = int(
    os.getenv("LIBRE_RATE_LIMIT_MAX_COOLDOWN_MINUTES", "30") or "30"
)
LIBRE_RATE_LIMIT_MAX_COOLDOWN_MINUTES = min(
    max(LIBRE_RATE_LIMIT_MAX_COOLDOWN_MINUTES, 1),
    LIBRE_RATE_LIMIT_HARD_CAP_MINUTES,
)
LIBRE_RATE_LIMIT_BACKOFF_FACTOR = max(
    1,
    int(os.getenv("LIBRE_RATE_LIMIT_BACKOFF_FACTOR", "2") or "2"),
)
LIBRE_RATE_LIMIT_STREAK = 0

# Pointeur sur le prochain utilisateur à traiter quand on limite la taille des lots
USER_POLL_CURSOR = 0

# Historique des tentatives de polling par utilisateur (en mémoire)
LAST_POLL_ATTEMPTS = {}
LAST_GLUCOSE_PAGE_VIEWS = {}

# Empêche deux appels CGM successifs à moins de MIN_SECONDS_BETWEEN_GLOBAL_CALLS
LAST_GLOBAL_CGM_CALL = None
LAST_SOURCE_CGM_CALLS: dict[str, dt.datetime] = {}
GLOBAL_CGM_CALL_LOCK = threading.Lock()


def has_carelink_credentials(user: User | None) -> bool:
    cred = getattr(user, "carelink_credentials", None)
    return bool(cred and cred.username)


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


def _source_min_interval_seconds(source_label: str) -> int:
    if source_label == "LibreLinkUp":
        return max(MIN_SECONDS_BETWEEN_LIBRE_CALLS, 0)
    return max(MIN_SECONDS_BETWEEN_GLOBAL_CALLS, 0)


def _source_throttle_allows_call(source_label: str):
    min_interval = _source_min_interval_seconds(source_label)
    if min_interval <= 0:
        return True, None

    now = dt.datetime.utcnow()
    last_call = LAST_SOURCE_CGM_CALLS.get(source_label)
    if last_call is None:
        return True, None

    delta = (now - last_call).total_seconds()
    if delta >= min_interval:
        return True, None

    remaining = int(min_interval - delta)
    return False, remaining


def _mark_source_call(source_label: str) -> None:
    LAST_SOURCE_CGM_CALLS[source_label] = dt.datetime.utcnow()


def _reserve_global_call_slot(source_label: str, user_id: int, context: str) -> None:
    """
    Réserve le prochain créneau API disponible.

    Les appels CGM sont sérialisés sous verrou pour réellement espacer les hits
    réseau, y compris si plusieurs threads/requests déclenchent des fetchs en parallèle.
    """
    waited_seconds = 0.0

    while True:
        with GLOBAL_CGM_CALL_LOCK:
            can_call_now, global_remaining = _global_throttle_allows_call()
            can_call_source, source_remaining = _source_throttle_allows_call(source_label)
            if can_call_now and can_call_source:
                _mark_global_call()
                _mark_source_call(source_label)
                if waited_seconds > 0:
                    print(
                        f"[CGM] user={user_id} -> créneau {source_label} libéré après "
                        f"{waited_seconds:.1f}s d'attente ({context})."
                    )
                return

            sleep_for = max(float(global_remaining or 0), float(source_remaining or 0), 0.0)
            if sleep_for <= 0:
                sleep_for = 0.5

        print(
            f"[CGM] user={user_id} -> attente {sleep_for:.1f}s avant appel "
            f"{source_label} ({context})."
        )
        time.sleep(sleep_for)
        waited_seconds += sleep_for


def _mark_libre_rate_limited(now_utc: dt.datetime):
    global LIBRE_RATE_LIMIT_UNTIL, LIBRE_RATE_LIMIT_STREAK
    LIBRE_RATE_LIMIT_STREAK += 1
    cooldown_minutes = LIBRE_RATE_LIMIT_COOLDOWN_MINUTES * (
        LIBRE_RATE_LIMIT_BACKOFF_FACTOR ** max(LIBRE_RATE_LIMIT_STREAK - 1, 0)
    )
    cooldown_minutes = min(cooldown_minutes, LIBRE_RATE_LIMIT_MAX_COOLDOWN_MINUTES)
    LIBRE_RATE_LIMIT_UNTIL = now_utc + dt.timedelta(minutes=cooldown_minutes)
    print(
        f"[CGM] LibreLinkUp rate-limité. "
        f"On désactive les appels Libre jusqu'à {_format_local_datetime(LIBRE_RATE_LIMIT_UNTIL)} "
        f"(cooldown={cooldown_minutes} min, streak={LIBRE_RATE_LIMIT_STREAK})."
    )


def _normalize_utc_naive(ts: dt.datetime | None) -> dt.datetime | None:
    if ts is None:
        return None
    if ts.tzinfo is None:
        return ts
    return ts.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _format_remaining_delay(seconds: int) -> str:
    if seconds <= 0:
        return "0 min"
    minutes = (seconds + 59) // 60
    if minutes < 60:
        return f"{minutes} min"
    hours = minutes // 60
    rem_minutes = minutes % 60
    if rem_minutes == 0:
        return f"{hours} h"
    return f"{hours} h {rem_minutes} min"


def _get_local_now() -> dt.datetime:
    tz_name = (settings.TZ or os.getenv("TZ") or "Europe/Paris").strip() or "Europe/Paris"
    try:
        return dt.datetime.now(ZoneInfo(tz_name))
    except Exception:
        tz = dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
        return dt.datetime.now(tz)


def _format_local_datetime(ts: dt.datetime | None) -> str:
    if ts is None:
        return "n/a"
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    local_ts = ts.astimezone(_get_local_now().tzinfo or dt.timezone.utc)
    return local_ts.strftime("%Y-%m-%d %H:%M:%S %Z")


def _is_within_night_window(now_local: dt.datetime) -> bool:
    start = LIBRE_BACKGROUND_NIGHT_START_HOUR
    end = LIBRE_BACKGROUND_NIGHT_END_HOUR
    hour = now_local.hour
    if start == end:
        return True
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


def _current_libre_cooldown_message(until_utc: dt.datetime) -> str:
    remaining_seconds = max(int((until_utc - dt.datetime.utcnow()).total_seconds()), 0)
    remaining_minutes = max(1, (remaining_seconds + 59) // 60)
    return (
        "LibreLinkUp est temporairement en cooldown global sur cette IP. "
        f"Prochaine tentative automatique dans environ {remaining_minutes} min."
    )


def _current_libre_slot_wait_message(seconds: int) -> str:
    return (
        "Un autre appel LibreLinkUp vient d'avoir lieu sur ce serveur. "
        f"Réessaie dans environ {_format_remaining_delay(seconds)}."
    )


def record_glucose_page_view(user_id: int, page_name: str) -> None:
    LAST_GLUCOSE_PAGE_VIEWS[user_id] = dt.datetime.utcnow()
    print(f"[CGM] user={user_id} -> page glucose ouverte ({page_name}), priorite haute active.")


def test_libre_credentials_guarded(
    *,
    user_id: int | None,
    email: str,
    password: str,
    region: str = "fr",
    client_version: str | None = None,
    context: str = "credentials_test",
) -> tuple[str, str]:
    now_utc = dt.datetime.utcnow()
    if LIBRE_RATE_LIMIT_UNTIL and now_utc < LIBRE_RATE_LIMIT_UNTIL:
        msg = _current_libre_cooldown_message(LIBRE_RATE_LIMIT_UNTIL)
        set_libre_status_flag(user_id, "warn", msg)
        return "warn", msg

    can_call_now, global_remaining = _global_throttle_allows_call()
    can_call_source, source_remaining = _source_throttle_allows_call("LibreLinkUp")
    if not can_call_now or not can_call_source:
        wait_seconds = max(int(global_remaining or 0), int(source_remaining or 0), 1)
        msg = _current_libre_slot_wait_message(wait_seconds)
        set_libre_status_flag(user_id, "warn", msg)
        return "warn", msg

    _reserve_global_call_slot("LibreLinkUp", user_id or 0, context)
    status, msg = test_libre_credentials(
        email=email,
        password=password,
        region=region,
        client_version=client_version,
        user_id=user_id,
    )
    if is_libre_status_rate_limited((status, msg)):
        _mark_libre_rate_limited(dt.datetime.utcnow())
    return status, msg


def _record_libre_fetch_success(user_id: int, context: str) -> None:
    global LIBRE_RATE_LIMIT_STREAK
    db = SessionLocal()
    try:
        cred = db.query(LibreCredentials).filter(LibreCredentials.user_id == user_id).first()
        if not cred:
            return
        now = dt.datetime.utcnow()
        cred.last_fetch_at = now
        cred.last_success_at = now
        cred.last_fetch_context = (context or "")[:32] or None
        LIBRE_RATE_LIMIT_STREAK = 0
        db.commit()
    except Exception as exc:
        db.rollback()
        print(f"[CGM] user={user_id} -> impossible d'enregistrer le succes Libre : {exc}")
    finally:
        db.close()


def _get_effective_last_libre_success(cred: LibreCredentials | None) -> dt.datetime | None:
    if not cred:
        return None
    last_success_at = _normalize_utc_naive(getattr(cred, "last_success_at", None))
    if last_success_at is not None:
        return last_success_at
    # Compat migration: reuse previous successful fetch timestamp if present.
    return _normalize_utc_naive(getattr(cred, "last_fetch_at", None))


def _libre_is_disabled(cred: LibreCredentials | None) -> bool:
    return bool(cred and getattr(cred, "disabled_at", None) is not None)


def _user_has_recent_page_view(user_id: int, now: dt.datetime) -> bool:
    last_view = LAST_GLUCOSE_PAGE_VIEWS.get(user_id)
    if last_view is None:
        return False
    return (now - last_view).total_seconds() <= LIBRE_PAGE_VIEW_PRIORITY_MINUTES * 60


def _user_has_recent_strava_activity(db, user_id: int, now: dt.datetime) -> bool:
    if LIBRE_RECENT_STRAVA_ACTIVITY_HOURS <= 0:
        return False

    cutoff = now - dt.timedelta(hours=LIBRE_RECENT_STRAVA_ACTIVITY_HOURS)
    recent_activity = (
        db.query(Activity.id)
        .filter(Activity.user_id == user_id, Activity.start_date.isnot(None), Activity.start_date >= cutoff)
        .order_by(Activity.start_date.desc())
        .first()
    )
    return recent_activity is not None


def should_attempt_libre_background_fetch(db, user: User) -> tuple[bool, str | None]:
    cred = user.libre_credentials
    if not cred:
        return False, "aucun compte LibreLinkUp"
    if _libre_is_disabled(cred):
        reason = cred.disabled_reason or "Libre desactive jusqu'a correction des identifiants"
        set_libre_status_flag(user.id, "error", reason)
        return False, reason

    now = dt.datetime.utcnow()
    last_success_at = _get_effective_last_libre_success(cred)

    if _user_has_recent_page_view(user.id, now):
        if last_success_at is None:
            return True, None
        age_seconds = max(int((now - last_success_at).total_seconds()), 0)
        priority_seconds = max(LIBRE_PAGE_REFRESH_MINUTES, 1) * 60
        if age_seconds >= priority_seconds:
            return True, None
        remaining = priority_seconds - age_seconds
        return False, f"page recente deja servie (reste {_format_remaining_delay(remaining)})"

    if (
        LIBRE_BACKGROUND_FETCH_FOR_RECENT_STRAVA_ACTIVITY
        and _user_has_recent_strava_activity(db, user.id, now)
    ):
        if last_success_at is None:
            return True, None
        age_seconds = max(int((now - last_success_at).total_seconds()), 0)
        priority_seconds = max(LIBRE_PAGE_REFRESH_MINUTES, 1) * 60
        if age_seconds >= priority_seconds:
            return True, None
        remaining = priority_seconds - age_seconds
        return False, f"activite Strava recente deja servie (reste {_format_remaining_delay(remaining)})"

    if not LIBRE_BACKGROUND_FETCH_FOR_INACTIVE_USERS:
        return False, "polling Libre de fond desactive pour les utilisateurs inactifs"

    if LIBRE_BACKGROUND_FETCH_NIGHT_ONLY:
        now_local = _get_local_now()
        if not _is_within_night_window(now_local):
            return False, (
                "polling Libre de fond reserve a la nuit "
                f"({LIBRE_BACKGROUND_NIGHT_START_HOUR:02d}h-{LIBRE_BACKGROUND_NIGHT_END_HOUR:02d}h)"
            )

    if LIBRE_BACKGROUND_POLL_INTERVAL_HOURS <= 0:
        return True, None

    if last_success_at is None:
        return True, None

    age_seconds = max(int((now - last_success_at).total_seconds()), 0)
    interval_seconds = LIBRE_BACKGROUND_POLL_INTERVAL_HOURS * 3600
    if age_seconds >= interval_seconds:
        return True, None

    remaining = interval_seconds - age_seconds
    return False, (
        "fetch Libre inactif deja effectue "
        f"(reste {_format_remaining_delay(remaining)})"
    )


def should_attempt_libre_page_refresh(user: User) -> tuple[bool, str | None]:
    cred = user.libre_credentials
    if not cred:
        return False, "aucun compte LibreLinkUp"
    if _libre_is_disabled(cred):
        reason = cred.disabled_reason or "Libre desactive jusqu'a correction des identifiants"
        set_libre_status_flag(user.id, "error", reason)
        return False, reason

    if get_active_glucose_source(user) not in {None, "abbott"}:
        return False, "une autre source glycémique est active"

    if LIBRE_PAGE_REFRESH_MINUTES <= 0:
        return True, None

    last_success_at = _get_effective_last_libre_success(cred)
    if last_success_at is None:
        return True, None

    now = dt.datetime.utcnow()
    age_seconds = max(int((now - last_success_at).total_seconds()), 0)
    min_interval_seconds = LIBRE_PAGE_REFRESH_MINUTES * 60
    if age_seconds >= min_interval_seconds:
        return True, None

    remaining = min_interval_seconds - age_seconds
    return False, (
        "refresh page récent "
        f"(reste {_format_remaining_delay(remaining)})"
    )


def _get_latest_realtime_glucose_ts(db, user_id: int) -> dt.datetime | None:
    latest_ts = (
        db.query(GlucosePoint.ts)
        .filter(GlucosePoint.user_id == user_id)
        .order_by(GlucosePoint.ts.desc())
        .limit(1)
        .scalar()
    )
    return _normalize_utc_naive(latest_ts)


def _should_attempt_non_libre_page_refresh(db, user_id: int) -> tuple[bool, str | None]:
    if LIBRE_PAGE_REFRESH_MINUTES <= 0:
        return True, None

    latest_ts = _get_latest_realtime_glucose_ts(db, user_id)
    if latest_ts is None:
        return True, None

    now = dt.datetime.utcnow()
    age_seconds = max(int((now - latest_ts).total_seconds()), 0)
    min_interval_seconds = LIBRE_PAGE_REFRESH_MINUTES * 60
    if age_seconds >= min_interval_seconds:
        return True, None

    remaining = min_interval_seconds - age_seconds
    return False, (
        "refresh page récent "
        f"(reste {_format_remaining_delay(remaining)})"
    )


def should_attempt_page_refresh(db, user: User) -> tuple[bool, str | None]:
    active_source = get_active_glucose_source(user)

    if active_source == "abbott":
        return should_attempt_libre_page_refresh(user)

    if active_source == "dexcom":
        if not has_dexcom_share_credentials(user.dexcom_tokens):
            return False, "aucun compte Dexcom Share"
        return _should_attempt_non_libre_page_refresh(db, user.id)

    if active_source == "medtronic_carelink":
        if not has_carelink_credentials(user):
            return False, "aucun compte CareLink"
        return _should_attempt_non_libre_page_refresh(db, user.id)

    if active_source == "nightscout":
        cred = getattr(user, "nightscout_credentials", None)
        if cred is None or not cred.base_url:
            return False, "aucun compte Nightscout"
        return _should_attempt_non_libre_page_refresh(db, user.id)

    return False, "aucune source glycémique active"


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


def fetch_realtime_points_for_user(
    db,
    user: User,
    *,
    context: str = "polling",
    allow_libre_fetch: bool = True,
):
    """
    Récupère les points temps réel pour la source glycémique active uniquement.
    Si aucune source n'est active, retourne une liste vide sans fallback implicite.
    """
    global LIBRE_RATE_LIMIT_UNTIL

    user_id = user.id
    meta = {
        "attempted_sources": [],
        "skipped_sources": [],
        "reason": None,
    }

    def try_libre():
        # Si l'utilisateur n'a pas d'identifiants Libre, on skip
        if not user.libre_credentials:
            return []

        if not allow_libre_fetch:
            meta["skipped_sources"].append("libre")
            meta["reason"] = "libre_deferred"
            return []

        now_utc = dt.datetime.utcnow()

        # Si on est encore dans la période de rate-limit, on n'appelle même pas l'API
        if LIBRE_RATE_LIMIT_UNTIL and now_utc < LIBRE_RATE_LIMIT_UNTIL:
            meta["skipped_sources"].append("libre")
            meta["reason"] = "libre_cooldown"
            set_libre_status_flag(
                user_id,
                "warn",
                _current_libre_cooldown_message(LIBRE_RATE_LIMIT_UNTIL),
            )
            print(
                f"[CGM] user={user_id} -> LibreLinkUp est en cooldown jusqu'à "
                f"{_format_local_datetime(LIBRE_RATE_LIMIT_UNTIL)}, on saute l'appel."
            )
            return []

        try:
            meta["attempted_sources"].append("libre")
            print(
                f"[CGM] user={user_id} -> tentative LibreLinkUp "
                f"(context={context}, allow_libre_fetch={allow_libre_fetch})."
            )
            _reserve_global_call_slot("LibreLinkUp", user_id, context)
            pts = read_graph(user_id=user_id) or []
            libre_status = get_last_libre_status(user_id)
            if is_libre_status_rate_limited(libre_status):
                _mark_libre_rate_limited(now_utc)
                meta["reason"] = "libre_rate_limited"
                return []
            if is_libre_status_credentials_error(libre_status):
                meta["reason"] = "libre_credentials_disabled"
                return []
            if libre_status and libre_status[0] == "ok":
                _record_libre_fetch_success(user_id, context)
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points LibreLinkUp ({context})")
            return pts
        except Exception as e:
            now_utc = dt.datetime.utcnow()
            msg = str(e)
            print(f"[CGM] user={user_id} -> erreur LibreLinkUp ({context}) : {msg}")

            # Si on détecte un rate limit (429 / Error 1015), on enclenche le cooldown global
            if "429" in msg or "Error 1015" in msg or "rate limited" in msg.lower():
                _mark_libre_rate_limited(now_utc)
                meta["reason"] = "libre_rate_limited"

            # On retourne [] pour permettre le fallback Dexcom éventuel
            return []

    def try_dexcom():
        # Si l'utilisateur n'a pas d'identifiants Dexcom Share, on skip
        if not has_dexcom_share_credentials(user.dexcom_tokens):
            return []
        try:
            meta["attempted_sources"].append("dexcom")
            _reserve_global_call_slot("Dexcom", user_id, context)
            now = dt.datetime.now(dt.timezone.utc)
            # on récupère une fenêtre raisonnable récente
            start = now - dt.timedelta(hours=REALTIME_RETENTION_HOURS)
            cli = DexcomClient(user_id=user_id, db=db)
            pts = cli.get_graph(start=start, end=now) or []
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points Dexcom ({context})")
            return pts
        except Exception as e:
            print(f"[CGM] user={user_id} -> erreur Dexcom ({context}) : {e}")
            return []

    def try_carelink():
        if not has_carelink_credentials(user):
            return []
        try:
            meta["attempted_sources"].append("medtronic_carelink")
            _reserve_global_call_slot("CareLink", user_id, context)
            now = dt.datetime.now(dt.timezone.utc)
            start = now - dt.timedelta(hours=REALTIME_RETENTION_HOURS)
            pts = []
            try:
                pts = fetch_carelink_glucose(user, start, now) or []
            except CareLinkNeedsReauthError as exc:
                if user.carelink_credentials:
                    user.carelink_credentials.status = "needs_reauth"
                    user.carelink_credentials.error_message = str(exc)
                    db.commit()
                meta["reason"] = "carelink_needs_reauth"
                print(f"[CGM] user={user_id} -> CareLink reauth requise ({context}) : {exc}")
                return []
            if user.carelink_credentials:
                user.carelink_credentials.status = "connected"
                user.carelink_credentials.error_message = None
                user.carelink_credentials.last_sync_at = dt.datetime.utcnow()
                db.commit()
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points CareLink ({context})")
            return [
                {
                    "ts": point["timestamp"],
                    "mgdl": point["glucose"],
                    "trend": point.get("trend"),
                    "source": point.get("source"),
                    "raw": point.get("raw"),
                }
                for point in pts
            ]
        except Exception as e:
            if user.carelink_credentials:
                user.carelink_credentials.status = "error"
                user.carelink_credentials.error_message = str(e)
                db.commit()
            print(f"[CGM] user={user_id} -> erreur CareLink ({context}) : {e}")
            return []

    def try_nightscout():
        cred = getattr(user, "nightscout_credentials", None)
        if cred is None or not cred.base_url:
            return []
        try:
            meta["attempted_sources"].append("nightscout")
            _reserve_global_call_slot("Nightscout", user_id, context)
            now = dt.datetime.now(dt.timezone.utc)
            start = now - dt.timedelta(hours=REALTIME_RETENTION_HOURS)
            pts = fetch_nightscout_glucose(user, start, now) or []
            if pts:
                print(f"[CGM] user={user_id} -> {len(pts)} points Nightscout ({context})")
            return [
                {
                    "ts": point["timestamp"],
                    "mgdl": point["glucose"],
                    "trend": point.get("trend"),
                    "source": point.get("source"),
                    "raw": point.get("raw"),
                }
                for point in pts
            ]
        except Exception as e:
            print(f"[CGM] user={user_id} -> erreur Nightscout ({context}) : {e}")
            return []

    points = []
    source_label = None

    preferred = get_active_glucose_source(user)

    if preferred == "abbott":
        points = try_libre()
        if points:
            source_label = "libre"
    elif preferred == "dexcom":
        points = try_dexcom()
        if points:
            source_label = "dexcom"
    elif preferred == "medtronic_carelink":
        points = try_carelink()
        if points:
            source_label = "medtronic_carelink"
    elif preferred == "nightscout":
        points = try_nightscout()
        if points:
            source_label = "nightscout"
    else:
        meta["reason"] = "no_active_source"

    return points, source_label, meta


def poll_glucose_once():
    """
    Récupère une fois les données CGM pour tous les utilisateurs qui ont
    une source CGM disponible (LibreCredentials, DexcomToken et/ou CareLinkCredential).
    Le passage est batché (MAX_USERS_PER_POLL) et saute seulement les utilisateurs
    interrogés récemment. Les appels API eux-mêmes sont cadencés par un verrou global.
    """
    now_local = _get_local_now()
    now_utc = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    print(
        f"[CGM] poll_glucose_once() appelé à {_format_local_datetime(now_local)} "
        f"(UTC {now_utc.strftime('%Y-%m-%d %H:%M:%S %Z')})"
    )

    # 1️⃣ Récupération des users concernés (au moins une source CGM)
    global USER_POLL_CURSOR

    db = SessionLocal()
    try:
        users = (
            db.query(User)
            .outerjoin(LibreCredentials, LibreCredentials.user_id == User.id)
            .outerjoin(DexcomToken, DexcomToken.user_id == User.id)
            .outerjoin(CareLinkCredential, CareLinkCredential.user_id == User.id)
            .outerjoin(NightscoutCredential, NightscoutCredential.user_id == User.id)
            .filter(
                (LibreCredentials.user_id != None)
                | (DexcomToken.user_id != None)
                | (CareLinkCredential.user_id != None)
                | (NightscoutCredential.user_id != None)
            )
            .order_by(User.id)
            .all()
        )
    finally:
        db.close()

    if not users:
        print("[CGM] Aucun utilisateur avec une source CGM en base.")
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

            allow_libre_fetch, libre_reason = should_attempt_libre_background_fetch(db, user_db)
            active_source = get_active_glucose_source(user_db)
            if (
                user_db.libre_credentials
                and not allow_libre_fetch
                and active_source == "abbott"
            ):
                print(f"[CGM] user={user_id} -> polling Libre reporté ({libre_reason}).")
                continue

            skip_poll, reason = _should_skip_user_poll(db, user_id)
            if skip_poll:
                print(f"[CGM] user={user_id} -> on saute le polling ({reason}).")
                continue

            # 2.1 Points CGM selon la source prioritaire de l'utilisateur
            points, source_label, fetch_meta = fetch_realtime_points_for_user(
                db,
                user_db,
                context="polling",
                allow_libre_fetch=allow_libre_fetch,
            )
            if fetch_meta["attempted_sources"]:
                LAST_POLL_ATTEMPTS[user_id] = dt.datetime.utcnow()

            if not points or not source_label:
                if fetch_meta["reason"] == "libre_cooldown":
                    print(
                        f"[CGM] user={user_id} -> polling reporté : LibreLinkUp en cooldown global "
                        f"et aucune autre source disponible."
                    )
                elif fetch_meta["reason"] == "libre_deferred":
                    print(
                        f"[CGM] user={user_id} -> polling Libre différé ({libre_reason or 'quota journalier'})."
                    )
                elif fetch_meta["reason"] == "libre_credentials_disabled":
                    print(
                        f"[CGM] user={user_id} -> polling Libre suspendu ({libre_reason or 'identifiants invalides'})."
                    )
                elif not fetch_meta["attempted_sources"] and not fetch_meta["skipped_sources"]:
                    print(
                        f"[CGM] user={user_id} -> aucune source CGM exploitable pour ce cycle, on passe."
                    )
                else:
                    print(
                        f"[CGM] user={user_id} -> aucun point CGM reçu après tentative "
                        f"({','.join(fetch_meta['attempted_sources']) or 'aucune'}), on passe."
                    )
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
        started_at = time.monotonic()
        try:
            poll_glucose_once()
        except Exception as e:
            print("[CGM] Erreur dans la boucle de polling :", e)
        elapsed = time.monotonic() - started_at
        sleep_for = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)
