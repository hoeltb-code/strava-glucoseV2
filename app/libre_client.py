# app/libre_client.py
# -----------------------------------------------------------------------------
# Ce module gère la connexion à LibreLinkUp et la récupération des données
# de glycémie pour un utilisateur donné.
#
# Il sert d’interface entre l’application principale (Python) et le helper Node.js
# (`libre_node/reader.mjs`), qui interroge directement l’API LibreLinkUp.
#
# Fonctionnement :
#
# 🔹 Récupération des identifiants :
#    - Cherche les identifiants LibreLinkUp en base (`LibreCredentials`) selon le user_id.
#    - Si aucun identifiant n’est trouvé, utilise les variables globales du fichier `.env`.
#
# 🔹 Exécution du helper Node :
#    - Lance le script Node.js `reader.mjs` en sous-processus.
#    - Passe les identifiants via les variables d’environnement.
#    - Récupère la sortie JSON contenant les points de glycémie.
#
# 🔹 Traitement et validation des données :
#    - Parse le JSON, convertit les timestamps ISO → datetime UTC.
#    - Filtre et trie les points valides (ts, mgdl, trend).
#    - En cas d’erreur (API, JSON, Node manquant, etc.), renvoie simplement une liste vide.
#
# 🔹 Fonction principale :
#    - `read_graph(user_id)` : retourne une liste de points de glycémie formatés :
#        [ { "ts": datetime(UTC), "mgdl": int, "trend": str|None }, ... ]
#
# En résumé : `libre_client.py` permet de récupérer les courbes de glycémie
# depuis LibreLinkUp pour les synchroniser avec les activités Strava.
# -----------------------------------------------------------------------------
import os
import json
import subprocess
import pathlib
import datetime as dt
from typing import List, Dict, Any, Optional

from .settings import settings
from app.database import SessionLocal
from app.models import LibreCredentials


class LibreError(RuntimeError):
    """Ancien type d'erreur LibreLinkUp (conservé si besoin ailleurs)."""
    pass


def _get_user_libre_credentials(user_id: int) -> Optional[LibreCredentials]:
    """
    Récupère les identifiants LibreLinkUp pour un utilisateur donné.
    Retourne un objet LibreCredentials ou None si rien n'est configuré.
    """
    db = SessionLocal()
    try:
        cred = db.query(LibreCredentials).filter(LibreCredentials.user_id == user_id).first()
        return cred
    finally:
        db.close()


def read_graph(user_id: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Appelle le helper Node et retourne une liste de points :
    [
      { "ts": datetime(UTC), "mgdl": int, "trend": str|None },
      ...
    ]

    - Si user_id est fourni :
        -> on cherche en base les identifiants LibreLinkUp pour cet utilisateur.
        -> si rien en base, on retombe sur les variables globales (settings.LIBRE_*).
    - Si user_id est None :
        -> comportement historique : on utilise settings.LIBRE_EMAIL / LIBRE_PASSWORD / LIBRE_REGION.

    ⚠️ IMPORTANT :
    - En cas d'erreur (Node manquant, 403/430 LibreLinkUp, JSON invalide, etc.),
      cette fonction NE LÈVE PLUS d'exception.
    - Elle renvoie simplement [] pour ne pas casser le webhook Strava.
    """

    # 0️⃣ Choix des identifiants à utiliser
    email = ""
    password = ""
    region = "fr"
    client_version = "4.16.0"

    if user_id is not None:
        cred = _get_user_libre_credentials(user_id)
        if cred:
            # Pour l'instant, password_encrypted contient le mot de passe en clair.
            email = cred.email
            password = cred.password_encrypted
            region = cred.region or "fr"
            client_version = cred.client_version or "4.16.0"
        else:
            print(f"⚠️ Aucun LibreCredentials trouvé pour user_id={user_id}, on utilise les valeurs globales.")
            email = settings.LIBRE_EMAIL or ""
            password = settings.LIBRE_PASSWORD or ""
            region = settings.LIBRE_REGION or "fr"
            client_version = os.getenv("LIBRE_CLIENT_VERSION", "4.16.0")
    else:
        # Comportement historique : variables globales .env
        email = settings.LIBRE_EMAIL or ""
        password = settings.LIBRE_PASSWORD or ""
        region = settings.LIBRE_REGION or "fr"
        client_version = os.getenv("LIBRE_CLIENT_VERSION", "4.16.0")

    # 1️⃣ Localisation du script Node
    project_root = pathlib.Path(__file__).resolve().parents[1]
    script = project_root / "libre_node" / "reader.mjs"
    if not script.exists():
        print(f"⚠️ Helper Node manquant : {script} (aucune donnée LibreLinkUp).")
        return []

    # 2️⃣ Préparation de l'environnement pour le process Node
    env = os.environ.copy()
    env.update({
        "LIBRE_EMAIL": email,
        "LIBRE_PASSWORD": password,
        "LIBRE_REGION": region,
        "LIBRE_CLIENT_VERSION": client_version,
    })

    # 3️⃣ Appel du script Node
    try:
        proc = subprocess.run(
            ["node", str(script)],
            capture_output=True,
            text=True,
            env=env,
            timeout=25,
        )
    except FileNotFoundError:
        print("⚠️ Node.js introuvable. Installe Node (ex: brew install node).")
        return []
    except Exception as e:
        print(f"⚠️ Erreur inattendue lors de l'exécution du helper Node : {e}")
        return []

    # 4️⃣ Gestion des erreurs du helper Node (ex: 403/430 LibreLinkUp)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        print(
            "⚠️ Helper Node a échoué (LibreLinkUp). Erreur ignorée pour ne pas casser le webhook.\n"
            f"Code retour : {proc.returncode}\n"
            f"Message : {err}"
        )
        return []

    stdout = proc.stdout.strip()
    if not stdout:
        print("⚠️ Helper Node a renvoyé une sortie vide (aucune donnée LibreLinkUp).")
        return []

    # 5️⃣ Parsing JSON
    try:
        arr = json.loads(stdout)
    except Exception as e:
        print(
            "⚠️ JSON invalide renvoyé par le helper LibreLinkUp (erreur ignorée) :",
            e,
            "Début de la sortie :",
            stdout[:200],
        )
        return []

    # 6️⃣ Conversion en liste de points Python
    out: List[Dict[str, Any]] = []
    for it in arr:
        try:
            ts = it.get("ts")
            if not ts:
                continue

            # Conversion ISO → datetime UTC
            dt_utc = (
                dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
                .astimezone(dt.timezone.utc)
            )

            mgdl_raw = it.get("mgdl", 0)
            mgdl = int(mgdl_raw)

            out.append({
                "ts": dt_utc,
                "mgdl": mgdl,
                "trend": it.get("trend"),
            })
        except Exception as e:
            # Point individuel ignoré, on log et on continue
            print(f"⚠️ Point LibreLinkUp ignoré ({it}) : {e}")
            continue

    # 7️⃣ Tri défensif par timestamp
    out.sort(key=lambda x: x["ts"])
    return out
