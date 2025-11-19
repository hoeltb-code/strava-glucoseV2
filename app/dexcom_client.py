# app/dexcom_client.py
# -----------------------------------------------------------------------------
# Client Dexcom pour l'application "Strava x Glucose".
#
# Rôles :
#   🔹 Gérer le flux OAuth Dexcom (URL d'auth, échange code → tokens, refresh).
#   🔹 Stocker les tokens en base dans la table DexcomToken (par user_id).
#   🔹 Fournir une méthode get_graph(start, end) qui renvoie les données CGM
#      au même format que LibreLinkUp :
#        [
#          {"ts": datetime(UTC), "mgdl": 123, "trend": "Flat" | None},
#          ...
#        ]
#
# Remarques importantes :
#   - Les URLs précises (AUTH, TOKEN, EGV) et le format exact des champs Dexcom
#     peuvent varier selon l'API utilisée (Production / Sandbox / US / EU).
#   - Ce fichier fournit une implémentation "squelette" prête à être ajustée
#     avec la vraie doc Dexcom (endpoints, scopes, noms de champs).
#   - En cas d'erreur réseau ou API, get_graph renvoie simplement [] pour ne pas
#     casser le reste de l'appli.
# -----------------------------------------------------------------------------

import datetime as dt
from typing import List, Dict, Any, Optional

import requests
from sqlalchemy.orm import Session

from .settings import settings
from app.models import DexcomToken


class DexcomClientError(RuntimeError):
    pass


class DexcomClient:
    """
    Client Dexcom associé à un user_id.

    - Utilisation côté OAuth (FastAPI routes) :
        cli = DexcomClient(user_id, db)
        url = cli.get_authorization_url()
        ...
        await cli.exchange_code_for_tokens(code)

    - Utilisation côté data :
        cli = DexcomClient(user_id, db)
        points = cli.get_graph(start, end)
    """

    def __init__(self, user_id: int, db: Session):
        self.user_id = user_id
        self.db = db

        # ⚙️ Paramètres Dexcom issus de settings / .env
        self.client_id = settings.DEXCOM_CLIENT_ID
        self.client_secret = settings.DEXCOM_CLIENT_SECRET
        self.redirect_uri = settings.DEXCOM_REDIRECT_URI

        # Endpoints de production (US ou EU) fournis par .env / settings
        self.auth_url = settings.DEXCOM_AUTH_URL
        self.token_url = settings.DEXCOM_TOKEN_URL
        self.egv_url = settings.DEXCOM_EGV_URL

        # Scopes par défaut (peut être surchargé dans .env si besoin)
        self.scope = getattr(settings, "DEXCOM_SCOPE", "offline_access")

    # ----------------------------------------------------------------------
    # OAuth : URL d'autorisation
    # ----------------------------------------------------------------------
    def get_authorization_url(self) -> str:
        """
        Construit l'URL Dexcom pour démarrer le flux OAuth.
        Le `state` est fixé au user_id pour faire le mapping au callback.
        """
        import urllib.parse as up

        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": self.scope,
            "state": str(self.user_id),
        }
        url = f"{self.auth_url}?{up.urlencode(params)}"
        print(f"[Dexcom] Authorization URL générée pour user_id={self.user_id} : {url}")
        return url

    # ----------------------------------------------------------------------
    # OAuth : échange code -> tokens
    # ----------------------------------------------------------------------
    async def exchange_code_for_tokens(self, code: str) -> None:
        """
        Échange le code d'autorisation Dexcom contre un access_token + refresh_token,
        puis les enregistre en base dans DexcomToken.
        """
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }

        try:
            resp = requests.post(self.token_url, data=data, timeout=20)
        except Exception as e:
            raise DexcomClientError(f"Erreur réseau lors de l'échange code→token Dexcom : {e}")

        if resp.status_code != 200:
            raise DexcomClientError(
                f"Echec échange code→token Dexcom (status={resp.status_code}) : {resp.text}"
            )

        payload = resp.json()

        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token")
        expires_in = payload.get("expires_in")  # en secondes

        if not access_token or not refresh_token:
            raise DexcomClientError("Réponse Dexcom invalide : tokens manquants.")

        now = dt.datetime.now(dt.timezone.utc)
        if isinstance(expires_in, (int, float)):
            expires_at = now + dt.timedelta(seconds=int(expires_in))
        else:
            # fallback : 8h
            expires_at = now + dt.timedelta(hours=8)

        # Conversion en epoch (BIGINT en base)
        expires_at_epoch = int(expires_at.timestamp())

        # Upsert dans DexcomToken
        token = (
            self.db.query(DexcomToken)
            .filter(DexcomToken.user_id == self.user_id)
            .one_or_none()
        )

        if token is None:
            token = DexcomToken(
                user_id=self.user_id,
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=expires_at_epoch,
            )
            self.db.add(token)
        else:
            token.access_token = access_token
            token.refresh_token = refresh_token
            token.expires_at = expires_at_epoch

        self.db.commit()
        self.db.refresh(token)
        print(f"[Dexcom] Tokens enregistrés / mis à jour pour user_id={self.user_id}")

    # ----------------------------------------------------------------------
    # Gestion des tokens (lecture + refresh)
    # ----------------------------------------------------------------------
    def _get_token_record(self) -> Optional[DexcomToken]:
        return (
            self.db.query(DexcomToken)
            .filter(DexcomToken.user_id == self.user_id)
            .one_or_none()
        )

    def _ensure_access_token(self) -> Optional[str]:
        """
        Retourne un access_token valide.
        - Si pas de token en base → None
        - Si expiré → tentative de refresh ; si OK, retourne le nouveau.
        """
        token = self._get_token_record()
        if not token:
            print(f"[Dexcom] Aucun token pour user_id={self.user_id}")
            return None

        now_epoch = int(dt.datetime.now(dt.timezone.utc).timestamp())
        if token.expires_at and token.expires_at > now_epoch - 60:
            # encore valide (avec une petite marge)
            return token.access_token

        # Token expiré → refresh
        if not token.refresh_token:
            print(f"[Dexcom] Pas de refresh_token pour user_id={self.user_id}")
            return None

        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": token.refresh_token,
        }

        try:
            resp = requests.post(self.token_url, data=data, timeout=20)
        except Exception as e:
            print(f"[Dexcom] Erreur réseau lors du refresh token : {e}")
            return None

        if resp.status_code != 200:
            print(f"[Dexcom] Echec refresh token (status={resp.status_code}) : {resp.text}")
            return None

        payload = resp.json()
        access_token = payload.get("access_token")
        refresh_token = payload.get("refresh_token") or token.refresh_token
        expires_in = payload.get("expires_in", 28800)  # fallback 8h

        if not access_token:
            print("[Dexcom] Réponse refresh invalide : access_token manquant.")
            return None

        now = dt.datetime.now(dt.timezone.utc)
        expires_at = now + dt.timedelta(seconds=int(expires_in))
        token.access_token = access_token
        token.refresh_token = refresh_token
        token.expires_at = int(expires_at.timestamp())

        self.db.commit()
        self.db.refresh(token)

        print(f"[Dexcom] Token refresh OK pour user_id={self.user_id}")
        return token.access_token

    # ----------------------------------------------------------------------
    # Lecture des données CGM
    # ----------------------------------------------------------------------
    def get_graph(
        self,
        start: dt.datetime,
        end: dt.datetime,
    ) -> List[Dict[str, Any]]:
        """
        Récupère les données de glycémie Dexcom entre `start` et `end`
        et les renvoie au format interne :

            [
              {"ts": datetime(UTC), "mgdl": int, "trend": str|None},
              ...
            ]

        En cas d'erreur (token manquant, API down, etc.), retourne [].
        """
        access_token = self._ensure_access_token()
        if not access_token:
            print(f"[Dexcom] Aucun access_token valide pour user_id={self.user_id}")
            return []

        # Normalisation en UTC
        if start.tzinfo is None:
            start = start.replace(tzinfo=dt.timezone.utc)
        else:
            start = start.astimezone(dt.timezone.utc)

        if end.tzinfo is None:
            end = end.replace(tzinfo=dt.timezone.utc)
        else:
            end = end.astimezone(dt.timezone.utc)

        # Dexcom veut un format du type YYYY-MM-DDThh:mm:ss (sans Z)
        def _fmt(d: dt.datetime) -> str:
            return d.strftime("%Y-%m-%dT%H:%M:%S")

        params = {
            "startDate": _fmt(start),
            "endDate": _fmt(end),
        }

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        try:
            resp = requests.get(self.egv_url, headers=headers, params=params, timeout=20)
        except Exception as e:
            print(f"[Dexcom] Erreur réseau lors de l'appel EGV : {e}")
            return []

        if resp.status_code != 200:
            print(f"[Dexcom] Echec appel EGV (status={resp.status_code}) : {resp.text}")
            return []

        data = resp.json()

        egvs = data.get("egvs") or data.get("values") or []
        out: List[Dict[str, Any]] = []

        for it in egvs:
            try:
                ts_raw = (
                    it.get("systemTime")
                    or it.get("displayTime")
                    or it.get("timestamp")
                )
                if not ts_raw:
                    continue

                ts = (
                    dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                    .astimezone(dt.timezone.utc)
                )

                mgdl_raw = it.get("value") or it.get("glucoseValue")
                if mgdl_raw is None:
                    continue
                mgdl = int(mgdl_raw)

                trend = it.get("trend") or it.get("trendDescription")

                out.append(
                    {
                        "ts": ts,
                        "mgdl": mgdl,
                        "trend": trend,
                    }
                )
            except Exception as e:
                print(f"[Dexcom] Point EGV ignoré ({it}) : {e}")
                continue

        out.sort(key=lambda x: x["ts"])
        return out
