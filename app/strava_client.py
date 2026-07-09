# app/strava_client.py
import os
import time
from typing import Optional, Dict, Any, List
import httpx

from app.database import SessionLocal
from app.models import StravaToken

# -----------------------------
# 🌐 Configuration Strava
# -----------------------------
STRAVA_CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
STRAVA_CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
STRAVA_REDIRECT_URI = os.getenv("STRAVA_REDIRECT_URI")

STRAVA_BASE_URL = "https://www.strava.com/api/v3"
STRAVA_OAUTH_TOKEN_URL = "https://www.strava.com/oauth/token"


class StravaClient:
    """
    Client Strava connecté à un utilisateur (multi-utilisateur via BDD).
    Pour l’instant, on suppose user_id=1 (on généralisera ensuite).
    """

    def __init__(self, user_id: int = 1):
        if not STRAVA_CLIENT_ID or not STRAVA_CLIENT_SECRET:
            raise RuntimeError("STRAVA_CLIENT_ID ou STRAVA_CLIENT_SECRET manquants dans l'environnement.")
        self.user_id = user_id

    # ======================================================
    # 🔑 Gestion des tokens via la base de données
    # ======================================================

    def _get_token_record(self, db) -> Optional[StravaToken]:
        return (
            db.query(StravaToken)
            .filter(StravaToken.user_id == self.user_id)
            .order_by(StravaToken.id.desc())
            .first()
        )

    def _save_tokens(
        self,
        db,
        access_token: str,
        refresh_token: str,
        expires_at: int,
        athlete_id: Optional[int] = None,
    ) -> StravaToken:
        token = self._get_token_record(db)
        if token:
            token.access_token = access_token
            token.refresh_token = refresh_token
            token.expires_at = expires_at
            if athlete_id is not None:
                token.athlete_id = athlete_id
        else:
            token = StravaToken(
                user_id=self.user_id,
                access_token=access_token,
                refresh_token=refresh_token,
                expires_at=expires_at,
                athlete_id=athlete_id,
            )
            db.add(token)
        db.commit()
        db.refresh(token)
        return token

    # ======================================================
    # 🔄 OAuth : obtention et refresh des tokens
    # ======================================================

    async def exchange_code(self, code: str) -> Dict[str, Any]:
        """Échange le code obtenu via /auth/callback contre access/refresh tokens et enregistre en base."""
        data = await self.exchange_code_payload(code)

        access_token = data["access_token"]
        refresh_token = data["refresh_token"]
        expires_at = data["expires_at"]
        athlete_id = data.get("athlete", {}).get("id")

        db = SessionLocal()
        try:
            self._save_tokens(db, access_token, refresh_token, expires_at, athlete_id)
        finally:
            db.close()

        print("✅ Tokens Strava enregistrés en base")
        return data

    async def exchange_code_payload(self, code: str) -> Dict[str, Any]:
        """Échange le code OAuth Strava et renvoie la charge utile sans persister les tokens."""
        async with httpx.AsyncClient() as client:
            r = await client.post(
                STRAVA_OAUTH_TOKEN_URL,
                data={
                    "client_id": STRAVA_CLIENT_ID,
                    "client_secret": STRAVA_CLIENT_SECRET,
                    "code": code,
                    "grant_type": "authorization_code",
                },
                timeout=15.0,
            )
        r.raise_for_status()
        return r.json()

    async def ensure_token(self) -> str:
        """Vérifie si le token Strava est valide, sinon le rafraîchit."""
        db = SessionLocal()
        try:
            token = self._get_token_record(db)
            if not token:
                raise RuntimeError("Aucun token Strava trouvé pour cet utilisateur.")

            now = int(time.time())
            if token.expires_at > now + 60:
                return token.access_token

            # Token expiré → refresh
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    STRAVA_OAUTH_TOKEN_URL,
                    data={
                        "client_id": STRAVA_CLIENT_ID,
                        "client_secret": STRAVA_CLIENT_SECRET,
                        "grant_type": "refresh_token",
                        "refresh_token": token.refresh_token,
                    },
                    timeout=15.0,
                )
            r.raise_for_status()
            data = r.json()

            access_token = data["access_token"]
            refresh_token = data["refresh_token"]
            expires_at = data["expires_at"]
            athlete_id = data.get("athlete", {}).get("id", token.athlete_id)

            token = self._save_tokens(db, access_token, refresh_token, expires_at, athlete_id)
            print("♻️ Token Strava rafraîchi")
            return token.access_token

        finally:
            db.close()

    # ======================================================
    # 🌍 Requêtes HTTP vers l’API Strava
    # ======================================================

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        access_token = await self.ensure_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient() as client:
            r = await client.get(f"{STRAVA_BASE_URL}{path}", headers=headers, params=params or {}, timeout=20.0)
        r.raise_for_status()
        return r.json()

    async def _put(self, path: str, data: Dict[str, Any]) -> Any:
        access_token = await self.ensure_token()
        headers = {"Authorization": f"Bearer {access_token}"}
        async with httpx.AsyncClient() as client:
            r = await client.put(f"{STRAVA_BASE_URL}{path}", headers=headers, data=data, timeout=20.0)
        r.raise_for_status()
        return r.json()

    # ======================================================
    # 🚴 Fonctions publiques
    # ======================================================

    async def list_activities(self, per_page: int = 1) -> List[Dict[str, Any]]:
        """Récupère les dernières activités Strava."""
        return await self._get("/athlete/activities", params={"per_page": per_page, "page": 1})

    async def get_activity(self, activity_id: int) -> Dict[str, Any]:
        """Récupère les détails d'une activité."""
        return await self._get(f"/activities/{activity_id}")

    async def update_activity_description(self, activity_id: int, description: str) -> Dict[str, Any]:
        """Met à jour la description d'une activité Strava."""
        desc = (description or "")[:1800]  # limite API
        return await self._put(f"/activities/{activity_id}", data={"description": desc})

    async def get_streams(
        self,
        activity_id: int,
        keys: str = "time,latlng,altitude,heartrate,cadence,velocity_smooth,moving,distance,watts,grade_smooth,temp",
        key_by_type: bool = True,
    ) -> Dict[str, Any]:
        """
        Récupère les données brutes (streams) pour une activité Strava :
        GPS, cardio, cadence, vitesse, etc.
        """
        params = {"keys": keys, "key_by_type": "true" if key_by_type else "false"}
        return await self._get(f"/activities/{activity_id}/streams", params=params)
