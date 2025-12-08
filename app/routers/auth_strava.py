# app/routers/auth_strava.py

from typing import Optional
import base64
import json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.settings import settings
from app.strava_client import StravaClient

router = APIRouter(tags=["auth-strava"])
templates = Jinja2Templates(directory="templates")


def _encode_state(user_id: int, next_path: Optional[str]) -> str:
    if not next_path:
        return str(user_id)
    payload = {"user_id": user_id, "next": next_path}
    raw = json.dumps(payload)
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("utf-8")


def _decode_state(state: Optional[str]) -> tuple[int, Optional[str]]:
    if state is None:
        return 1, None
    try:
        return int(state), None
    except ValueError:
        try:
            padding = "=" * (-len(state) % 4)
            decoded = base64.urlsafe_b64decode((state + padding).encode("utf-8")).decode("utf-8")
            data = json.loads(decoded)
            user_id = int(data.get("user_id", 1))
            next_path = data.get("next")
            return user_id, next_path
        except Exception:
            raise HTTPException(400, "Invalid state value returned by Strava")


@router.get("/auth/strava")
def auth_strava(user_id: int = 1, next: Optional[str] = Query(default=None)):
    """
    Démarre le flux OAuth Strava pour un utilisateur donné (user_id).

    En pratique :
      1) Tu fais /auth/login → tu récupères user_id dans la réponse
      2) Tu appelles /auth/strava?user_id=CE_ID
    Le user_id est passé à Strava via le paramètre 'state'.
    """
    next_path = next if next and next.startswith("/") else None
    state = _encode_state(user_id, next_path)
    scopes = "activity:read_all,activity:write"
    url = (
        "https://www.strava.com/oauth/authorize"
        f"?client_id={settings.STRAVA_CLIENT_ID}"
        f"&redirect_uri={settings.STRAVA_REDIRECT_URI}"
        f"&response_type=code&approval_prompt=auto"
        f"&scope={scopes}"
        f"&state={state}"
    )
    return RedirectResponse(url)


@router.get("/auth/callback", response_class=HTMLResponse)
async def auth_callback(
    request: Request,
    code: str = "",
    error: str = "",
    state: Optional[str] = Query(default=None),
):
    """
    Callback Strava : reçoit le code d'autorisation + le state (ici user_id).
    Enregistre les tokens Strava en base, puis affiche une page
    qui redirige vers le dashboard utilisateur.
    """
    if error:
        raise HTTPException(400, f"OAuth error: {error}")

    user_id, next_path = _decode_state(state)

    cli = StravaClient(user_id=user_id)
    data = await cli.exchange_code(code)

    athlete_id = data.get("athlete", {}).get("id")

    print(
        "✅ Strava tokens enregistrés en base pour :",
        {"user_id": user_id, "athlete_id": athlete_id},
    )

    if next_path and next_path.startswith("/"):
        return RedirectResponse(url=next_path)

    return templates.TemplateResponse(
        "auth_strava_callback.html",
        {
            "request": request,
            "user_id": user_id,
        },
    )
