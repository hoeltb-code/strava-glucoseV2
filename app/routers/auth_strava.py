# app/routers/auth_strava.py

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.settings import settings
from app.strava_client import StravaClient

router = APIRouter(tags=["auth-strava"])
templates = Jinja2Templates(directory="templates")


@router.get("/auth/strava")
def auth_strava(user_id: int = 1):
    """
    Démarre le flux OAuth Strava pour un utilisateur donné (user_id).

    En pratique :
      1) Tu fais /auth/login → tu récupères user_id dans la réponse
      2) Tu appelles /auth/strava?user_id=CE_ID
    Le user_id est passé à Strava via le paramètre 'state'.
    """
    scopes = "activity:read_all,activity:write"
    url = (
        "https://www.strava.com/oauth/authorize"
        f"?client_id={settings.STRAVA_CLIENT_ID}"
        f"&redirect_uri={settings.STRAVA_REDIRECT_URI}"
        f"&response_type=code&approval_prompt=auto"
        f"&scope={scopes}"
        f"&state={user_id}"
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

    try:
        user_id = int(state) if state is not None else 1
    except ValueError:
        raise HTTPException(400, "Invalid state (expected user_id)")

    cli = StravaClient(user_id=user_id)
    data = await cli.exchange_code(code)

    athlete_id = data.get("athlete", {}).get("id")

    print(
        "✅ Strava tokens enregistrés en base pour :",
        {"user_id": user_id, "athlete_id": athlete_id},
    )

    return templates.TemplateResponse(
        "auth_strava_callback.html",
        {
            "request": request,
            "user_id": user_id,
        },
    )
