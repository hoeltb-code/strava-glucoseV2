# app/routers/auth_dexcom.py

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import SessionLocal
from app.dexcom_client import DexcomClient

router = APIRouter(tags=["auth-dexcom"])
templates = Jinja2Templates(directory="templates")


@router.get("/auth/dexcom")
def auth_dexcom(user_id: int = 1):
    """
    Démarre le flux OAuth Dexcom pour un utilisateur donné (user_id).
    Le user_id est passé dans le paramètre 'state' de l'URL Dexcom.
    """
    db = SessionLocal()
    try:
        cli = DexcomClient(user_id=user_id, db=db)
        url = cli.get_authorization_url()
    finally:
        db.close()

    return RedirectResponse(url)


@router.get("/auth/dexcom/callback", response_class=HTMLResponse)
async def auth_dexcom_callback(
    request: Request,
    code: str = "",
    error: str = "",
    state: Optional[str] = Query(default=None),
):
    """
    Callback Dexcom :
    - reçoit le code d'autorisation + state (user_id),
    - demande à DexcomClient d'échanger le code contre des tokens
      et de les enregistrer dans DexcomToken,
    - affiche une page de confirmation avec un lien vers le profil utilisateur.
    """
    if error:
        raise HTTPException(400, f"Dexcom OAuth error: {error}")

    try:
        user_id = int(state) if state is not None else 1
    except ValueError:
        raise HTTPException(400, "Invalid state (expected user_id)")

    db = SessionLocal()
    try:
        cli = DexcomClient(user_id=user_id, db=db)
        await cli.exchange_code_for_tokens(code)
        print(f"✅ Dexcom tokens enregistrés en base pour user_id={user_id}")
    finally:
        db.close()

    return templates.TemplateResponse(
        "auth_dexcom_callback.html",
        {
            "request": request,
            "user_id": user_id,
        },
    )
