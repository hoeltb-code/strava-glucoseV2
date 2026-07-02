# app/routers/auth_dexcom.py

from typing import Optional

from fastapi import APIRouter, Query
from fastapi.responses import RedirectResponse

router = APIRouter(tags=["auth-dexcom"])


@router.get("/auth/dexcom")
def auth_dexcom(user_id: int = 1):
    return RedirectResponse(
        url=(
            f"/ui/user/{user_id}/profile"
            "?dexcom_status=warn"
            "&dexcom_msg=La+connexion+Dexcom+se+fait+maintenant+via+le+formulaire+Dexcom+Share."
            "#dexcom"
        ),
        status_code=302,
    )


@router.get("/auth/dexcom/callback")
def auth_dexcom_callback(
    code: str = "",
    error: str = "",
    state: Optional[str] = Query(default=None),
):
    user_id = state or "1"
    message = "Le+callback+OAuth+Dexcom+n'est+plus+utilis%C3%A9.+Configure+Dexcom+Share+depuis+le+profil."
    if error:
        message = f"Dexcom+OAuth+abandonn%C3%A9+%3A+{error}"
    return RedirectResponse(
        url=f"/ui/user/{user_id}/profile?dexcom_status=warn&dexcom_msg={message}#dexcom",
        status_code=302,
    )
