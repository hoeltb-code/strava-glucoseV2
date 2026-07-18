# app/routers/webhooks.py
from fastapi import APIRouter, HTTPException, Query, Body
from fastapi.responses import JSONResponse
from app.settings import settings
from app.database import SessionLocal
from app.models import StravaToken

router = APIRouter(tags=["webhooks"])  # 🔴 PAS de prefix ici

@router.get("/webhooks/strava")
async def strava_webhook_verify(
    hub_mode: str | None = Query(None, alias="hub.mode"),
    hub_challenge: str | None = Query(None, alias="hub.challenge"),
    hub_verify_token: str | None = Query(None, alias="hub.verify_token"),
):
    if settings.STRAVA_VERIFY_TOKEN and hub_verify_token != settings.STRAVA_VERIFY_TOKEN:
        raise HTTPException(status_code=403, detail="verify token mismatch")

    if hub_challenge:
        return JSONResponse({"hub.challenge": hub_challenge}, status_code=200)

    return JSONResponse({"error": "missing hub.challenge"}, status_code=400)


@router.post("/webhooks/strava")
async def webhook_event(body: dict = Body(...)):
    print("📩 Webhook reçu :", body)

    if body.get("object_type") != "activity":
        return {"ok": True}
    if body.get("aspect_type") not in ("create", "update"):
        return {"ok": True}

    activity_id = body.get("object_id")
    athlete_id = body.get("owner_id")
    if not activity_id or not athlete_id:
        return {"ok": True}

    db = SessionLocal()
    try:
        token = (
            db.query(StravaToken)
            .filter(StravaToken.athlete_id == int(athlete_id))
            .first()
        )
        if not token:
            print(f"⚠️ Aucun StravaToken trouvé pour athlete_id={athlete_id}")
            return {"status": "unknown_athlete"}

        user_id = token.user_id
        print(f"➡️ Webhook mappé sur user_id={user_id} (athlete_id={athlete_id})")
    finally:
        db.close()

    # ✅ import local pour éviter l'import circulaire
    try:
        from app.main import request_activity_enrichment
        result = await request_activity_enrichment(
            int(activity_id),
            user_id=user_id,
            trigger_source="webhook",
            immediate=True,
        )
        run_status = result.get("status")
        job_status = result.get("job_status")
        reason = result.get("reason") or result.get("job_last_reason")
        retry_at = result.get("job_next_retry_at") or result.get("retry_at")
        if job_status == "retry":
            print(
                f"⏳ Activité {activity_id} en reprise programmée pour user_id={user_id} "
                f"(run_status={run_status}, reason={reason}, retry_at={retry_at})."
            )
        elif job_status == "failed":
            print(
                f"⚠️ Activité {activity_id} échouée pour user_id={user_id} "
                f"(run_status={run_status}, reason={reason})."
            )
        else:
            print(
                f"✅ Activité {activity_id} traitée pour user_id={user_id} "
                f"(run_status={run_status}, job_status={job_status})."
            )
    except Exception as e:
        print(f"⚠️ Erreur traitement activité {activity_id} pour user_id={user_id} :", e)

    return {"status": "received"}
