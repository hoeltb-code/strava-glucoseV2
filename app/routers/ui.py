# app/routers/ui.py

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import SessionLocal
from app.models import User, Activity

router = APIRouter(prefix="/ui", tags=["ui"])
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """
    Page d'accueil de l'interface utilisateur.
    Affiche simplement un tableau des utilisateurs enregistrés.
    """
    db = SessionLocal()
    users = db.query(User).all()
    db.close()

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "users": users,
        },
    )


@router.get("/user/{user_id}", response_class=HTMLResponse)
async def user_dashboard(request: Request, user_id: int):
    """
    Affiche la liste des activités d'un utilisateur donné.
    """
    db = SessionLocal()
    user = db.query(User).filter(User.id == user_id).first()
    activities = (
        db.query(Activity)
        .filter(Activity.user_id == user_id)
        .order_by(Activity.start_date.desc())
        .all()
    )
    db.close()

    return templates.TemplateResponse(
        "user_dashboard.html",
        {
            "request": request,
            "user": user,
            "activities": activities,
        },
    )
