# app/auth.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from passlib.context import CryptContext

from app.database import get_db
from app import models

router = APIRouter(prefix="/auth", tags=["auth"])

# Contexte de hashage des mots de passe
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


@router.post("/signup")
def signup(email: str, password: str, db: Session = Depends(get_db)):
    """
    Crée un nouvel utilisateur avec email + mot de passe.
    Le mot de passe est stocké sous forme hashée.
    """
    # Vérifie si l'email existe déjà
    existing_user = db.query(models.User).filter(models.User.email == email).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    # Hash du mot de passe
    hashed_pw = pwd_context.hash(password)

    # Création du nouvel utilisateur
    user = models.User(email=email, password_hash=hashed_pw)
    db.add(user)
    db.commit()
    db.refresh(user)

    return {"message": "User created successfully", "user_id": user.id}


@router.post("/login")
def login(email: str, password: str, db: Session = Depends(get_db)):
    """
    Vérifie l'email et le mot de passe.
    Pour l'instant, on renvoie juste un message + user_id si c'est ok.
    Ça suffit largement pour continuer le développement de la v6 en local.
    """
    user = db.query(models.User).filter(models.User.email == email).first()

    # Si l'utilisateur n'existe pas ou que le mot de passe est mauvais
    if not user or not pwd_context.verify(password, user.password_hash):
        raise HTTPException(status_code=400, detail="Invalid email or password")

    return {
        "message": "Login successful",
        "user_id": user.id,
        "email": user.email,
    }
