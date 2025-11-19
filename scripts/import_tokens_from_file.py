# scripts/import_tokens_from_file.py
import os
import sys
import json
from datetime import datetime

sys.path.append(os.path.abspath("."))

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import User, StravaToken

TOKENS_PATH = "app/tokens.json"  # adapte si besoin

def main():
    if not os.path.exists(TOKENS_PATH):
        print(f"❌ Fichier {TOKENS_PATH} introuvable.")
        return

    with open(TOKENS_PATH, "r") as f:
        data = json.load(f)

    access_token = data.get("access_token")
    refresh_token = data.get("refresh_token")
    expires_at = data.get("expires_at")

    db: Session = SessionLocal()
    user = db.query(User).filter(User.id == 1).first()
    if not user:
        print("❌ Aucun utilisateur id=1 trouvé.")
        return

    token = StravaToken(
        user_id=user.id,
        access_token=access_token,
        refresh_token=refresh_token,
        expires_at=expires_at,
        created_at=datetime.utcnow(),
    )
    db.add(token)
    db.commit()
    print(f"✅ Tokens importés pour user_id={user.id}")

if __name__ == "__main__":
    main()
