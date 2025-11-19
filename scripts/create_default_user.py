# scripts/create_default_user.py
import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath("."))

from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import User

def main():
    db: Session = SessionLocal()
    email = "benoit@example.com"
    password_hash = "dev-only"  # on fera un vrai hash plus tard

    existing = db.query(User).filter(User.email == email).first()
    if existing:
        print(f"⚠️ User {email} existe déjà (id={existing.id})")
        return

    user = User(email=email, password_hash=password_hash, created_at=datetime.utcnow())
    db.add(user)
    db.commit()
    db.refresh(user)
    print(f"✅ User créé : id={user.id}, email={user.email}")

if __name__ == "__main__":
    main()
