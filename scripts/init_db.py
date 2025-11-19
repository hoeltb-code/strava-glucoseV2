# scripts/init_db.py
import os
import sys
sys.path.append(os.path.abspath("."))

from app.database import init_db, DATABASE_URL

if __name__ == "__main__":
    print("🔧 Initialisation de la base de données...")
    init_db()

    # On affiche sur QUELLE base on a travaillé
    if DATABASE_URL.startswith("sqlite"):
        print(f"✅ Base SQLite initialisée : {DATABASE_URL}")
    else:
        print(f"✅ Base distante initialisée : {DATABASE_URL}")
