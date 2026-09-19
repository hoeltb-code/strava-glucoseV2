"""Run with python -m scripts.backfill_activity_analytics (local DB, no external calls)."""
from app.database import init_db
from app.analytics_service import maintain_batch

if __name__ == "__main__":
    init_db()
    total=0
    while count := maintain_batch(25):
        total+=count
        print(f"{total} activités préparées",flush=True)
