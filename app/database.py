# app/database.py
import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./strava_glucose.db")

connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def _ensure_column(table_name: str, column_name: str, ddl: str):
    inspector = inspect(engine)
    if table_name not in inspector.get_table_names():
        return
    columns = {col["name"] for col in inspector.get_columns(table_name)}
    if column_name in columns:
        return
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {ddl}"))


def _run_local_schema_fixes():
    is_sqlite = DATABASE_URL.startswith("sqlite")
    libre_last_fetch_type = "DATETIME" if is_sqlite else "TIMESTAMP"

    _ensure_column("dexcom_tokens", "share_username", "share_username TEXT")
    _ensure_column("dexcom_tokens", "share_password", "share_password TEXT")
    _ensure_column("dexcom_tokens", "share_region", "share_region VARCHAR(16)")
    _ensure_column(
        "libre_credentials",
        "last_fetch_at",
        f"last_fetch_at {libre_last_fetch_type}",
    )
    _ensure_column(
        "libre_credentials",
        "last_success_at",
        f"last_success_at {libre_last_fetch_type}",
    )
    _ensure_column(
        "libre_credentials",
        "last_fetch_context",
        "last_fetch_context VARCHAR(32)",
    )
    _ensure_column(
        "libre_credentials",
        "disabled_at",
        f"disabled_at {libre_last_fetch_type}",
    )
    _ensure_column(
        "libre_credentials",
        "disabled_reason",
        "disabled_reason VARCHAR(255)",
    )
    _ensure_column(
        "libre_credentials",
        "disabled_notified_at",
        f"disabled_notified_at {libre_last_fetch_type}",
    )
    _ensure_column(
        "user_settings",
        "desc_enable_auto_block",
        "desc_enable_auto_block BOOLEAN DEFAULT 1",
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE libre_credentials "
                "SET last_success_at = last_fetch_at "
                "WHERE last_success_at IS NULL AND last_fetch_at IS NOT NULL"
            )
        )

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    from app import models
    Base.metadata.create_all(bind=engine)
    _run_local_schema_fixes()
