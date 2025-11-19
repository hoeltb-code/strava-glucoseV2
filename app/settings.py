# app/settings.py
# -----------------------------------------------------------------------------
# Configuration centralisée de l’application via Pydantic.
# Chargement automatique depuis `.env`.
#
# 🔹 STRAVA : Authentification OAuth & Webhook
# 🔹 DATABASE : Connexion SQLAlchemy
# 🔹 LIBRELINKUP : Identifiants globaux (par défaut)
# 🔹 DEXCOM : Identifiants et endpoints OAuth2
# 🔹 AUTRES : Clé secrète, fuseau horaire, etc.
# -----------------------------------------------------------------------------
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    # === STRAVA ===
    STRAVA_CLIENT_ID: str = Field(...)
    STRAVA_CLIENT_SECRET: str = Field(...)
    STRAVA_REDIRECT_URI: str = Field(...)
    STRAVA_VERIFY_TOKEN: str = Field(...)

    # === DATABASE ===
    DATABASE_URL: str = Field(default="sqlite:///./strava_glucose.db")

    # === LIBRELINKUP (global pour fallback) ===
    LIBRE_EMAIL: str = Field(...)
    LIBRE_PASSWORD: str = Field(...)
    LIBRE_REGION: str = Field(default="fr")

    # === DEXCOM (OAuth2) ===
    # Identifiants (à placer dans .env)
    DEXCOM_CLIENT_ID: str = Field(...)
    DEXCOM_CLIENT_SECRET: str = Field(...)
    DEXCOM_REDIRECT_URI: str = Field(...)

    # Endpoints Dexcom (sandbox par défaut)
    DEXCOM_AUTH_URL: str = Field(default="https://sandbox-api.dexcom.com/v2/oauth2/login")
    DEXCOM_TOKEN_URL: str = Field(default="https://sandbox-api.dexcom.com/v2/oauth2/token")
    DEXCOM_EGV_URL: str = Field(default="https://sandbox-api.dexcom.com/v2/users/self/egvs")
    DEXCOM_SCOPE: str = Field(default="offline_access")

    # === SÉCURITÉ / AUTRES ===
    SECRET_KEY: str = Field(...)
    TZ: str | None = "Europe/Paris"

    # Lecture automatique du fichier .env
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
