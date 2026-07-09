import os


DEFAULT_ENV = {
    "STRAVA_CLIENT_ID": "test",
    "STRAVA_CLIENT_SECRET": "test",
    "STRAVA_REDIRECT_URI": "http://localhost/strava",
    "STRAVA_VERIFY_TOKEN": "test",
    "LIBRE_EMAIL": "libre@example.com",
    "LIBRE_PASSWORD": "secret",
    "DEXCOM_CLIENT_ID": "test",
    "DEXCOM_CLIENT_SECRET": "test",
    "DEXCOM_REDIRECT_URI": "http://localhost/dexcom",
    "SECRET_KEY": "test-secret-key",
}


for key, value in DEFAULT_ENV.items():
    os.environ.setdefault(key, value)
