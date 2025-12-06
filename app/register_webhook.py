import os
import requests
from dotenv import load_dotenv

load_dotenv()  # charge .env local

CLIENT_ID = os.getenv("STRAVA_CLIENT_ID")
CLIENT_SECRET = os.getenv("STRAVA_CLIENT_SECRET")
CALLBACK_URL = os.getenv("CALLBACK_URL")
VERIFY_TOKEN = os.getenv("STRAVA_VERIFY_TOKEN", "benoit-strava-token")

if not CALLBACK_URL:
    raise SystemExit("❌ CALLBACK_URL manquant dans .env")

print("➡️ Enregistrement webhook Strava…")
print("URL =", CALLBACK_URL)

# Supprimer les anciens webhooks
# (Strava ne gère qu'1-2 subscriptions max)
subs = requests.get(
    "https://www.strava.com/api/v3/push_subscriptions",
    params={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
).json()

for sub in subs:
    print("🗑️ suppression ancienne subscription :", sub)
    requests.delete(
        f"https://www.strava.com/api/v3/push_subscriptions/{sub['id']}",
        params={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET},
    )

# Enregistrer le bon webhook
resp = requests.post(
    "https://www.strava.com/api/v3/push_subscriptions",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "callback_url": CALLBACK_URL,
        "verify_token": VERIFY_TOKEN,
    },
)

print("📌 Strava response:")
print(resp.status_code, resp.text)
