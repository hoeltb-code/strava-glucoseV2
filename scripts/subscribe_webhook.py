# scripts/subscribe_webhook.py
import os
import sys
import httpx

STRAVA_PUSH = "https://www.strava.com/api/v3/push_subscriptions"

def main() -> int:
    client_id = os.getenv("STRAVA_CLIENT_ID")
    client_secret = os.getenv("STRAVA_CLIENT_SECRET")
    verify_token = os.getenv("STRAVA_VERIFY_TOKEN", "devtoken")
    callback_url = os.getenv("CALLBACK_URL")  # ex: https://xxxx.ngrok.io/webhooks/strava

    if not (client_id and client_secret and callback_url):
        print("Missing env. Need STRAVA_CLIENT_ID, STRAVA_CLIENT_SECRET, CALLBACK_URL")
        return 2

    with httpx.Client(timeout=20) as c:
        # (optionnel) lister les subscriptions existantes
        try:
            r_list = c.get(STRAVA_PUSH, params={"client_id": client_id, "client_secret": client_secret})
            r_list.raise_for_status()
            print("Existing subscriptions:", r_list.json())
        except Exception as e:
            print("List subscriptions failed (ignored):", e)

        # créer l'abonnement
        r = c.post(
            STRAVA_PUSH,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "callback_url": callback_url,
                "verify_token": verify_token,
            },
        )
        if r.status_code in (200, 201, 409):
            print("Subscription response:", r.status_code, r.text)
            return 0
        else:
            print("Subscription failed:", r.status_code, r.text)
            return 1

if __name__ == "__main__":
    sys.exit(main())
