# app/routers/__init__.py
"""
Ce package regroupe les différents routers FastAPI de l'application.

Pour l'instant, on expose seulement :
- auth_strava
- auth_dexcom

Le router webhooks sera branché plus tard, une fois la fonction
enrich_activity déplacée hors de app.main pour éviter les import cycles.
"""

from . import auth_strava
from . import auth_dexcom

# ⚠️ NE PAS importer webhooks ici pour le moment, sinon boucle circulaire :
# from . import webhooks
