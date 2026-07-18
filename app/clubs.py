from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parent.parent
CLUBS_JSON_PATH = ROOT_DIR / "data" / "clubs.json"
CLUB_LOGOS_DIR = ROOT_DIR / "static" / "club_logos"


def _normalize_slug(value: Any) -> str:
    return str(value or "").strip().lower()


def _sanitize_logo_filename(value: Any) -> str | None:
    filename = Path(str(value or "")).name.strip()
    if not filename:
        return None
    return filename


def load_club_catalog() -> list[dict[str, Any]]:
    try:
        raw = json.loads(CLUBS_JSON_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

    if not isinstance(raw, list):
        return []

    clubs: list[dict[str, Any]] = []
    seen_slugs: set[str] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        slug = _normalize_slug(entry.get("slug"))
        name = str(entry.get("name") or "").strip()
        if not slug or not name or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        clubs.append(
            {
                "slug": slug,
                "name": name,
                "logo": _sanitize_logo_filename(entry.get("logo")),
                "active": bool(entry.get("active", True)),
            }
        )
    return clubs


def get_available_clubs(*, include_inactive: bool = False) -> list[dict[str, Any]]:
    clubs = load_club_catalog()
    if include_inactive:
        return clubs
    return [club for club in clubs if club.get("active", True)]


def get_club_by_slug(slug: str | None, *, include_inactive: bool = True) -> dict[str, Any] | None:
    slug_norm = _normalize_slug(slug)
    if not slug_norm:
        return None
    clubs = load_club_catalog() if include_inactive else get_available_clubs()
    for club in clubs:
        if club["slug"] == slug_norm:
            return club
    return None


def get_club_logo_static_url(logo_filename: str | None) -> str | None:
    filename = _sanitize_logo_filename(logo_filename)
    if not filename:
        return None
    if not (CLUB_LOGOS_DIR / filename).is_file():
        return None
    return f"/static/club_logos/{filename}"


def build_club_payload(slug: str | None) -> dict[str, Any] | None:
    club = get_club_by_slug(slug, include_inactive=True)
    if club is None:
        slug_norm = _normalize_slug(slug)
        if not slug_norm:
            return None
        return {
            "slug": slug_norm,
            "name": slug_norm,
            "logo": None,
            "logo_url": None,
            "active": False,
            "missing": True,
        }
    return {
        **club,
        "logo_url": get_club_logo_static_url(club.get("logo")),
        "missing": False,
    }
