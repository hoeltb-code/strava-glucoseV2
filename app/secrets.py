from __future__ import annotations

import base64
import hashlib
from typing import Optional

from app.settings import settings


_SECRET_PREFIX = "xor1:"


def _secret_bytes() -> bytes:
    secret = (settings.SECRET_KEY or "").encode("utf-8")
    return hashlib.sha256(secret).digest()


def encrypt_secret(value: Optional[str]) -> Optional[str]:
    if value in (None, ""):
        return value

    payload = value.encode("utf-8")
    key = _secret_bytes()
    encrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(payload))
    return _SECRET_PREFIX + base64.urlsafe_b64encode(encrypted).decode("ascii")


def decrypt_secret(value: Optional[str]) -> Optional[str]:
    if value in (None, ""):
        return value
    if not isinstance(value, str):
        return value
    if not value.startswith(_SECRET_PREFIX):
        return value

    encoded = value[len(_SECRET_PREFIX):]
    encrypted = base64.urlsafe_b64decode(encoded.encode("ascii"))
    key = _secret_bytes()
    payload = bytes(b ^ key[i % len(key)] for i, b in enumerate(encrypted))
    return payload.decode("utf-8")
