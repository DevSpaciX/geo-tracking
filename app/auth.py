"""Simplified authentication: the caller identifies itself with ``X-User-Id``.

A real deployment would validate a JWT / session cookie here; the rest of the
code only depends on the resolved ``user_id`` string.
"""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import Header, HTTPException, WebSocket, status

USER_ID_RE = re.compile(r"^[A-Za-z0-9_.@-]{1,64}$")


def validate_user_id(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    return value if USER_ID_RE.fullmatch(value) else None


async def get_current_user_id(x_user_id: Annotated[str | None, Header(alias="X-User-Id")] = None) -> str:
    user_id = validate_user_id(x_user_id)
    if user_id is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Missing or invalid X-User-Id header")
    return user_id


def user_id_from_websocket(websocket: WebSocket) -> str | None:
    """Browsers cannot set headers on WebSocket upgrades, so accept a query param too."""
    return validate_user_id(websocket.headers.get("x-user-id") or websocket.query_params.get("user_id"))
