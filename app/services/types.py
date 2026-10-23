"""Plain data records passed between the ingestion pipeline and the fan-out layer."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

AlertEvent = Literal["enter", "inside", "exit"]


@dataclass(slots=True)
class LocationUpdate:
    device_id: str
    latitude: float
    longitude: float
    timestamp: datetime

    def to_wire(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id,
            "lat": self.latitude,
            "lon": self.longitude,
            "ts": self.timestamp.isoformat(),
        }


@dataclass(slots=True)
class Alert:
    event: AlertEvent
    zone_id: uuid.UUID
    user_id: str
    zone_name: str
    device_id: str
    latitude: float
    longitude: float
    distance_m: float
    timestamp: datetime

    def to_wire(self) -> dict[str, Any]:
        return {
            "event": self.event,
            "zone_id": str(self.zone_id),
            "zone_name": self.zone_name,
            "device_id": self.device_id,
            "lat": self.latitude,
            "lon": self.longitude,
            "distance_m": round(self.distance_m, 1),
            "ts": self.timestamp.isoformat(),
        }
