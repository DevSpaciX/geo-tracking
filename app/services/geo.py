"""Small pure geo helpers (no database)."""

from __future__ import annotations

import math
from typing import NamedTuple

EARTH_RADIUS_M = 6_371_008.8


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres (used by tests and the generator only;
    the service itself relies on PostGIS for distance checks)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


class BBox(NamedTuple):
    """Viewport filter a live client may set: [min_lon, min_lat, max_lon, max_lat]."""

    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float

    @classmethod
    def parse(cls, raw: object) -> "BBox":
        if not isinstance(raw, (list, tuple)) or len(raw) != 4:
            raise ValueError("bbox must be [min_lon, min_lat, max_lon, max_lat]")
        min_lon, min_lat, max_lon, max_lat = (float(v) for v in raw)
        if not (-90 <= min_lat <= max_lat <= 90) or not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
            raise ValueError("bbox out of range")
        return cls(min_lon, min_lat, max_lon, max_lat)

    def contains(self, lat: float, lon: float) -> bool:
        if not (self.min_lat <= lat <= self.max_lat):
            return False
        if self.min_lon <= self.max_lon:
            return self.min_lon <= lon <= self.max_lon
        # Viewport crosses the antimeridian.
        return lon >= self.min_lon or lon <= self.max_lon
