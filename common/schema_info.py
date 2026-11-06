from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime, timezone


class StationInfo(BaseModel):
    system_id: str
    station_id: str
    name: str
    short_name: Optional[str] = None
    lat: float
    lon: float
    capacity: Optional[int] = None
    region_id: Optional[str] = None
    is_charging: bool = False
    extra: dict[str, Any] = {}
    source_version: str
    updated_at: datetime


CORE = {"station_id", "name", "short_name", "lat", "lon",
        "capacity", "region_id", "is_charging"}


def normalize_information(feed: dict, system_id: str) -> list[StationInfo]:
    ver = feed.get("version", "2.2")
    now = datetime.now(timezone.utc)
    out = []
    for s in feed["data"]["stations"]:
        rid = s.get("region_id")
        out.append(StationInfo(
            system_id=system_id,
            station_id=str(s["station_id"]),
            name=s["name"],
            short_name=s.get("short_name"),
            lat=s["lat"],
            lon=s["lon"],
            capacity=s.get("capacity"),
            region_id=str(rid) if rid is not None else None,
            is_charging=bool(s.get("is_charging", False)),
            extra={k: v for k, v in s.items() if k not in CORE},
            source_version=ver,
            updated_at=now,
        ))
    return out
