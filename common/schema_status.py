from pydantic import BaseModel, Field
from typing import Optional, Any, List
from datetime import datetime, timezone


class VehicleCount(BaseModel):
    vehicle_type_id: str
    count: int = Field(ge=0)


class StationStatus(BaseModel):
    system_id: str
    station_id: str
    snapshot_ts: datetime
    last_reported: Optional[datetime] = None
    num_bikes_available: int = Field(ge=0)
    num_bikes_disabled: int = Field(default=0, ge=0)
    num_docks_available: int = Field(default=0, ge=0)
    num_docks_disabled: int = Field(default=0, ge=0)
    num_ebikes_available: Optional[int] = Field(default=None, ge=0)
    is_installed: bool
    is_renting: bool
    is_returning: bool
    vehicle_types_available: List[VehicleCount] = []
    extra: dict[str, Any] = {}
    source_version: str
    ingest_ts: datetime


CORE = {
    "station_id", "num_bikes_available", "num_bikes_disabled",
    "num_docks_available", "num_docks_disabled", "num_ebikes_available",
    "is_installed", "is_renting", "is_returning", "last_reported",
    "vehicle_types_available",
}


def to_dt(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc) if v > 86400 else None
    # dự phòng cho 3.0
    return datetime.fromisoformat(v.replace("Z", "+00:00"))


def normalize_status(feed: dict, system_id: str) -> list[StationStatus]:
    ver = feed.get("version", "2.2")
    snap = to_dt(feed["last_updated"])
    now = datetime.now(timezone.utc)
    out = []
    for s in feed["data"]["stations"]:
        out.append(StationStatus(
            system_id=system_id,
            station_id=str(s["station_id"]),
            snapshot_ts=snap,
            last_reported=to_dt(s.get("last_reported")),
            num_bikes_available=s["num_bikes_available"],
            num_bikes_disabled=s.get("num_bikes_disabled", 0),
            num_docks_available=s.get("num_docks_available", 0),
            num_docks_disabled=s.get("num_docks_disabled", 0),
            num_ebikes_available=s.get("num_ebikes_available"),
            is_installed=bool(s["is_installed"]),
            is_renting=bool(s["is_renting"]),
            is_returning=bool(s["is_returning"]),
            vehicle_types_available=s.get("vehicle_types_available", []),
            extra={k: v for k, v in s.items() if k not in CORE},
            source_version=ver,
            ingest_ts=now,
        ))
    return out
