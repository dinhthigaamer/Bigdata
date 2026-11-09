import logging
from datetime import datetime, timezone
from typing import Any, List, Optional

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


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
    vehicle_types_available: List[VehicleCount] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)
    source_version: str
    ingest_ts: datetime


CORE = {
    "station_id", "num_bikes_available", "num_bikes_disabled",
    "num_docks_available", "num_docks_disabled", "num_ebikes_available",
    "is_installed", "is_renting", "is_returning", "last_reported",
    "vehicle_types_available",
}


def to_dt(v):
    """v2: số giây POSIX; v3: chuỗi ISO 8601. Giá trị 0/rỗng -> None."""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc) if v > 86400 else None
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


def normalize_status(feed: dict, system_id: str) -> list[StationStatus]:
    ver = str(feed.get("version", "2.2"))
    now = datetime.now(timezone.utc)
    snap = to_dt(feed.get("last_updated")) or now
    out, skipped = [], 0
    for s in feed["data"]["stations"]:
        try:
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
                vehicle_types_available=s.get("vehicle_types_available") or [],
                extra={k: v for k, v in s.items() if k not in CORE},
                source_version=ver,
                ingest_ts=now,
            ))
        except (KeyError, TypeError, ValueError) as e:  # ValidationError là ValueError
            skipped += 1
            log.debug("[%s] bỏ qua station_status lỗi: %s", system_id, e)
    if skipped:
        log.warning("[%s] station_status: bỏ qua %d bản ghi lỗi",
                    system_id, skipped)
    return out
