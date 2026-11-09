import logging
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field

log = logging.getLogger(__name__)


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
    extra: dict[str, Any] = Field(default_factory=dict)
    source_version: str
    updated_at: datetime


CORE = {"station_id", "name", "short_name", "lat", "lon",
        "capacity", "region_id", "is_charging"}


def text_of(value) -> str:
    """Tên trạm: v2 là chuỗi, v3 là mảng [{text, language}]."""
    if isinstance(value, list):
        return value[0]["text"] if value else ""
    return value or ""


def normalize_information(feed: dict, system_id: str) -> list[StationInfo]:
    ver = str(feed.get("version", "2.2"))
    now = datetime.now(timezone.utc)
    out, skipped = [], 0
    for s in feed["data"]["stations"]:
        try:
            rid = s.get("region_id")
            short = s.get("short_name")
            out.append(StationInfo(
                system_id=system_id,
                station_id=str(s["station_id"]),
                name=text_of(s["name"]),
                short_name=text_of(short) if short is not None else None,
                lat=s["lat"],
                lon=s["lon"],
                capacity=s.get("capacity"),
                region_id=str(rid) if rid is not None else None,
                is_charging=bool(s.get("is_charging", False)),
                extra={k: v for k, v in s.items() if k not in CORE},
                source_version=ver,
                updated_at=now,
            ))
        except (KeyError, TypeError, ValueError) as e:  # ValidationError là ValueError
            skipped += 1
            log.debug("[%s] bỏ qua station_information lỗi: %s", system_id, e)
    if skipped:
        log.warning(
            "[%s] station_information: bỏ qua %d bản ghi lỗi", system_id, skipped)
    return out
