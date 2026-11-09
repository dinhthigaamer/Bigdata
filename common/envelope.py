"""
Envelope bọc mỗi bản ghi đã chuẩn hóa. Mỗi loại dữ liệu có một class riêng, tự quyết định:
  - payload là model nào,
  - topic Kafka nào,
  - key của message là gì,
  - metadata nguồn (version, last_updated, ttl) lấy từ đâu (hàm build).

Thêm loại dữ liệu mới = viết thêm một class + đưa vào AnyEnvelope.
"""
import json
from datetime import datetime, timezone
from typing import Annotated, Any, ClassVar, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from common.schemas.station_info import StationInfo
from common.schemas.station_status import StationStatus, to_dt
from common.schemas.weather import WeatherMessage

SCHEMA_VERSION = "1.0"


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ------------------------------------------------------------------ Base
class EnvelopeBase(BaseModel):
    """Phần chung của mọi envelope."""
    model_config = ConfigDict(frozen=True)

    topic: ClassVar[str]

    schema_version: str = SCHEMA_VERSION
    record_type: str
    system_id: str
    source_version: Optional[str] = None
    source_last_updated: Optional[datetime] = None
    source_ttl: Optional[int] = None
    fetched_at: datetime = Field(default_factory=_now)
    payload: BaseModel

    def key(self) -> str:
        """Key của message Kafka. Mỗi loại tự định nghĩa."""
        raise NotImplementedError


# ------------------------------------------------------------------ GBFS
class GbfsEnvelope(EnvelopeBase):
    """Logic chung của các feed GBFS: metadata nằm ở cấp feed (version, last_updated, ttl)."""

    @classmethod
    def build(cls, feed: dict, payload: BaseModel):
        ttl = feed.get("ttl")
        return cls(
            system_id=payload.system_id,
            source_version=str(feed.get("version", "2.2")),
            source_last_updated=to_dt(feed.get("last_updated")),
            source_ttl=int(ttl) if isinstance(ttl, (int, float)) else None,
            payload=payload,
        )

    def key(self) -> str:
        return f"{self.system_id}:{self.payload.station_id}"


class StationStatusEnvelope(GbfsEnvelope):
    topic: ClassVar[str] = "gbfs.station_status"
    record_type: Literal["station_status"] = "station_status"
    payload: StationStatus


class StationInfoEnvelope(GbfsEnvelope):
    topic: ClassVar[str] = "gbfs.station_information"
    record_type: Literal["station_information"] = "station_information"
    payload: StationInfo


# --------------------------------------------------------------- Weather
class WeatherEnvelope(EnvelopeBase):
    """Thời tiết: không có feed-level metadata, system_id suy ra từ location_id."""
    topic: ClassVar[str] = "weather.open_meteo"
    record_type: Literal["weather_message"] = "weather_message"
    payload: WeatherMessage

    @classmethod
    def build(cls, payload: WeatherMessage):
        return cls(
            system_id=f"open-meteo:{payload.location_id}",
            source_ttl=payload.interval,
            payload=payload,
        )

    def key(self) -> str:
        # (địa điểm, mốc thời gian): bản mới nhất của cùng khóa ghi đè bản cũ (forecast cập nhật)
        return f"{self.payload.location_id}:{self.payload.time:%Y%m%dT%H%M}"


# --------------------------------------------------------------- Parse
AnyEnvelope = Annotated[
    Union[StationStatusEnvelope, StationInfoEnvelope, WeatherEnvelope],
    Field(discriminator="record_type"),
]
_adapter = TypeAdapter(AnyEnvelope)


def parse_envelope(raw: Union[dict, str, bytes]) -> EnvelopeBase:
    """Phía nhận: JSON/dict -> đúng class envelope theo record_type, payload đã được validate."""
    obj: Any = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    major = str(obj.get("schema_version", "")).split(".")[0]
    if major != SCHEMA_VERSION.split(".")[0]:
        raise ValueError(
            f"schema_version không hỗ trợ: {obj.get('schema_version')}")
    # record_type lạ -> ValidationError (là ValueError)
    return _adapter.validate_python(obj)
