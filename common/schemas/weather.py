from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict

WeatherDataType = Literal["observed", "forecast"]

# Các biến lấy từ khối minutely_15 của Open-Meteo
VARIABLES = (
    "temperature_2m", "relative_humidity_2m",
    "precipitation", "rain", "snowfall",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
    "visibility", "weather_code",
)


class WeatherMessage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    location_id: str              # khóa join với hệ thống GBFS, vd "montreal"
    # tọa độ lưới Open-Meteo trả về (có thể lệch so với tọa độ yêu cầu)
    latitude: float
    longitude: float

    time: datetime                # luôn là UTC
    fetch_time: datetime
    data_type: WeatherDataType
    source: Literal["open_meteo"] = "open_meteo"
    interval: int                 # giây

    # Open-Meteo có thể trả null cho từng biến, nên các số đo đều Optional
    temperature_2m: Optional[float] = None
    relative_humidity_2m: Optional[int] = None
    precipitation: Optional[float] = None
    rain: Optional[float] = None
    snowfall: Optional[float] = None
    wind_speed_10m: Optional[float] = None
    wind_direction_10m: Optional[int] = None
    wind_gusts_10m: Optional[float] = None
    visibility: Optional[float] = None
    weather_code: Optional[int] = None


def _to_utc(value, tz: timezone) -> datetime:
    """Open-Meteo trả chuỗi ISO theo múi giờ của request (mặc định GMT) hoặc unixtime."""
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return datetime.fromisoformat(value).replace(tzinfo=tz).astimezone(timezone.utc)


def parse_weather_response(
    data: dict,
    location_id: str,
    fetch_time: datetime | None = None,
) -> list[WeatherMessage]:
    fetch_time = fetch_time or datetime.now(timezone.utc)

    # Không giả định UTC: dùng offset mà API trả về (khi request timezone=auto thì khác 0)
    tz = timezone(timedelta(seconds=data.get("utc_offset_seconds", 0)))
    current = data["current"]
    current_time = _to_utc(current["time"], tz)
    interval = current.get("interval", 900)

    minutely = data["minutely_15"]
    series = {v: minutely.get(v) for v in VARIABLES}

    messages = []
    for i, ts in enumerate(minutely["time"]):
        weather_time = _to_utc(ts, tz)
        messages.append(WeatherMessage(
            location_id=location_id,
            latitude=data["latitude"],
            longitude=data["longitude"],
            time=weather_time,
            fetch_time=fetch_time,
            # mốc <= thời điểm hiện tại là observed, sau đó là forecast
            data_type="observed" if weather_time <= current_time else "forecast",
            interval=interval,
            **{v: (s[i] if s else None) for v, s in series.items()},
        ))
    return messages


# if __name__ == "__main__":
#     import json
#     import sys

#     path = sys.argv[1] if len(sys.argv) > 1 else "montreal_weather.json"
#     with open(path, encoding="utf-8") as f:
#         msgs = parse_weather_response(json.load(f), location_id="montreal")
#     print(f"Total: {len(msgs)} | observed: "
#           f"{sum(m.data_type == 'observed' for m in msgs)} | forecast: "
#           f"{sum(m.data_type == 'forecast' for m in msgs)}")
#     print(msgs[0].model_dump_json(indent=2))
