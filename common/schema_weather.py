from datetime import datetime, timezone
from typing import Literal
import json
from pydantic import BaseModel, ConfigDict, Field


WeatherDataType = Literal["observed", "forecast"]


class WeatherMessage(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    latitude: float
    longitude: float

    time: datetime
    fetch_time: datetime

    data_type: WeatherDataType
    source: Literal["open_meteo"] = "open_meteo"

    interval: int

    temperature_2m: float
    relative_humidity_2m: int

    precipitation: float
    rain: float
    snowfall: float

    wind_speed_10m: float
    wind_direction_10m: int
    wind_gusts_10m: float

    visibility: float
    weather_code: int


def parse_weather_response(
    data: dict,
    fetch_time: datetime | None = None,
) -> list[WeatherMessage]:

    fetch_time = fetch_time or datetime.now(timezone.utc)

    current_time = datetime.fromisoformat(
        data["current"]["time"]
    ).replace(tzinfo=timezone.utc)

    minutely = data["minutely_15"]

    messages = []

    for i, timestamp in enumerate(minutely["time"]):

        weather_time = datetime.fromisoformat(
            timestamp
        ).replace(tzinfo=timezone.utc)

        # Phân loại dựa trên mốc thời gian hiện tại
        data_type = (
            "observed"
            if weather_time <= current_time
            else "forecast"
        )

        message = WeatherMessage(
            latitude=data["latitude"],
            longitude=data["longitude"],

            time=weather_time,
            fetch_time=fetch_time,

            data_type=data_type,

            interval=data["current"]["interval"],

            temperature_2m=minutely["temperature_2m"][i],
            relative_humidity_2m=minutely["relative_humidity_2m"][i],

            precipitation=minutely["precipitation"][i],
            rain=minutely["rain"][i],
            snowfall=minutely["snowfall"][i],

            wind_speed_10m=minutely["wind_speed_10m"][i],
            wind_direction_10m=minutely["wind_direction_10m"][i],
            wind_gusts_10m=minutely["wind_gusts_10m"][i],

            visibility=minutely["visibility"][i],
            weather_code=minutely["weather_code"][i],
        )

        messages.append(message)

    return messages


def parse_weather_response(
    data: dict,
    fetch_time: datetime | None = None,
) -> list[WeatherMessage]:

    fetch_time = fetch_time or datetime.now(timezone.utc)

    current_time = datetime.fromisoformat(
        data["current"]["time"]
    ).replace(tzinfo=timezone.utc)

    minutely = data["minutely_15"]

    messages = []

    for i, timestamp in enumerate(minutely["time"]):

        weather_time = datetime.fromisoformat(
            timestamp
        ).replace(tzinfo=timezone.utc)

        # Phân loại dựa trên mốc thời gian hiện tại
        data_type = (
            "observed"
            if weather_time <= current_time
            else "forecast"
        )

        message = WeatherMessage(
            latitude=data["latitude"],
            longitude=data["longitude"],

            time=weather_time,
            fetch_time=fetch_time,

            data_type=data_type,

            interval=data["current"]["interval"],

            temperature_2m=minutely["temperature_2m"][i],
            relative_humidity_2m=minutely["relative_humidity_2m"][i],

            precipitation=minutely["precipitation"][i],
            rain=minutely["rain"][i],
            snowfall=minutely["snowfall"][i],

            wind_speed_10m=minutely["wind_speed_10m"][i],
            wind_direction_10m=minutely["wind_direction_10m"][i],
            wind_gusts_10m=minutely["wind_gusts_10m"][i],

            visibility=minutely["visibility"][i],
            weather_code=minutely["weather_code"][i],
        )

        messages.append(message)

    return messages


def main():
    # Đọc dữ liệu đầu vào
    with open("montreal_weather.json", "r", encoding="utf-8") as f:
        data = json.load(f)

    # Parse dữ liệu
    messages = parse_weather_response(data)

    print(f"Total messages: {len(messages)}")

    # Thống kê
    observed_count = sum(
        msg.data_type == "observed"
        for msg in messages
    )

    forecast_count = sum(
        msg.data_type == "forecast"
        for msg in messages
    )

    print(f"Observed: {observed_count}")
    print(f"Forecast: {forecast_count}")

    # In từng message
    for i, message in enumerate(messages, start=1):
        print(f"\n{'=' * 60}")
        print(f"Message {i}")
        print(f"{'=' * 60}")

        print(f"Time       : {message.time}")
        print(f"Fetch time : {message.fetch_time}")
        print(f"Data type  : {message.data_type}")

        print("\nJSON:")
        print(
            message.model_dump_json(indent=4)
        )

    # Lưu kết quả
    with open("weather_messages.json", "w", encoding="utf-8") as f:
        json.dump(
            [msg.model_dump(mode="json") for msg in messages],
            f,
            indent=4,
            ensure_ascii=False,
        )

    print("\nSaved to weather_messages.json")


if __name__ == "__main__":
    main()
