import requests
from datetime import datetime, timedelta, timezone

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_montreal_weather():
    # Tọa độ trung tâm Montreal
    latitude = 45.5019
    longitude = -73.5674

    # Thời gian UTC
    now = datetime.now(timezone.utc)

    # Làm tròn xuống mốc 15 phút gần nhất
    start_time = now.replace(
        minute=(now.minute // 15) * 15,
        second=0,
        microsecond=0
    )

    end_time = start_time + timedelta(hours=1)

    params = {
        "latitude": latitude,
        "longitude": longitude,

        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "rain",
            "snowfall",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "visibility",
            "weather_code",
        ],

        "minutely_15": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "rain",
            "snowfall",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "visibility",
            "weather_code",
        ],

        "start_minutely_15": start_time.strftime("%Y-%m-%dT%H:%M"),
        "end_minutely_15": end_time.strftime("%Y-%m-%dT%H:%M"),

        "timezone": "UTC",
        "wind_speed_unit": "kmh",
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


if __name__ == "__main__":
    data = fetch_montreal_weather()

    with open("montreal_weather.json", "w", encoding="utf-8") as f:
        import json
        json.dump(data, f, indent=2, ensure_ascii=False)

    weather = data["minutely_15"]

    for i, timestamp in enumerate(weather["time"]):
        print(f"\nTimestamp: {timestamp}")

        print("Temperature:", weather["temperature_2m"][i])
        print("Humidity:", weather["relative_humidity_2m"][i])
        print("Precipitation:", weather["precipitation"][i])
        print("Wind speed:", weather["wind_speed_10m"][i])
        print("Wind gusts:", weather["wind_gusts_10m"][i])
        print("Visibility:", weather["visibility"][i])
        print("Weather code:", weather["weather_code"][i])
