import openmeteo_requests
import pandas as pd
import os
from datetime import datetime

openmeteo = openmeteo_requests.Client()

URL = "https://api.open-meteo.com/v1/forecast"

LOCATIONS = [
    {"city": "Recife", "lat": -8.0584933, "lon": -34.8848193},
    {"city": "Sao_Paulo", "lat": -23.5505199, "lon": -46.6333094},
    {"city": "Rio_de_Janeiro", "lat": -22.9068467, "lon": -43.1728965},
    {"city": "Brasilia", "lat": -15.7801, "lon": -47.9292},
]

VOLUME_PATH = "/Volumes/workspace/default/data/weather_data"
OUTPUT_FORMAT = "parquet"
TABLE_NAME = "workspace.default.table"


def main():

    df_weather = pd.DataFrame()
    today_str = datetime.now().strftime("%Y-%m-%d")

    for location in LOCATIONS:

        params = {
            "latitude": location["lat"],
            "longitude": location["lon"],
            "hourly": ["temperature_2m", "precipitation", "wind_speed_10m"],
            "current": ["temperature_2m", "relative_humidity_2m"],
            "start_date": today_str,
            "end_date": today_str,
            "timezone": "auto",
        }

        responses = openmeteo.weather_api(URL, params=params)
        response = responses[0]

        hourly = response.Hourly()

        hourly_start = hourly.Time()
        hourly_end = hourly.TimeEnd()
        hourly_interval = hourly.Interval()

        utc_offset = response.UtcOffsetSeconds()

        timestamps = pd.date_range(
            start=pd.to_datetime(hourly_start + utc_offset, unit="s"),
            end=pd.to_datetime(hourly_end + utc_offset, unit="s"),
            freq=pd.Timedelta(seconds=hourly_interval),
            inclusive="left",
        )

        temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        precipitation = hourly.Variables(1).ValuesAsNumpy()
        wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()

        hourly_data = {
            "city": location["city"],
            "datetime": timestamps,
            "temperature_2m": temperature_2m,
            "precipitation": precipitation,
            "wind_speed_10m": wind_speed_10m,
        }

        df_hourly = pd.DataFrame(data=hourly_data)

        df_weather = pd.concat([df_weather, df_hourly], ignore_index=True)

    timestamp_str = datetime.now().strftime("%d%m%Y")

    if OUTPUT_FORMAT.lower() == "parquet":
        file_path = os.path.join(VOLUME_PATH, f"weather_data_{timestamp_str}.parquet")
        df_weather.to_parquet(file_path, index=False)


if __name__ == "__main__":
    main()
