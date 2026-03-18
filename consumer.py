"""
consumer.py
-----------
Reads from two separate Kafka topics:
  - osm_amenities : OSM amenity records
  - weather_data  : Real-time weather records (via OpenWeatherMap)

Weather is kept updated in the background.
For every OSM record received, prints OSM details first
then the current weather data immediately below it.

Note:
  - api_timestamp : The actual observation time from OpenWeatherMap
  - fetched_at    : The time we fetched it (so you can see the lag)
"""

import json
import threading
from kafka import KafkaConsumer
from config.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    OSM_TOPIC,
    OSM_CONSUMER_GROUP,
    WEATHER_TOPIC,
    WEATHER_CONSUMER_GROUP,
)

# ── Shared latest weather (updated by background thread) ────────────────────
latest_weather = {}
weather_lock   = threading.Lock()
print_lock     = threading.Lock()


# ── Background thread: keeps latest weather up to date ──────────────────────
def consume_weather():
    consumer = KafkaConsumer(
        WEATHER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=WEATHER_CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
    )
    for message in consumer:
        with weather_lock:
            latest_weather.update(message.value)


# ── Main thread: prints OSM + weather together for each OSM record ──────────
def consume_osm():
    consumer = KafkaConsumer(
        OSM_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=OSM_CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
    )
    print(f"Consumer Started — listening on '{OSM_TOPIC}' and '{WEATHER_TOPIC}' ...\n")

    for message in consumer:
        d = message.value

        with weather_lock:
            w = latest_weather.copy()

        with print_lock:
            print("=" * 55)
            print(f"LAT         : {d.get('latitude')}")
            print(f"LON         : {d.get('longitude')}")
            print(f"OSM ID      : {d.get('osm_id')}")
            print(f"AMENITY     : {d.get('amenity')}")
            print(f"NAME        : {d.get('name') or 'N/A'}")
            print(f"LOCATION    : {w.get('location') or 'N/A'}")
            print(f"TYPE        : {d.get('element_type')}")
            print("-" * 55)
            if w:
                print(f"OBS TIME    : {w.get('api_timestamp')}  ← OWM observation time")
                print(f"FETCHED AT  : {w.get('fetched_at')}  ← our actual fetch time")
                print(f"TEMP        : {w.get('temperature_c')}°C")
                print(f"FEELS LIKE  : {w.get('apparent_temperature_c')}°C")
                print(f"HUMIDITY    : {w.get('humidity_pct')}%")
                print(f"RAIN        : {w.get('rain_mm')} mm")
                print(f"SNOWFALL    : {w.get('snowfall_cm')} cm")
                print(f"WIND        : {w.get('wind_speed_kmh')} km/h")
                print(f"GUSTS       : {w.get('wind_gusts_kmh')} km/h")
                print(f"VISIBILITY  : {w.get('visibility_m')} m")
                print(f"PRESSURE    : {w.get('surface_pressure_hpa')} hPa")
                print(f"WEATHER     : {w.get('weather_description')}")
            else:
                print("WEATHER     : waiting for weather data ...")
            print("=" * 55)
            print()


# ── Start weather background thread, then run OSM consumer ──────────────────
if __name__ == "__main__":
    t_weather = threading.Thread(target=consume_weather, daemon=True)
    t_weather.start()
    consume_osm()
