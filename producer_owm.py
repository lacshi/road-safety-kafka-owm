"""
producer_owm.py
---------------
Streams real-time OSM amenities to 'osm_amenities' topic
and real-time weather data to 'weather_data' topic using OpenWeatherMap.

Why OpenWeatherMap instead of Open-Meteo?
  - Open-Meteo has a 5-15 min processing lag (data always behind real time)
  - OpenWeatherMap has a 2-5 min lag with fresher observation timestamps

Setup:
  1. Get a free API key from https://openweathermap.org/api
  2. Replace OWM_API_KEY below with your key
  3. Run: python producer_owm.py
"""

import json
import time
import datetime
import threading
import requests
from kafka import KafkaProducer
from config.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    OSM_TOPIC,
    WEATHER_TOPIC,
)

# ── OpenWeatherMap API Key ────────────────────────────────────────────────────
OWM_API_KEY = "87ba999509e0ca598e9efa8fccb43853"   # 👈 Replace with your free key from openweathermap.org

# ── Location ──────────────────────────────────────────────────────────────────
LOCATION     = "London"
BBOX         = (51.49, -0.15, 51.52, -0.08)   # south, west, north, east
LAT          = (BBOX[0] + BBOX[2]) / 2         # 51.505
LON          = (BBOX[1] + BBOX[3]) / 2         # -0.115

# ── API URLs ──────────────────────────────────────────────────────────────────
OVERPASS_URL = "http://overpass-api.de/api/interpreter"
OWM_URL      = "https://api.openweathermap.org/data/2.5/weather"

# ── Shared Kafka Producer ─────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# ── Pipeline 1: OSM → osm_amenities (real-time continuous loop) ───────────────
def run_osm_producer():
    print(f"[OSM] Starting real-time stream to '{OSM_TOPIC}' ...")
    while True:
        try:
            query = f"""
            [out:json][timeout:60];
            (
              node["amenity"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
              way["amenity"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
              relation["amenity"]({BBOX[0]},{BBOX[1]},{BBOX[2]},{BBOX[3]});
            );
            out center tags;
            """
            response = requests.post(OVERPASS_URL, data=query, timeout=60)
            response.raise_for_status()
            elements = response.json()["elements"]
            print(f"[OSM] Fetched {len(elements)} elements. Streaming ...\n")

            for el in elements:
                tags    = el.get("tags", {})
                amenity = tags.get("amenity")
                if not amenity:
                    continue

                if el["type"] == "node":
                    lat, lon = el.get("lat"), el.get("lon")
                else:
                    centre = el.get("center", {})
                    lat, lon = centre.get("lat"), centre.get("lon")

                record = {
                    "osm_id":       el["id"],
                    "element_type": el["type"],
                    "latitude":     lat,
                    "longitude":    lon,
                    "amenity":      amenity,
                    "name":         tags.get("name") or "N/A",
                }

                producer.send(OSM_TOPIC, record)
                print(f"[OSM] Sent → lat={lat} | lon={lon} | amenity={amenity} | name={record['name']}")
                time.sleep(0.1)

            producer.flush()
            print("[OSM] Batch done. Re-fetching in 60s ...\n")
            time.sleep(60)

        except Exception as e:
            print(f"[OSM] Error: {e}. Retrying in 30s ...")
            time.sleep(30)


# ── Pipeline 2: OpenWeatherMap → weather_data ─────────────────────────────────
def run_weather_producer():
    print(f"[WEATHER] Starting OpenWeatherMap real-time stream to '{WEATHER_TOPIC}' ...")
    last_record = None

    while True:
        try:
            params = {
                "lat":   LAT,
                "lon":   LON,
                "appid": OWM_API_KEY,
                "units": "metric",   # Celsius, m/s wind (converted below)
            }
            resp = requests.get(OWM_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # OWM returns Unix timestamp — convert to readable ISO format
            obs_time   = datetime.datetime.utcfromtimestamp(data["dt"]).strftime("%Y-%m-%dT%H:%M:%S")
            fetch_time = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

            record = {
                "location":               LOCATION,
                "api_timestamp":          obs_time,         # OWM observation time
                "fetched_at":             fetch_time,       # Actual time we fetched (shows real lag)
                "temperature_c":          data["main"]["temp"],
                "apparent_temperature_c": data["main"]["feels_like"],
                "humidity_pct":           data["main"]["humidity"],
                "rain_mm":                data.get("rain", {}).get("1h", 0.0),
                "snowfall_cm":            data.get("snow", {}).get("1h", 0.0),
                "wind_speed_kmh":         round(data["wind"]["speed"] * 3.6, 2),  # m/s → km/h
                "wind_gusts_kmh":         round(data["wind"].get("gust", 0) * 3.6, 2),
                "visibility_m":           data.get("visibility", None),
                "surface_pressure_hpa":   data["main"]["pressure"],
                "weather_description":    data["weather"][0]["description"].title(),
            }

            # Only send to Kafka if data has actually changed (avoid duplicate messages)
            comparable = {k: v for k, v in record.items() if k != "fetched_at"}
            last_comparable = {k: v for k, v in last_record.items() if k != "fetched_at"} if last_record else None

            if last_comparable is None or comparable != last_comparable:
                producer.send(WEATHER_TOPIC, record)
                producer.flush()
                print(
                    f"[WEATHER] Change detected → Sent | "
                    f"obs={obs_time} | fetched={fetch_time} | "
                    f"{record['weather_description']} | {record['temperature_c']}°C"
                )
                last_record = record
            else:
                print(f"[WEATHER] No change at {fetch_time} — skipping Kafka send.")

            time.sleep(15)   # Poll every 15s — OWM updates every 2-5 min

        except Exception as e:
            print(f"[WEATHER] Error: {e}. Retrying in 30s ...")
            time.sleep(30)


# ── Run both pipelines in parallel forever ────────────────────────────────────
if __name__ == "__main__":
    if OWM_API_KEY == "YOUR_API_KEY_HERE":
        print("❌ ERROR: Please set your OpenWeatherMap API key in OWM_API_KEY before running.")
        exit(1)

    t_osm     = threading.Thread(target=run_osm_producer,     daemon=True)
    t_weather = threading.Thread(target=run_weather_producer, daemon=True)

    t_osm.start()
    t_weather.start()

    print("Both pipelines running in real time. Press Ctrl+C to stop.\n")
    t_osm.join()
    t_weather.join()
