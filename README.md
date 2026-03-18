# Road Safety Kafka — OpenWeatherMap Edition

Real-time road safety data pipeline using Apache Kafka.
Streams OSM amenities and live weather data in parallel.

- Weather API switched from **Open-Meteo → OpenWeatherMap** (2–5 min lag vs 5–15 min)
- Weather producer now polls every **15s** with **change detection** (no duplicate Kafka messages)
- Consumer shows both `OBS TIME` (OWM observation) and `FETCHED AT` (actual fetch time) so you can see the exact lag

## Project Structure
```
road-safety-owm/
├── config/
│   └── kafka_config.py       # Kafka broker + topic settings
├── producer/
│   └── producer_owm.py       # OSM + OpenWeatherMap producer (run this)
├── consumer/
│   └── consumer.py           # Kafka consumer (prints OSM + weather together)
├── docker/
│   └── docker-compose.yml    # Zookeeper + Kafka setup
└── requirements.txt
```

## Setup

### 1. Get OpenWeatherMap API Key (free)
Go to https://openweathermap.org/api → Sign up → Copy your API key

### 2. Set your API key
Open `producer/producer_owm.py` and replace:
```python
OWM_API_KEY = "YOUR_API_KEY_HERE"
```

### 3. Start Kafka
```bash
cd docker
docker-compose up -d
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Run Producer (Terminal 1)
```bash
python producer/producer_owm.py
```

### 6. Run Consumer (Terminal 2)
```bash
python consumer/consumer.py
```

## Sample Consumer Output
```
=======================================================
LAT         : 51.5074
LON         : -0.1278
OSM ID      : 123456789
AMENITY     : hospital
NAME        : St Thomas' Hospital
LOCATION    : London
TYPE        : node
-------------------------------------------------------
OBS TIME    : 2024-01-15T19:52:00  ← OWM observation time
FETCHED AT  : 2024-01-15T19:54:12  ← our actual fetch time
TEMP        : 8.3°C
FEELS LIKE  : 5.1°C
HUMIDITY    : 78%
RAIN        : 0.0 mm
SNOWFALL    : 0.0 cm
WIND        : 21.6 km/h
GUSTS       : 32.4 km/h
VISIBILITY  : 9000 m
PRESSURE    : 1012 hPa
WEATHER     : Light Rain
=======================================================
```
