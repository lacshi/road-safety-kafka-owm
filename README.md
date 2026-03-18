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

### 4. Install dependencies (in folder path)
```bash
pip install -r requirements.txt
```

### 5. Run Producer (Terminal 1)
```bash
python -m producer.producer_owm
```

### 6. Run Consumer (Terminal 2)
```bash
python -m consumer.consumer
```

## Sample Consumer Output
```
=======================================================

=======================================================
LAT         : 51.5006138
LON         : -0.1077833
OSM ID      : 671694989
AMENITY     : fast_food
NAME        : Lebanese Grills
LOCATION    : London
TYPE        : node
-------------------------------------------------------
OBS TIME    : 2026-03-18T21:07:30  ← OWM observation time
FETCHED AT  : 2026-03-18T21:15:41  ← our actual fetch time
TEMP        : 9.83°C
FEELS LIKE  : 9.17°C
HUMIDITY    : 66%
RAIN        : 0.0 mm
SNOWFALL    : 0.0 cm
WIND        : 6.44 km/h
GUSTS       : 11.27 km/h
VISIBILITY  : 10000 m
PRESSURE    : 1024 hPa
WEATHER     : Clear Sky
=======================================================

```
