"""
generate_missing_sources.py
============================
Run this ONCE to create the 2 missing data source files:
  - data/raw/airports.json       (JSON source)
  - data/raw/aircrafts.parquet   (Parquet source)

You only need your flights CSV already in data/raw/flights.csv

Run:  python generate_missing_sources.py
"""

import os
import json
import random
import numpy as np
import pandas as pd

random.seed(42)
np.random.seed(42)

os.makedirs("data/raw", exist_ok=True)

# ================================================================
# FILE 1 — airports.json   (JSON source requirement)
# ================================================================
print("Creating airports.json ...")

airports = [
    {"iata_code": "ATL", "airport_name": "Hartsfield-Jackson Atlanta",      "city": "Atlanta",       "country": "USA", "region": "Americas", "latitude": 33.64,  "longitude": -84.43, "timezone": "UTC-5", "num_terminals": 2, "annual_capacity_M": 107.4},
    {"iata_code": "LAX", "airport_name": "Los Angeles International",        "city": "Los Angeles",   "country": "USA", "region": "Americas", "latitude": 33.94,  "longitude": -118.41,"timezone": "UTC-8", "num_terminals": 9, "annual_capacity_M": 88.1},
    {"iata_code": "ORD", "airport_name": "O'Hare International",             "city": "Chicago",       "country": "USA", "region": "Americas", "latitude": 41.98,  "longitude": -87.91, "timezone": "UTC-6", "num_terminals": 4, "annual_capacity_M": 83.2},
    {"iata_code": "DFW", "airport_name": "Dallas Fort Worth International",  "city": "Dallas",        "country": "USA", "region": "Americas", "latitude": 32.90,  "longitude": -97.04, "timezone": "UTC-6", "num_terminals": 5, "annual_capacity_M": 75.1},
    {"iata_code": "DEN", "airport_name": "Denver International",             "city": "Denver",        "country": "USA", "region": "Americas", "latitude": 39.86,  "longitude": -104.67,"timezone": "UTC-7", "num_terminals": 1, "annual_capacity_M": 69.0},
    {"iata_code": "JFK", "airport_name": "John F Kennedy International",     "city": "New York",      "country": "USA", "region": "Americas", "latitude": 40.64,  "longitude": -73.78, "timezone": "UTC-5", "num_terminals": 6, "annual_capacity_M": 62.5},
    {"iata_code": "SFO", "airport_name": "San Francisco International",      "city": "San Francisco", "country": "USA", "region": "Americas", "latitude": 37.62,  "longitude": -122.38,"timezone": "UTC-8", "num_terminals": 4, "annual_capacity_M": 57.9},
    {"iata_code": "SEA", "airport_name": "Seattle-Tacoma International",     "city": "Seattle",       "country": "USA", "region": "Americas", "latitude": 47.45,  "longitude": -122.31,"timezone": "UTC-8", "num_terminals": 1, "annual_capacity_M": 51.8},
    {"iata_code": "LAS", "airport_name": "Harry Reid International",         "city": "Las Vegas",     "country": "USA", "region": "Americas", "latitude": 36.08,  "longitude": -115.15,"timezone": "UTC-8", "num_terminals": 2, "annual_capacity_M": 51.5},
    {"iata_code": "MCO", "airport_name": "Orlando International",            "city": "Orlando",       "country": "USA", "region": "Americas", "latitude": 28.43,  "longitude": -81.31, "timezone": "UTC-5", "num_terminals": 4, "annual_capacity_M": 50.6},
    {"iata_code": "CLT", "airport_name": "Charlotte Douglas International",  "city": "Charlotte",     "country": "USA", "region": "Americas", "latitude": 35.21,  "longitude": -80.94, "timezone": "UTC-5", "num_terminals": 1, "annual_capacity_M": 48.8},
    {"iata_code": "PHX", "airport_name": "Phoenix Sky Harbor International", "city": "Phoenix",       "country": "USA", "region": "Americas", "latitude": 33.44,  "longitude": -112.01,"timezone": "UTC-7", "num_terminals": 3, "annual_capacity_M": 46.9},
    {"iata_code": "MIA", "airport_name": "Miami International",              "city": "Miami",         "country": "USA", "region": "Americas", "latitude": 25.80,  "longitude": -80.29, "timezone": "UTC-5", "num_terminals": 3, "annual_capacity_M": 45.9},
    {"iata_code": "EWR", "airport_name": "Newark Liberty International",     "city": "Newark",        "country": "USA", "region": "Americas", "latitude": 40.69,  "longitude": -74.17, "timezone": "UTC-5", "num_terminals": 3, "annual_capacity_M": 43.4},
    {"iata_code": "MSP", "airport_name": "Minneapolis Saint Paul Intl",      "city": "Minneapolis",   "country": "USA", "region": "Americas", "latitude": 44.88,  "longitude": -93.22, "timezone": "UTC-6", "num_terminals": 2, "annual_capacity_M": 40.0},
    {"iata_code": "BOS", "airport_name": "Logan International",              "city": "Boston",        "country": "USA", "region": "Americas", "latitude": 42.36,  "longitude": -71.01, "timezone": "UTC-5", "num_terminals": 3, "annual_capacity_M": 38.5},
    {"iata_code": "DTW", "airport_name": "Detroit Metropolitan Wayne County","city": "Detroit",       "country": "USA", "region": "Americas", "latitude": 42.21,  "longitude": -83.35, "timezone": "UTC-5", "num_terminals": 2, "annual_capacity_M": 35.2},
    {"iata_code": "PHL", "airport_name": "Philadelphia International",       "city": "Philadelphia",  "country": "USA", "region": "Americas", "latitude": 39.87,  "longitude": -75.24, "timezone": "UTC-5", "num_terminals": 4, "annual_capacity_M": 33.1},
    {"iata_code": "LGA", "airport_name": "LaGuardia Airport",                "city": "New York",      "country": "USA", "region": "Americas", "latitude": 40.78,  "longitude": -73.87, "timezone": "UTC-5", "num_terminals": 4, "annual_capacity_M": 30.0},
    {"iata_code": "IAH", "airport_name": "George Bush Intercontinental",     "city": "Houston",       "country": "USA", "region": "Americas", "latitude": 29.99,  "longitude": -95.34, "timezone": "UTC-6", "num_terminals": 5, "annual_capacity_M": 45.0},
    {"iata_code": "BWI", "airport_name": "Baltimore Washington International","city": "Baltimore",    "country": "USA", "region": "Americas", "latitude": 39.18,  "longitude": -76.67, "timezone": "UTC-5", "num_terminals": 1, "annual_capacity_M": 27.5},
    {"iata_code": "SLC", "airport_name": "Salt Lake City International",     "city": "Salt Lake City","country": "USA", "region": "Americas", "latitude": 40.79,  "longitude": -111.98,"timezone": "UTC-7", "num_terminals": 1, "annual_capacity_M": 26.3},
    {"iata_code": "IAD", "airport_name": "Washington Dulles International",  "city": "Washington DC", "country": "USA", "region": "Americas", "latitude": 38.94,  "longitude": -77.46, "timezone": "UTC-5", "num_terminals": 4, "annual_capacity_M": 23.7},
    {"iata_code": "DCA", "airport_name": "Ronald Reagan Washington National","city": "Washington DC", "country": "USA", "region": "Americas", "latitude": 38.85,  "longitude": -77.04, "timezone": "UTC-5", "num_terminals": 3, "annual_capacity_M": 23.9},
    {"iata_code": "MDW", "airport_name": "Chicago Midway International",     "city": "Chicago",       "country": "USA", "region": "Americas", "latitude": 41.79,  "longitude": -87.75, "timezone": "UTC-6", "num_terminals": 1, "annual_capacity_M": 22.2},
    {"iata_code": "TPA", "airport_name": "Tampa International",              "city": "Tampa",         "country": "USA", "region": "Americas", "latitude": 27.98,  "longitude": -82.53, "timezone": "UTC-5", "num_terminals": 1, "annual_capacity_M": 21.7},
    {"iata_code": "PDX", "airport_name": "Portland International",           "city": "Portland",      "country": "USA", "region": "Americas", "latitude": 45.59,  "longitude": -122.60,"timezone": "UTC-8", "num_terminals": 1, "annual_capacity_M": 19.8},
    {"iata_code": "HNL", "airport_name": "Daniel K Inouye International",    "city": "Honolulu",      "country": "USA", "region": "Americas", "latitude": 21.32,  "longitude": -157.92,"timezone": "UTC-10","num_terminals": 2, "annual_capacity_M": 21.4},
    {"iata_code": "STL", "airport_name": "St Louis Lambert International",   "city": "St Louis",      "country": "USA", "region": "Americas", "latitude": 38.75,  "longitude": -90.37, "timezone": "UTC-6", "num_terminals": 2, "annual_capacity_M": 15.6},
    {"iata_code": "OAK", "airport_name": "Oakland International",            "city": "Oakland",       "country": "USA", "region": "Americas", "latitude": 37.72,  "longitude": -122.22,"timezone": "UTC-8", "num_terminals": 2, "annual_capacity_M": 13.4},
]

with open("data/raw/airports.json", "w") as f:
    json.dump(airports, f, indent=2)

print(f"  ✅ airports.json created — {len(airports)} airports")


# ================================================================
# FILE 2 — aircrafts.parquet   (Parquet source requirement)
# ================================================================
print("Creating aircrafts.parquet ...")

AIRCRAFT_TYPES = [
    ("B737", "Boeing",   "737-800",    162, 5765,  "Narrow"),
    ("B738", "Boeing",   "737 MAX 8",  178, 6570,  "Narrow"),
    ("B77W", "Boeing",   "777-300ER",  396, 13649, "Wide"),
    ("B788", "Boeing",   "787-8",      242, 13621, "Wide"),
    ("B789", "Boeing",   "787-9",      296, 14140, "Wide"),
    ("A319", "Airbus",   "A319",       124, 6850,  "Narrow"),
    ("A320", "Airbus",   "A320",       150, 6150,  "Narrow"),
    ("A321", "Airbus",   "A321",       185, 7400,  "Narrow"),
    ("A332", "Airbus",   "A330-200",   253, 13450, "Wide"),
    ("A359", "Airbus",   "A350-900",   325, 15000, "Wide"),
    ("E190", "Embraer",  "E190",        98, 4537,  "Regional"),
    ("CRJ9", "Bombardier","CRJ-900",    90, 2956,  "Regional"),
]

AIRLINES = ["AA", "DL", "UA", "WN", "AS", "B6", "NK", "F9", "HA", "G4"]

rows = []
for i in range(1000):
    code, mfr, model, cap, rng, cat = random.choice(AIRCRAFT_TYPES)
    airline = random.choice(AIRLINES)
    age = random.randint(1, 22)
    rows.append({
        "registration":          f"N{random.randint(100,999)}{random.choice(['AA','DL','UA','WN','SW'])}",
        "icao_type":             code,
        "manufacturer":          mfr,
        "model":                 model,
        "category":              cat,
        "seating_capacity":      cap,
        "range_km":              rng,
        "airline_code":          airline,
        "year_manufactured":     2024 - age,
        "age_years":             age,
        "engines":               2 if cat != "Wide" else random.choice([2, 4]),
        "max_takeoff_weight_kg": random.randint(50000, 352000),
        "fuel_capacity_L":       random.randint(20000, 180000),
        "is_active":             random.choice([True] * 9 + [False]),
    })

df_aircraft = pd.DataFrame(rows)
df_aircraft.to_parquet("data/raw/aircrafts.parquet", index=False)

print(f"  ✅ aircrafts.parquet created — {len(df_aircraft)} aircraft records")

print("""
====================================================
  ALL FILES READY in data/raw/
====================================================
  ✅  data/raw/flights.csv        (your download)
  ✅  data/raw/airports.json      (just created)
  ✅  data/raw/aircrafts.parquet  (just created)

  Next step:  python run_pipeline.py
====================================================
""")
