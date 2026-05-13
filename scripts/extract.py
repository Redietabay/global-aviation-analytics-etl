"""
extract.py — Stage 1: Extract (Standard System)
"""
import os
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAFE_TMP = os.path.join(BASE_DIR, "tmp")
SAFE_STAGING = os.path.join(SAFE_TMP, "aviation_staging")

os.makedirs(SAFE_TMP, exist_ok=True)
os.makedirs(SAFE_STAGING, exist_ok=True)

def safe_path(p):
    return p.replace("\\", "/")

def get_spark():
    spark = (
        SparkSession.builder
        .appName("Aviation-ETL-Extract")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", safe_path(os.path.join(SAFE_TMP, "warehouse")))
        .config("spark.local.dir", safe_path(SAFE_TMP))
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={safe_path(SAFE_TMP)}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={safe_path(SAFE_TMP)}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def extract_stage():
    print("=" * 60)
    print("  STAGE 1 — EXTRACT")
    print("=" * 60)

    raw_path = os.path.join(BASE_DIR, "data", "raw")

    flights_path  = safe_path(os.path.join(raw_path, "flights.csv"))
    airports_path = safe_path(os.path.join(raw_path, "airports.json"))
    aircraft_path = safe_path(os.path.join(raw_path, "aircrafts.parquet"))

    print(f"Flights  : {flights_path}")
    print(f"Airports : {airports_path}")
    print(f"Aircraft : {aircraft_path}")
    print(f"Staging  : {safe_path(SAFE_STAGING)}\n")

    spark = get_spark()

    try:
        df_flights = spark.read.option("header", "true").option("inferSchema", "true").csv(flights_path)
        df_airports = spark.read.option("multiline", "true").json(airports_path)
        df_aircraft = spark.read.parquet(aircraft_path)

        print(f"Flights rows : {df_flights.count():,}")
        print(f"Airports rows: {df_airports.count():,}")
        print(f"Aircraft rows: {df_aircraft.count():,}")

        df_flights.write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "stg_flights.parquet")))
        df_airports.write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "stg_airports.parquet")))
        df_aircraft.write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "stg_aircraft.parquet")))

        print("\n✅ Extract Stage Completed Successfully!")

    except Exception as e:
        print(f"\n❌ Error during Extract Stage: {e}")
        raise e
    finally:
        spark.stop()