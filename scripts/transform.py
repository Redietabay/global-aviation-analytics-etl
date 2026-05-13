"""
transform.py  —  Stage 2: Transform (PySpark Optimization)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, when, year, month, to_timestamp, hour

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAFE_TMP = os.path.join(BASE_DIR, "tmp")
SAFE_STAGING = os.path.join(SAFE_TMP, "aviation_staging")

def safe_path(p):
    return p.replace("\\", "/")

def get_spark():
    spark = (
        SparkSession.builder
        .appName("Aviation-ETL-Transform")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.warehouse.dir", safe_path(os.path.join(SAFE_TMP, "spark-warehouse")))
        .config("spark.local.dir", safe_path(SAFE_TMP))
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={safe_path(SAFE_TMP)}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={safe_path(SAFE_TMP)}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def transform_stage():
    print("=" * 55)
    print("  STAGE 2 — TRANSFORM  (Apache PySpark)")
    print("=" * 55)

    spark = get_spark()

    try:
        df_flights  = spark.read.parquet(safe_path(os.path.join(SAFE_STAGING, "stg_flights.parquet")))
        df_airports = spark.read.parquet(safe_path(os.path.join(SAFE_STAGING, "stg_airports.parquet")))
        df_aircraft = spark.read.parquet(safe_path(os.path.join(SAFE_STAGING, "stg_aircraft.parquet")))

        # Map typical column combinations safely
        airport_code_col = "IATA" if "IATA" in df_airports.columns else "iata_code"
        duration_col = "flight_duration_minutes" if "flight_duration_minutes" in df_flights.columns else "flight_duration_min"

        df_flights = df_flights.filter(col("flight_id").isNotNull()).fillna({"delay_minutes": 0, "passengers_boarded": 0})
        df_flights = df_flights.withColumn("dep_timestamp", to_timestamp(col("scheduled_departure"), "yyyy-MM-dd HH:mm:ss"))
        df_flights = df_flights.withColumn("flight_year", year(col("dep_timestamp")))
        df_flights = df_flights.withColumn("flight_month", month(col("dep_timestamp")))
        df_flights = df_flights.withColumn("departure_hour", hour(col("dep_timestamp")))

        df_flights = df_flights.withColumn("delay_category",
            when(col("delay_minutes") == 0, "On-Time")
            .when(col("delay_minutes") <= 30, "Minor Delay")
            .when(col("delay_minutes") <= 120, "Moderate Delay")
            .otherwise("Severe Delay")
        )

        avg_rev = df_flights.agg(avg("ticket_revenue_usd")).first()[0] or 0
        df_flights = df_flights.withColumn("revenue_tier",
            when(col("ticket_revenue_usd") >= avg_rev * 1.5, "HIGH")
            .when(col("ticket_revenue_usd") >= avg_rev, "MEDIUM")
            .otherwise("LOW")
        )

        df_dep = df_airports.select(col(airport_code_col).alias("dep_iata"), col("city").alias("dep_city"), col("country").alias("dep_country"), col("region").alias("dep_region"))
        df_arr = df_airports.select(col(airport_code_col).alias("arr_iata"), col("city").alias("arr_city"), col("country").alias("arr_country"))

        df_fact = df_flights.join(df_dep, df_flights.departure_airport == df_dep.dep_iata, "left") \
                            .join(df_arr, df_flights.arrival_airport == df_arr.arr_iata, "left")

        df_airline = df_fact.groupBy("airline_code").agg(
            count("flight_id").alias("total_flights"),
            spark_sum("passengers_boarded").alias("total_passengers"),
            spark_sum("ticket_revenue_usd").alias("total_revenue_usd"),
            avg("delay_minutes").alias("avg_delay_min"),
            avg(duration_col).alias("avg_duration_min"),
        )

        df_route = df_fact.groupBy("departure_airport", "arrival_airport", "dep_city", "arr_city").agg(
            count("flight_id").alias("flight_count"),
            avg("distance_km").alias("avg_distance_km"),
            avg("ticket_revenue_usd").alias("avg_revenue_usd"),
            avg("delay_minutes").alias("avg_delay_min"),
        )

        df_airport_summary = df_fact.groupBy("departure_airport", "dep_city", "dep_country", "dep_region").agg(
            count("flight_id").alias("departures"),
            spark_sum("passengers_boarded").alias("total_passengers_departed"),
            avg("delay_minutes").alias("avg_departure_delay"),
        )

        df_fact.coalesce(1).write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "fact_flights.parquet")))
        df_airline.coalesce(1).write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "airline_summary.parquet")))
        df_route.coalesce(1).write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "route_summary.parquet")))
        df_airport_summary.coalesce(1).write.mode("overwrite").parquet(safe_path(os.path.join(SAFE_STAGING, "airport_summary.parquet")))

        print("✅ Transform Stage Completed Successfully!")

    except Exception as e:
        print(f"❌ Transform failed: {e}")
        raise
    finally:
        spark.stop()