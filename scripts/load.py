"""
load.py  —  Stage 3: Load (DuckDB Database Layer)
"""
import os
import duckdb

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SAFE_TMP = os.path.join(BASE_DIR, "tmp")
SAFE_STAGING = os.path.join(SAFE_TMP, "aviation_staging")

def safe_path(p):
    return p.replace("\\", "/")

def load_stage():
    print("=" * 55)
    print("  STAGE 3 — LOAD  (DuckDB Target Sink Layer)")
    print("=" * 55)

    processed_dir = safe_path(os.path.join(BASE_DIR, "data", "processed"))
    os.makedirs(processed_dir, exist_ok=True)

    db_path = safe_path(os.path.join(processed_dir, "aviation_warehouse.duckdb"))

    con = duckdb.connect(db_path)
    try:
        tables = {
            "fact_flights": safe_path(os.path.join(SAFE_STAGING, "fact_flights.parquet", "*.parquet")),
            "dim_airline": safe_path(os.path.join(SAFE_STAGING, "airline_summary.parquet", "*.parquet")),
            "dim_route": safe_path(os.path.join(SAFE_STAGING, "route_summary.parquet", "*.parquet")),
            "dim_airport": safe_path(os.path.join(SAFE_STAGING, "airport_summary.parquet", "*.parquet")),
        }

        print("Loading high-volume data tables into DuckDB backend...")
        for tbl, path in tables.items():
            con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{path}')")
            n = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Loaded Table {tbl:15s}: {n:,} rows")

        print("\nExporting flat reporting layer CSV files...")
        for tbl in tables.keys():
            out_csv = safe_path(os.path.join(processed_dir, f"{tbl}.csv"))
            con.execute(f"COPY {tbl} TO '{out_csv}' (HEADER, DELIMITER ',')")
            print(f"  Exported BI Target asset: {tbl}.csv")

        print("\n✅ Load Stage Completed Successfully!")
        print(f"   Database File Location: {db_path}")

    except Exception as e:
        print(f"❌ Load failed: {e}")
        raise
    finally:
        con.close()