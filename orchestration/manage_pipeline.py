"""
manage_pipeline.py — Workflow Management & Task Orchestration
Fulfills the Workflow Management Orchestration Objective using Prefect
"""
import os
import sys
import subprocess
from prefect import task, flow

@task(retries=1, retry_delay_seconds=15)
def execute_etl_engine():
    """
    Task worker to execute the core PySpark/DuckDB ETL pipeline
    """
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    script_path = os.path.join(base_dir, "run_pipeline.py")
    
    print(f"Orchestrator invoking core data platform runner at: {script_path}")
    
    # Human Fix: Add env variables forcing UTF-8 to prevent Windows emoji encoding crashes
    custom_env = os.environ.copy()
    custom_env["PYTHONIOENCODING"] = "utf-8"
    custom_env["PYTHONUTF8"] = "1"
    
    # Run the script as a managed system subprocess with the clean encoding environment
    result = subprocess.run(
        [sys.executable, script_path], 
        capture_output=True, 
        text=True, 
        env=custom_env,
        encoding="utf-8"
    )
    
    if result.returncode != 0:
        print(f"ETL Execution Failure Logs:\n{result.stderr}")
        raise RuntimeError("The data pipeline execution engine failed.")
        
    print(f"ETL Execution Success Logs:\n{result.stdout}")
    return True

@flow(name="aviation-etl-orchestration-flow")
def master_flow():
    """
    Master workflow flow controller definition
    """
    print("Initializing scheduled platform management task loops...")
    execute_etl_engine()

if __name__ == "__main__":
    master_flow()